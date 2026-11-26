"""Registered account bindings: the trusted configuration a publication hangs off.

A binding row must exist before a manifest or publication can reference it (both
tables carry a foreign key), and only the operator writer role may insert one.
Registration is therefore the first ledger write of commissioning (plan C1 step
5), and it deliberately enables nothing: every row starts with
`publish_enabled=false` and no capability measured.

What registration will not do is as important:

* **No credential value is stored** — a reference and a version only.
* **Re-registering an id with different identity fields is a conflict.** A new
  member, app or adapter is a new binding with fresh approvals, never an edit.
  Identical re-registration is an idempotent no-op, so a retried command is safe.
* **An observation never widens anything.** `record_observation` writes what a
  probe measured — scopes, expiry, when — and only for the credential version the
  binding currently names, so a stale secret's probe cannot overwrite a fresh one.
* **Rotation is the one sanctioned identity edit.** `rotate` moves a binding to a
  new credential reference and version — the same app, member and adapter, so
  approvals and the publication ledger carry over untouched. It disables
  publishing and clears the stored observation in the same write, compare-and-set
  on the version being replaced. Only `reenable` turns publishing back on, and only
  for a binding that was enabled before the rotation and has since been re-measured
  healthy for the new version (`reenable_problems`).
* **First enablement is from a receipt.** `enable_capability` marks ONE
  capability `enabled`, naming the canary publication that proved it, and turns
  publishing on. It refuses unless that publication is a `published` receipt on
  this binding for exactly that capability, and the current credential is
  measured healthy (`enable_problems`). A text canary enables text and nothing
  else (plan C1 steps 7 and 8).
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from .auth import TokenObservation
from .health import InMemoryObservationCache, ObservationCache
from .models import AccountBinding, Capability, CapabilityStatus
from .store import PublicationRecord, StoreConflict

__all__ = [
    "IDENTITY_FIELDS",
    "BindingStore",
    "InMemoryBindingStore",
    "as_registration",
    "conflict_message",
    "enable_problems",
    "enabled",
    "reenable_problems",
    "registration_conflicts",
    "require_observation_version",
    "rotated",
]

#: The fields that make a binding *this* binding. Observations, capabilities and
#: `publish_enabled` evolve after registration and are not part of identity.
IDENTITY_FIELDS: tuple[str, ...] = (
    "account_id",
    "subject_kind",
    "subject_id",
    "app_id",
    "author_urn",
    "allowed_organization_urns",
    "adapter",
    "declared_scopes",
    "credential_ref",
    "credential_version",
)


def registration_conflicts(existing: AccountBinding, proposed: AccountBinding) -> list[str]:
    """Identity fields on which `proposed` differs from what is registered."""
    return [name for name in IDENTITY_FIELDS if getattr(existing, name) != getattr(proposed, name)]


def as_registration(binding: AccountBinding) -> AccountBinding:
    """The binding as registration stores it: nothing enabled, nothing observed."""
    return binding.model_copy(
        update={
            "observed_scopes": (),
            "token_expires_at": None,
            "token_observed_at": None,
            "capabilities": (),
            "publish_enabled": False,
            "rotated_at": None,
            "rotated_from_version": None,
            "reenable_after_measure": False,
        }
    )


def rotated(
    binding: AccountBinding,
    *,
    expected_version: str,
    credential_ref: str,
    credential_version: str,
    at: datetime,
) -> AccountBinding:
    """The binding after a rotation, or `StoreConflict` when it may not rotate.

    Pure, so the in-memory and Postgres stores refuse exactly the same cases.
    Publication rows are not touched: their key carries no credential, so a
    published or unknown record stays published or unknown across the move.
    """
    if binding.credential_version != expected_version:
        raise StoreConflict(
            f"binding {binding.binding_id!r} names credential version {binding.credential_version!r}, "
            f"not {expected_version!r}; re-read it before rotating"
        )
    if credential_version == binding.credential_version:
        raise StoreConflict(
            f"binding {binding.binding_id!r} already names credential version {credential_version!r}; "
            "a rotation needs a new version"
        )
    return binding.model_copy(
        update={
            "credential_ref": credential_ref,
            "credential_version": credential_version,
            "observed_scopes": (),
            "token_expires_at": None,
            "token_observed_at": None,
            "publish_enabled": False,
            "rotated_at": at,
            "rotated_from_version": binding.credential_version,
            # A second rotation before re-enabling keeps the original intent.
            "reenable_after_measure": binding.publish_enabled or binding.reenable_after_measure,
        }
    )


def reenable_problems(
    binding: AccountBinding, observation: TokenObservation | None, *, now: datetime
) -> list[str]:
    """Why `binding` may not be re-enabled now. Empty means it may.

    Re-enabling restores what a rotation took away; it is not the canary's
    enablement. So it needs: a rotation hold, a re-measure of the CURRENT
    version taken after the rotation, and that measure healthy, with the
    declared scopes and an expiry still in the future.
    """
    problems: list[str] = []
    if binding.publish_enabled:
        problems.append("publishing is already enabled")
    if not binding.reenable_after_measure:
        problems.append(
            "the binding was not enabled before its rotation; first enablement is the canary path "
            "(commission, then enable from the receipt), not a re-enable"
        )
    if binding.token_observed_at is None or binding.token_expires_at is None:
        problems.append("no measurement is stored for the current credential: run `account inspect --record`")
    elif binding.rotated_at is not None and binding.token_observed_at < binding.rotated_at:
        problems.append("the stored measurement predates the rotation: run `account inspect --record`")
    if binding.token_expires_at is not None and binding.token_expires_at <= now:
        problems.append("the measured expiry has passed")
    if not set(binding.declared_scopes).issubset(binding.observed_scopes):
        problems.append("the observed scopes do not cover the declared scopes")
    if observation is None:
        problems.append("no health observation is cached for the current credential version")
    else:
        if observation.credential_version != binding.credential_version:
            problems.append("the cached observation is for another credential version")
        if not observation.healthy:
            problems.append(f"the last observation is not healthy ({observation.reason or 'unverified'})")
        if binding.rotated_at is not None and observation.observed_at < binding.rotated_at:
            problems.append("the cached observation predates the rotation")
    return problems


def _health_problems(
    binding: AccountBinding, observation: TokenObservation | None, *, now: datetime
) -> list[str]:
    """Why the current credential is not measured healthy. Empty means it is."""
    problems: list[str] = []
    if binding.token_observed_at is None or binding.token_expires_at is None:
        problems.append("no measurement is stored for the current credential: run `account inspect --record`")
    if binding.token_expires_at is not None and binding.token_expires_at <= now:
        problems.append("the measured expiry has passed")
    if not set(binding.declared_scopes).issubset(binding.observed_scopes):
        problems.append("the observed scopes do not cover the declared scopes")
    if observation is None:
        problems.append("no health observation is cached for the current credential version")
    else:
        if observation.credential_version != binding.credential_version:
            problems.append("the cached observation is for another credential version")
        if not observation.healthy:
            problems.append(f"the last observation is not healthy ({observation.reason or 'unverified'})")
    return problems


def enable_problems(
    binding: AccountBinding,
    evidence: PublicationRecord | None,
    *,
    capability: Capability,
    observation: TokenObservation | None,
    now: datetime,
) -> list[str]:
    """Why `capability` may not be enabled on `binding` from `evidence`. Empty means it may.

    The receipt must be this binding's, `published` with a post URN, sent through
    this binding's adapter, and of exactly this capability's shape. The capability
    must still be `unknown` (an `unavailable` one is a recorded refusal, not a
    gap), the binding must not be held after a rotation (that is `account
    reenable`), and the current credential must be measured healthy.
    """
    problems: list[str] = []
    if evidence is None:
        problems.append("the evidence publication is not in the ledger")
    else:
        if evidence.binding_id != binding.binding_id:
            problems.append(f"the evidence publication belongs to binding {evidence.binding_id}, not this one")
        if evidence.state != "published" or not evidence.post_urn:
            problems.append(
                f"the evidence publication has no published receipt (state {evidence.state}); "
                "an unknown send is reconciled first, never enabled from"
            )
        if evidence.adapter is not None and evidence.adapter != binding.adapter:
            problems.append(f"the evidence was sent through {evidence.adapter}, not this binding's {binding.adapter}")
        if evidence.draft.required_capability != capability:
            problems.append(
                f"the evidence is a {evidence.draft.required_capability!r} publication; "
                f"it enables {evidence.draft.required_capability!r} only, not {capability!r}"
            )
    state = binding.capability_state(capability)
    if state == "enabled":
        problems.append(f"capability {capability!r} is already enabled")
    elif state == "unavailable":
        problems.append(f"capability {capability!r} is recorded unavailable; that is a refusal, not a gap")
    if binding.reenable_after_measure:
        problems.append("the binding is held after a credential rotation: re-measure, then `account reenable`")
    problems.extend(_health_problems(binding, observation, now=now))
    return problems


def enabled(
    binding: AccountBinding,
    *,
    capability: Capability,
    evidence_publication_id: str,
    observed_at: datetime,
) -> AccountBinding:
    """The binding with `capability` enabled from that receipt, and publishing on.

    Pure, so both stores write exactly the same row. Other capabilities are kept
    as they are. Raises `StoreConflict` if the capability is no longer `unknown`.
    """
    if binding.capability_state(capability) != "unknown":
        raise StoreConflict(
            f"binding {binding.binding_id!r} capability {capability!r} is "
            f"{binding.capability_state(capability)}; re-run the check"
        )
    if binding.reenable_after_measure:
        raise StoreConflict(f"binding {binding.binding_id!r} is held after a rotation; re-run the check")
    status = CapabilityStatus(
        capability=capability,
        state="enabled",
        observed_at=observed_at,
        evidence_publication_id=evidence_publication_id,
    )
    kept = tuple(item for item in binding.capabilities if item.capability != capability)
    return binding.model_copy(update={"capabilities": (*kept, status), "publish_enabled": True})


class BindingStore(Protocol):
    """Persistence for account bindings. Implementations must refuse conflicts."""

    @property
    def observations(self) -> ObservationCache:
        """The token observation cache that lives beside these bindings."""
        ...

    async def register(self, binding: AccountBinding) -> bool:
        """Insert `binding`. True when created, False when identical already exists."""
        ...

    async def get(self, binding_id: str) -> AccountBinding | None: ...

    async def record_observation(self, binding_id: str, observation: TokenObservation) -> AccountBinding:
        """Store a probe's measurements. Refuses a credential-version mismatch."""
        ...

    async def rotate(
        self,
        binding_id: str,
        *,
        expected_version: str,
        credential_ref: str,
        credential_version: str,
        at: datetime,
    ) -> AccountBinding:
        """Move to a new credential version; disables publishing. See `rotated`."""
        ...

    async def reenable(self, binding_id: str, *, expected_version: str) -> AccountBinding:
        """Set `publish_enabled` back on after a rotation hold. Callers check `reenable_problems` first."""
        ...

    async def enable_capability(
        self,
        binding_id: str,
        *,
        capability: Capability,
        evidence_publication_id: str,
        observed_at: datetime,
        expected_version: str,
    ) -> AccountBinding:
        """First enablement from a canary receipt. Callers check `enable_problems` first. See `enabled`."""
        ...


class InMemoryBindingStore:
    """Dict-backed `BindingStore` for tests and offline fixtures."""

    def __init__(self) -> None:
        self._rows: dict[str, AccountBinding] = {}
        self._observations = InMemoryObservationCache()

    @property
    def observations(self) -> InMemoryObservationCache:
        return self._observations

    async def register(self, binding: AccountBinding) -> bool:
        stored = as_registration(binding)
        existing = self._rows.get(binding.binding_id)
        if existing is not None:
            differing = registration_conflicts(existing, stored)
            if differing:
                raise StoreConflict(conflict_message(binding.binding_id, differing))
            return False
        self._rows[binding.binding_id] = stored
        return True

    async def get(self, binding_id: str) -> AccountBinding | None:
        return self._rows.get(binding_id)

    async def record_observation(self, binding_id: str, observation: TokenObservation) -> AccountBinding:
        existing = self._rows.get(binding_id)
        if existing is None:
            raise StoreConflict(f"binding {binding_id!r} is not registered")
        require_observation_version(existing, observation)
        updated = existing.model_copy(
            update={
                "observed_scopes": observation.observed_scopes,
                "token_expires_at": observation.expires_at,
                "token_observed_at": observation.observed_at,
            }
        )
        self._rows[binding_id] = updated
        return updated

    async def rotate(
        self,
        binding_id: str,
        *,
        expected_version: str,
        credential_ref: str,
        credential_version: str,
        at: datetime,
    ) -> AccountBinding:
        existing = self._rows.get(binding_id)
        if existing is None:
            raise StoreConflict(f"binding {binding_id!r} is not registered")
        updated = rotated(
            existing,
            expected_version=expected_version,
            credential_ref=credential_ref,
            credential_version=credential_version,
            at=at,
        )
        self._rows[binding_id] = updated
        return updated

    async def reenable(self, binding_id: str, *, expected_version: str) -> AccountBinding:
        existing = self._rows.get(binding_id)
        if existing is None:
            raise StoreConflict(f"binding {binding_id!r} is not registered")
        if existing.credential_version != expected_version or not existing.reenable_after_measure:
            raise StoreConflict(f"binding {binding_id!r} changed since it was checked; re-run the check")
        updated = existing.model_copy(update={"publish_enabled": True, "reenable_after_measure": False})
        self._rows[binding_id] = updated
        return updated

    async def enable_capability(
        self,
        binding_id: str,
        *,
        capability: Capability,
        evidence_publication_id: str,
        observed_at: datetime,
        expected_version: str,
    ) -> AccountBinding:
        existing = self._rows.get(binding_id)
        if existing is None:
            raise StoreConflict(f"binding {binding_id!r} is not registered")
        if existing.credential_version != expected_version:
            raise StoreConflict(f"binding {binding_id!r} changed since it was checked; re-run the check")
        updated = enabled(
            existing,
            capability=capability,
            evidence_publication_id=evidence_publication_id,
            observed_at=observed_at,
        )
        self._rows[binding_id] = updated
        return updated


def conflict_message(binding_id: str, differing: list[str]) -> str:
    return (
        f"binding {binding_id!r} is already registered with different {', '.join(differing)}; "
        "a changed member, app or adapter is a NEW binding with fresh approvals, not an edit"
    )


def require_observation_version(binding: AccountBinding, observation: TokenObservation) -> None:
    if observation.credential_version != binding.credential_version:
        raise StoreConflict(
            f"observation is for credential version {observation.credential_version!r} but binding "
            f"{binding.binding_id!r} names {binding.credential_version!r}; a stale probe never overwrites"
        )
