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
"""

from __future__ import annotations

from typing import Protocol

from .auth import TokenObservation
from .models import AccountBinding
from .store import StoreConflict

__all__ = [
    "IDENTITY_FIELDS",
    "BindingStore",
    "InMemoryBindingStore",
    "as_registration",
    "conflict_message",
    "require_observation_version",
    "registration_conflicts",
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
        }
    )


class BindingStore(Protocol):
    """Persistence for account bindings. Implementations must refuse conflicts."""

    async def register(self, binding: AccountBinding) -> bool:
        """Insert `binding`. True when created, False when identical already exists."""
        ...

    async def get(self, binding_id: str) -> AccountBinding | None: ...

    async def record_observation(self, binding_id: str, observation: TokenObservation) -> AccountBinding:
        """Store a probe's measurements. Refuses a credential-version mismatch."""
        ...


class InMemoryBindingStore:
    """Dict-backed `BindingStore` for tests and offline fixtures."""

    def __init__(self) -> None:
        self._rows: dict[str, AccountBinding] = {}

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
