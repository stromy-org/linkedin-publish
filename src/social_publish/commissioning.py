"""Meta commissioning: one grant for one canary, then enablement from its own receipt.

The LinkedIn model (ORG-PLAN-285 C1 steps 7–8), mirrored on the neutral core:

1. **Grant.** `commission_problems` is the pre-flight and `new_grant` the row.
   A grant names exactly one stored Meta publication, its digest, its binding,
   its `operation:format` capability and the commissioning actor. It licenses
   that one send while the capability is still `unknown`; never an
   `unavailable` capability, never a binding held after a rotation, never
   another operation or format. It expires after `GRANT_TTL`.
2. **Send.** Only a `DispatchService` built with `commissioning_actor` can use
   it — the scheduler never is — and the grant is consumed immediately before
   the durable `sending` transition. On Postgres only the commissioner role may
   write `consumed_at` (migration 0006), so a runtime handed an actor is
   refused by the database and the record returns to `pending`.
3. **Enable.** `enable_problems` refuses unless the evidence publication is a
   receipt of a send **this ledger made** (it has a `sending` event: an
   imported row proves nothing about this binding's credential), on this
   binding, through its adapter, of exactly this (operation, format), and in a
   receipt state: `published`, or for a Facebook native schedule `scheduled` —
   which the adapter only returns after the created schedule read back with
   its id, message and slot. The capability must still be `unknown`, the
   binding not held, and the token measured (an expiry in the future, or
   explicit no-expiry evidence). `enabled_binding` is the pure result.

Nothing here touches the network or a credential.
"""

from __future__ import annotations

import secrets
from collections.abc import Sequence
from datetime import datetime, timedelta

from .adapters.meta import DECLARED
from .errors import StoreConflict
from .ledger import SocialApproval, SocialEvent, SocialGrant, capability_key
from .model import OperationCapability, SocialBinding, SocialRecord

__all__ = [
    "ENABLE_RECEIPT_STATES",
    "GRANT_TTL",
    "commission_problems",
    "enable_problems",
    "enabled_binding",
    "new_grant",
    "token_problems",
]

#: How long a Meta commissioning grant stays usable (LinkedIn's `GRANT_TTL`).
GRANT_TTL = timedelta(minutes=30)

#: The receipt states that prove a (platform, operation) worked. A Facebook
#: native schedule is proven once it is created AND read back (`scheduled`);
#: everything else only by `published`.
ENABLE_RECEIPT_STATES: dict[tuple[str, str], frozenset[str]] = {
    ("facebook_page", "schedule_post"): frozenset({"scheduled", "published"}),
    ("facebook_page", "reply"): frozenset({"published"}),
    ("instagram_professional", "publish_post"): frozenset({"published"}),
    ("instagram_professional", "reply"): frozenset({"published"}),
}

#: Events that only a dispatch of this ledger writes once the provider accepted
#: (or a read-only reconcile found) the object.
_RECEIPT_EVENTS = frozenset({"scheduled", "published", "reconciled_scheduled", "reconciled_published"})


def _declared(platform: str, operation: str, fmt: str) -> bool:
    return (platform, operation, fmt) in DECLARED


def token_problems(binding: SocialBinding, *, now: datetime) -> list[str]:
    """Why the binding's current token is not measured usable. Empty means it is."""
    if binding.token_observed_at is None:
        return [
            "no token evidence is recorded for this credential version: run "
            f"`meta inspect --binding {binding.binding_id} --record` first"
        ]
    if binding.token_never_expires:
        return []
    if binding.token_expires_at is None:
        return ["the recorded token evidence has neither an expiry nor no-expiry evidence; re-inspect"]
    if binding.token_expires_at <= now:
        return [f"the token expired at {binding.token_expires_at.isoformat()}: re-mint, rotate, re-inspect"]
    return []


def commission_problems(
    binding: SocialBinding,
    record: SocialRecord | None,
    approval: SocialApproval | None,
    *,
    expected_digest: str,
    operation: str,
    format: str,  # noqa: A002 - the domain word
    open_grant: SocialGrant | None,
    now: datetime,
) -> list[str]:
    """Why a grant for this publication may not be issued. Empty means it may."""
    if record is None:
        return ["the publication is not in the ledger"]
    problems: list[str] = []
    if binding.platform == "linkedin" or record.platform == "linkedin":
        return ["a LinkedIn publication is commissioned with `account commission`, not `meta commission`"]
    if record.binding_id != binding.binding_id:
        problems.append(f"the publication belongs to binding {record.binding_id}, not {binding.binding_id}")
    if record.payload_digest != expected_digest:
        problems.append(
            f"the ledger holds digest {record.payload_digest}, not the {expected_digest} you reviewed; "
            "what you reviewed is not what is stored"
        )
    if record.state != "pending":
        problems.append(f"the publication is {record.state}; only a pending publication is commissioned")
    if (record.operation, record.format) != (operation, format):
        problems.append(
            f"the publication is {record.operation} {record.format}; a grant for {operation} {format} would "
            "license nothing (a canary enables its own shape only)"
        )
    if not _declared(binding.platform, operation, format):
        problems.append(f"the Meta adapter does not implement {binding.platform} {operation} {format}")
    state = binding.capability_state(operation, format)
    if state == "enabled":
        problems.append(f"{operation} {format} is already enabled; no grant is needed or issued")
    elif state == "unavailable":
        problems.append(f"{operation} {format} is recorded unavailable; that is a refusal, not a gap")
    if binding.reenable_after_measure:
        problems.append("the binding is held after a credential rotation; re-measure it first")
    if approval is None or not approval.is_active(digest=expected_digest, now=now):
        problems.append("the publication has no active approval for this digest; import the approval first")
    elif approval.binding_id != binding.binding_id:
        problems.append("the approval was issued for a different binding")
    if now >= record.expires_at:
        problems.append(f"the publication's window closed at {record.expires_at.isoformat()}")
    if open_grant is not None:
        problems.append(
            f"the publication already holds an open grant ({open_grant.grant_id}, expires "
            f"{open_grant.expires_at.isoformat()}); it is consumed or expires, never stacked"
        )
    return problems


def new_grant(record: SocialRecord, *, actor: str, now: datetime, ttl: timedelta = GRANT_TTL) -> SocialGrant:
    """The grant row for exactly this record, issued to `actor`, expiring after `ttl`."""
    if not actor.strip():
        raise ValueError("a grant names the commissioning actor it is issued to")
    if ttl <= timedelta(0) or ttl > GRANT_TTL:
        raise ValueError(f"a commissioning grant lives at most {GRANT_TTL}")
    return SocialGrant(
        grant_id=f"grant_{secrets.token_urlsafe(12)}",
        publication_id=record.publication_id,
        payload_digest=record.payload_digest,
        binding_id=record.binding_id,
        capability=capability_key(record.operation, record.format),
        issued_by=actor,
        issued_at=now,
        expires_at=now + ttl,
    )


def enable_problems(
    binding: SocialBinding,
    evidence: SocialRecord | None,
    events: Sequence[SocialEvent],
    *,
    operation: str,
    format: str,  # noqa: A002 - the domain word
    now: datetime,
) -> list[str]:
    """Why (operation, format) may not be enabled on `binding` from `evidence`. Empty means it may."""
    problems: list[str] = []
    if binding.platform == "linkedin":
        return ["a LinkedIn capability is enabled with `account enable`, not `meta enable`"]
    if not _declared(binding.platform, operation, format):
        problems.append(f"the Meta adapter does not implement {binding.platform} {operation} {format}")
    if evidence is None:
        problems.append("the evidence publication is not in the ledger")
    else:
        if evidence.binding_id != binding.binding_id:
            problems.append(f"the evidence publication belongs to binding {evidence.binding_id}, not this one")
        if (evidence.operation, evidence.format) != (operation, format):
            problems.append(
                f"the evidence is a {evidence.operation} {evidence.format} publication; it enables that pair "
                f"only, never {operation} {format}"
            )
        accepted = ENABLE_RECEIPT_STATES.get((binding.platform, operation), frozenset())
        if evidence.state not in accepted or not evidence.remote_id:
            wanted = " or ".join(sorted(accepted)) or "published"
            problems.append(
                f"the evidence has no {wanted} receipt with a Meta object id (state {evidence.state}); "
                "an unknown send is reconciled first, never enabled from"
            )
        if evidence.adapter is not None and evidence.adapter != binding.adapter:
            problems.append(f"the evidence was sent through {evidence.adapter}, not this binding's {binding.adapter}")
        kinds = {event.kind for event in events}
        if "sending" not in kinds or not kinds & _RECEIPT_EVENTS:
            problems.append(
                "the evidence is not the receipt of a send this ledger made (no `sending` and receipt events); "
                "an imported row is never commissioning evidence"
            )
    state = binding.capability_state(operation, format)
    if state == "enabled":
        problems.append(f"{operation} {format} is already enabled")
    elif state == "unavailable":
        problems.append(f"{operation} {format} is recorded unavailable; that is a refusal, not a gap")
    if binding.reenable_after_measure:
        problems.append("the binding is held after a credential rotation; re-measure it first")
    problems.extend(token_problems(binding, now=now))
    return problems


def enabled_binding(
    binding: SocialBinding,
    *,
    operation: str,
    format: str,  # noqa: A002 - the domain word
    evidence_publication_id: str,
    observed_at: datetime,
) -> SocialBinding:
    """The binding with exactly (operation, format) enabled from that receipt, and publishing on.

    Pure, so the in-memory and Postgres stores write the same row. Every other
    capability is kept. Raises `StoreConflict` when the pair is no longer `unknown`.
    """
    if binding.capability_state(operation, format) != "unknown":
        raise StoreConflict(
            f"binding {binding.binding_id!r} {operation} {format} is "
            f"{binding.capability_state(operation, format)}; re-run the check"
        )
    kept = tuple(c for c in binding.capabilities if (c.operation, c.format) != (operation, format))
    status = OperationCapability(
        operation=operation,
        format=format,
        state="enabled",
        observed_at=observed_at,
        evidence_publication_id=evidence_publication_id,
    )
    return binding.model_copy(update={"capabilities": (*kept, status), "publish_enabled": True})
