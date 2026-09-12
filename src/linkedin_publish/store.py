"""The publication ledger: records, the store protocol, and an in-memory store.

The ledger exists for one promise, and it is narrower than "exactly once":

    **No automatic replay after an ambiguous send.**

Exactly-once delivery across an external API and our database is not achievable
and is not claimed. What is achievable is that an uncertain outcome stops and
waits for a human, and that no new run id, tick, process, token rotation,
checkpoint replay or re-import can talk the system into sending it again.

The uniqueness key is `(subject_kind, subject_id, campaign_id, post_id,
account_id)`. The same key arriving with a *different* payload digest is a
conflict, never a second post.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from .models import Adapter, PostDraft, SubjectKind

__all__ = [
    "ApprovalRecord",
    "CommissioningGrant",
    "InMemoryPublicationStore",
    "ManifestRecord",
    "PublicationEvent",
    "PublicationKey",
    "PublicationRecord",
    "PublicationState",
    "PublicationStore",
    "StoreConflict",
]

PublicationState = Literal[
    "pending",
    "claimed",
    "sending",
    "published",
    "failed",
    "unknown",
    "expired",
    "cancelled",
]

#: States from which no automated transition may ever send again.
TERMINAL_OR_HELD: frozenset[str] = frozenset({"published", "failed", "unknown", "expired", "cancelled"})

_ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "pending": frozenset({"claimed", "expired", "cancelled"}),
    "claimed": frozenset({"pending", "sending", "cancelled"}),
    # A lease that expires mid-send becomes `unknown`, never `pending`: the send
    # may have been accepted, and `pending` is a licence to retry.
    "sending": frozenset({"published", "pending", "failed", "unknown"}),
    "unknown": frozenset({"published", "failed"}),
    "published": frozenset(),
    "failed": frozenset(),
    "expired": frozenset(),
    "cancelled": frozenset(),
}


class StoreConflict(Exception):
    """A compare-and-set lost, or a key was reused with a different payload."""


class PublicationKey(BaseModel):
    """The natural key. Stable across runs, ticks, processes and credentials."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subject_kind: SubjectKind
    subject_id: str
    campaign_id: str
    post_id: str
    account_id: str

    def __str__(self) -> str:
        return f"{self.subject_kind}:{self.subject_id}/{self.campaign_id}/{self.post_id}@{self.account_id}"


class PublicationRecord(BaseModel):
    """One approved payload and everything known about its delivery."""

    model_config = ConfigDict(extra="forbid")

    publication_id: str
    key: PublicationKey
    binding_id: str
    #: Digest over the approved bytes. Immutable. A change invalidates approval.
    payload_digest: str
    draft: PostDraft
    state: PublicationState = "pending"

    scheduled_at: datetime
    expires_at: datetime
    #: Earliest next attempt — set by a rate-limit deferral. Always inside the
    #: approval window; a deferral can cross midnight but not the expiry.
    not_before: datetime | None = None

    attempt_token: str | None = None
    lease_deadline: datetime | None = None
    attempts: int = 0

    post_urn: str | None = None
    permalink: str | None = None
    adapter: Adapter | None = None
    published_at: datetime | None = None
    failure_code: str | None = None
    failure_detail: str | None = None

    #: Set when an operator authors a replacement after a failure. The original
    #: is never re-sent; the replacement is a new id with its own approval.
    replaces: str | None = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def is_due(self, now: datetime) -> bool:
        """`scheduled_at <= now < expires_at`, and past any deferral."""
        if self.not_before is not None and now < self.not_before:
            return False
        return self.scheduled_at <= now < self.expires_at

    def is_expired(self, now: datetime) -> bool:
        return now >= self.expires_at


class ManifestRecord(BaseModel):
    """An imported manifest, stored as the exact normalized bytes it arrived as."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_id: str
    campaign_id: str
    binding_id: str
    digest: str
    content: bytes
    imported_at: datetime
    imported_by: str


class ApprovalRecord(BaseModel):
    """An operator's approval of a specific digest.

    The actor and time are stamped by the trusted writer, never supplied by a
    caller. A boolean in a tool argument or a manifest field cannot create one.
    """

    model_config = ConfigDict(extra="forbid")

    approval_id: str
    publication_id: str
    payload_digest: str
    binding_id: str
    approved_by: str
    approved_at: datetime
    revoked_at: datetime | None = None
    revoked_by: str | None = None

    def is_active(self, *, digest: str, now: datetime) -> bool:
        """Valid only for the exact digest, and only while not revoked."""
        if self.revoked_at is not None and self.revoked_at <= now:
            return False
        return self.payload_digest == digest


class CommissioningGrant(BaseModel):
    """A one-shot licence to send *one* publication while publishing is disabled.

    This is how the C1 canary goes out before the binding is enabled. It is
    scoped to one stored publication, one digest, one binding and one capability,
    issued only under the operator CLI's commissioning actor, and consumed at the
    durable transition into `sending`. A scheduler cannot create one; an unknown
    outcome cannot renew one.
    """

    model_config = ConfigDict(extra="forbid")

    grant_id: str
    publication_id: str
    payload_digest: str
    binding_id: str
    capability: str
    issued_by: str
    issued_at: datetime
    expires_at: datetime
    consumed_at: datetime | None = None

    def is_usable(self, *, publication_id: str, digest: str, binding_id: str, now: datetime) -> bool:
        return (
            self.consumed_at is None
            and now < self.expires_at
            and self.publication_id == publication_id
            and self.payload_digest == digest
            and self.binding_id == binding_id
        )


class PublicationEvent(BaseModel):
    """Append-only audit row."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    publication_id: str
    at: datetime
    kind: str
    actor: str
    detail: dict[str, object] = Field(default_factory=dict)


@runtime_checkable
class PublicationStore(Protocol):
    """Durable storage for the ledger. Implemented in-memory and on Postgres."""

    async def upsert_publication(self, record: PublicationRecord) -> PublicationRecord: ...

    async def get_publication(self, publication_id: str) -> PublicationRecord | None: ...

    async def find_by_key(self, key: PublicationKey) -> PublicationRecord | None: ...

    async def find_due(
        self, *, binding_id: str, campaign_id: str | None, now: datetime, limit: int
    ) -> list[PublicationRecord]: ...

    async def all_for_binding(self, binding_id: str) -> list[PublicationRecord]: ...

    async def compare_and_set(
        self,
        publication_id: str,
        *,
        expected_state: PublicationState,
        expected_attempt_token: str | None,
        updates: dict[str, object],
    ) -> PublicationRecord: ...

    async def append_event(self, event: PublicationEvent) -> None: ...

    async def events(self, publication_id: str) -> list[PublicationEvent]: ...

    async def get_approval(self, publication_id: str) -> ApprovalRecord | None: ...

    async def put_approval(self, approval: ApprovalRecord) -> None: ...

    async def get_grant(self, publication_id: str) -> CommissioningGrant | None: ...

    async def put_grant(self, grant: CommissioningGrant) -> None: ...

    async def consume_grant(self, grant_id: str, *, at: datetime) -> bool: ...


class InMemoryPublicationStore:
    """A complete, correct, process-local store.

    Used by fixtures and by the offline CLI. It implements the same CAS
    semantics as the Postgres store — including refusing a key reuse with a
    different digest — so a test that passes here is testing the real rules, not
    a lenient stand-in. It is not durable and not shared: a deployment that
    injects this instead of the Postgres store has no ledger at all.
    """

    def __init__(self) -> None:
        self._records: dict[str, PublicationRecord] = {}
        self._by_key: dict[str, str] = {}
        self._events: list[PublicationEvent] = []
        self._approvals: dict[str, ApprovalRecord] = {}
        self._grants: dict[str, CommissioningGrant] = {}
        self._lock = asyncio.Lock()

    async def upsert_publication(self, record: PublicationRecord) -> PublicationRecord:
        async with self._lock:
            key = str(record.key)
            existing_id = self._by_key.get(key)
            if existing_id is not None:
                existing = self._records[existing_id]
                if existing.payload_digest != record.payload_digest:
                    raise StoreConflict(
                        f"publication key {key} already exists with a different payload digest; "
                        "a revised payload needs a new publication id and its own approval"
                    )
                return existing
            self._records[record.publication_id] = record
            self._by_key[key] = record.publication_id
            return record

    async def get_publication(self, publication_id: str) -> PublicationRecord | None:
        return self._records.get(publication_id)

    async def find_by_key(self, key: PublicationKey) -> PublicationRecord | None:
        publication_id = self._by_key.get(str(key))
        return self._records.get(publication_id) if publication_id else None

    async def find_due(
        self, *, binding_id: str, campaign_id: str | None, now: datetime, limit: int
    ) -> list[PublicationRecord]:
        due = [
            record
            for record in self._records.values()
            if record.binding_id == binding_id
            and record.state == "pending"
            and (campaign_id is None or record.key.campaign_id == campaign_id)
            and record.is_due(now)
        ]
        due.sort(key=lambda record: record.scheduled_at)
        return due[:limit]

    async def all_for_binding(self, binding_id: str) -> list[PublicationRecord]:
        return [record for record in self._records.values() if record.binding_id == binding_id]

    async def compare_and_set(
        self,
        publication_id: str,
        *,
        expected_state: PublicationState,
        expected_attempt_token: str | None,
        updates: dict[str, object],
    ) -> PublicationRecord:
        async with self._lock:
            record = self._records.get(publication_id)
            if record is None:
                raise StoreConflict(f"unknown publication {publication_id}")
            if record.state != expected_state:
                raise StoreConflict(
                    f"publication {publication_id} is {record.state}, expected {expected_state}"
                )
            if expected_attempt_token is not None and record.attempt_token != expected_attempt_token:
                raise StoreConflict(f"publication {publication_id} is owned by another attempt")

            target = updates.get("state")
            if isinstance(target, str) and target != record.state:
                if target not in _ALLOWED_TRANSITIONS[record.state]:
                    raise StoreConflict(f"illegal transition {record.state} -> {target}")

            updated = record.model_copy(update={**updates, "updated_at": datetime.now(timezone.utc)})
            self._records[publication_id] = updated
            return updated

    async def append_event(self, event: PublicationEvent) -> None:
        self._events.append(event)

    async def events(self, publication_id: str) -> list[PublicationEvent]:
        return [event for event in self._events if event.publication_id == publication_id]

    async def get_approval(self, publication_id: str) -> ApprovalRecord | None:
        return self._approvals.get(publication_id)

    async def put_approval(self, approval: ApprovalRecord) -> None:
        self._approvals[approval.publication_id] = approval

    async def get_grant(self, publication_id: str) -> CommissioningGrant | None:
        return self._grants.get(publication_id)

    async def put_grant(self, grant: CommissioningGrant) -> None:
        self._grants[grant.publication_id] = grant

    async def consume_grant(self, grant_id: str, *, at: datetime) -> bool:
        async with self._lock:
            for key, grant in self._grants.items():
                if grant.grant_id == grant_id:
                    if grant.consumed_at is not None:
                        return False
                    self._grants[key] = grant.model_copy(update={"consumed_at": at})
                    return True
            return False


def allowed_transitions(state: PublicationState) -> Sequence[str]:
    """States reachable from `state`. Exposed for tests and documentation."""
    return sorted(_ALLOWED_TRANSITIONS[state])
