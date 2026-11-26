"""The platform-neutral ledger: approvals, events, the store protocol, an in-memory store.

Same promise as the LinkedIn ledger, and the same tables: **no automatic replay
after an ambiguous send.** Every compare-and-set names the state it expects and,
when it holds one, the attempt token; a transition outside
`social_publish.states.TRANSITIONS` is refused.

**One owner per external action.** An intent (a C3 operation key) may have many
approval versions over time, but at most one *live* ledger row — one that is not
cancelled, expired or failed. `import_intent` enforces it atomically: the same
approval imported twice returns the existing row and writes nothing; a newer
approval of the same intent replaces an old row only while that row is still
`pending` (it is cancelled in the same transaction), and is refused
(`old_action_unresolved`) once the old row has reached the provider in any way.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from .errors import ApprovalRefused, StoreConflict
from .model import SocialBinding, SocialRecord
from .states import TRANSITIONS, SocialState, allowed

__all__ = [
    "ImportOutcome",
    "InMemorySocialStore",
    "LIVE_STATES",
    "SocialApproval",
    "SocialEvent",
    "SocialStore",
    "StoreConflict",
    "UPDATABLE",
]

#: Rows that still own their intent: everything but a definite non-send.
LIVE_STATES: frozenset[str] = frozenset(TRANSITIONS) - {"cancelled", "expired", "failed"}

#: Columns a compare-and-set may write. Mirrors the runtime role's column grants
#: (migrations 0002 and 0006); the database is the real boundary.
UPDATABLE: frozenset[str] = frozenset(
    {
        "state",
        "not_before",
        "attempt_token",
        "lease_deadline",
        "attempts",
        "remote_id",
        "container_id",
        "permalink",
        "adapter",
        "published_at",
        "failure_code",
        "failure_detail",
    }
)


class SocialApproval(BaseModel):
    """An approval of one exact payload digest, stamped by the trusted writer."""

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
        if self.revoked_at is not None and self.revoked_at <= now:
            return False
        return self.payload_digest == digest


class SocialEvent(BaseModel):
    """Append-only audit row. `detail` never holds content or a signed URL."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    publication_id: str
    at: datetime
    kind: str
    actor: str
    detail: dict[str, object] = Field(default_factory=dict)


class ImportOutcome(BaseModel):
    """What one import did: the owning row, and whether this call created it."""

    model_config = ConfigDict(extra="forbid")

    record: SocialRecord
    created: bool
    superseded: str | None = None


@runtime_checkable
class SocialStore(Protocol):
    """Durable storage for neutral records. Implemented in memory and on Postgres."""

    async def import_intent(
        self, record: SocialRecord, approval: SocialApproval, *, actor: str, at: datetime
    ) -> ImportOutcome: ...

    async def get(self, publication_id: str) -> SocialRecord | None: ...

    async def live_for_intent(self, intent_id: str) -> SocialRecord | None: ...

    async def all_for_binding(self, binding_id: str) -> list[SocialRecord]: ...

    async def find_due(self, *, binding_id: str, now: datetime, limit: int) -> list[SocialRecord]: ...

    async def compare_and_set(
        self,
        publication_id: str,
        *,
        expected_state: SocialState,
        expected_attempt_token: str | None,
        updates: dict[str, object],
    ) -> SocialRecord: ...

    async def append_event(self, event: SocialEvent) -> None: ...

    async def events(self, publication_id: str) -> list[SocialEvent]: ...

    async def get_approval(self, publication_id: str) -> SocialApproval | None: ...


def check_updates(updates: dict[str, object]) -> None:
    unknown = set(updates) - UPDATABLE
    if unknown:
        raise StoreConflict(f"refusing to update non-delivery columns: {sorted(unknown)}")


def decide_import(
    existing: list[SocialRecord], record: SocialRecord
) -> tuple[SocialRecord | None, SocialRecord | None]:
    """Shared import rule. Returns (row to return as-is, pending row to supersede).

    Raises `ApprovalRefused` when a newer approval meets an old row that already
    reached the provider, or an approval meets a row of another digest under the
    same natural key.
    """
    for row in existing:
        if row.payload_digest == record.payload_digest:
            return row, None
    live = [row for row in existing if row.state in LIVE_STATES]
    if len(live) > 1:  # pragma: no cover - the live-intent index forbids it
        raise ApprovalRefused("approval_conflict", f"intent {record.intent_id} has {len(live)} live rows")
    if live:
        old = live[0]
        if old.state != "pending":
            raise ApprovalRefused(
                "old_action_unresolved",
                f"intent {record.intent_id} is already {old.state} as {old.publication_id}; "
                "resolve the earlier external action before a newer approval can own it",
            )
        return None, old
    return None, None


class InMemorySocialStore:
    """A complete, correct, process-local store with the Postgres store's rules.

    Not durable and not shared. A deployment that injects this has no ledger;
    it exists for the contract tests and offline evaluation.
    """

    def __init__(self, bindings: list[SocialBinding] | None = None) -> None:
        self._records: dict[str, SocialRecord] = {}
        self._approvals: dict[str, SocialApproval] = {}
        self._events: list[SocialEvent] = []
        self._lock = asyncio.Lock()
        self.bindings: dict[str, SocialBinding] = {b.binding_id: b for b in bindings or []}

    async def import_intent(
        self, record: SocialRecord, approval: SocialApproval, *, actor: str, at: datetime
    ) -> ImportOutcome:
        async with self._lock:
            existing = [r for r in self._records.values() if r.intent_id == record.intent_id]
            same, supersede = decide_import(existing, record)
            if same is not None:
                return ImportOutcome(record=same, created=False)
            for row in self._records.values():
                if row.key == record.key and row.payload_digest != record.payload_digest:
                    raise ApprovalRefused("approval_conflict", f"natural key {record.key} holds another digest")
            superseded: str | None = None
            if supersede is not None:
                self._records[supersede.publication_id] = supersede.model_copy(
                    update={"state": "cancelled", "failure_code": "superseded", "updated_at": at}
                )
                self._events.append(
                    SocialEvent(publication_id=supersede.publication_id, at=at, kind="superseded", actor=actor)
                )
                superseded = supersede.publication_id
            self._records[record.publication_id] = record
            self._approvals[record.publication_id] = approval
            self._events.append(
                SocialEvent(
                    publication_id=record.publication_id,
                    at=at,
                    kind="imported",
                    actor=actor,
                    detail={"state": record.state, "intent_id": record.intent_id},
                )
            )
            return ImportOutcome(record=record, created=True, superseded=superseded)

    async def get(self, publication_id: str) -> SocialRecord | None:
        return self._records.get(publication_id)

    async def live_for_intent(self, intent_id: str) -> SocialRecord | None:
        for row in self._records.values():
            if row.intent_id == intent_id and row.state in LIVE_STATES:
                return row
        return None

    async def all_for_binding(self, binding_id: str) -> list[SocialRecord]:
        return [r for r in self._records.values() if r.binding_id == binding_id]

    async def find_due(self, *, binding_id: str, now: datetime, limit: int) -> list[SocialRecord]:
        due = [
            r for r in self._records.values() if r.binding_id == binding_id and r.state == "pending" and r.is_due(now)
        ]
        due.sort(key=lambda r: r.scheduled_at)
        return due[:limit]

    async def compare_and_set(
        self,
        publication_id: str,
        *,
        expected_state: SocialState,
        expected_attempt_token: str | None,
        updates: dict[str, object],
    ) -> SocialRecord:
        check_updates(updates)
        async with self._lock:
            record = self._records.get(publication_id)
            if record is None:
                raise StoreConflict(f"unknown publication {publication_id}")
            if record.state != expected_state:
                raise StoreConflict(f"publication {publication_id} is {record.state}, expected {expected_state}")
            if expected_attempt_token is not None and record.attempt_token != expected_attempt_token:
                raise StoreConflict(f"publication {publication_id} is owned by another attempt")
            target = updates.get("state")
            if isinstance(target, str) and not allowed(record.state, target):
                raise StoreConflict(f"illegal transition {record.state} -> {target}")
            updated = record.model_copy(update={**updates, "updated_at": datetime.now(UTC)})
            self._records[publication_id] = updated
            return updated

    async def append_event(self, event: SocialEvent) -> None:
        self._events.append(event)

    async def events(self, publication_id: str) -> list[SocialEvent]:
        return [e for e in self._events if e.publication_id == publication_id]

    async def get_approval(self, publication_id: str) -> SocialApproval | None:
        return self._approvals.get(publication_id)

    async def revoke(self, publication_id: str, *, by: str, at: datetime) -> None:
        """Test and operator helper: revoke the approval of one publication."""
        approval = self._approvals.get(publication_id)
        if approval is not None:
            self._approvals[publication_id] = approval.model_copy(update={"revoked_at": at, "revoked_by": by})
