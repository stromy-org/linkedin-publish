"""The platform-neutral dispatcher: one tick per binding, the same rules for every adapter.

The send sequence, in the order that matters (identical to the LinkedIn
`PublicationService`):

    claim (CAS)  →  every gate re-read  →  stage (persisted)  →  live preflight
                 →  state=sending (durable, committed)  →  the one write  →  receipt

What the core guarantees, whatever the adapter:

1. **The adapter comes from the trusted binding**, once, before anything else.
   There is no fallback: an adapter that is missing, or that answered
   ambiguously, is never replaced by another one for the same record.
2. **Capability is exact.** The record's (operation, format) must be both
   declared by the adapter and `enabled` on the binding. Otherwise it is refused
   before transport and the record keeps its format; it is never sent as
   something "close".
3. **`sending` is durable before the write leaves.** A crash after that point
   leaves `unknown`, recovered from the dead lease, never `pending`.
4. **`unknown` is reconciled read-only.** The adapter's `reconcile` may settle it
   (exactly one match) or leave it; the core never calls `send` for it again.
5. **`scheduled` and `handoff` are never fresh work.** A scheduled record is only
   read back; a handoff is only recorded.
6. **Publishing disabled means no write.** A binding with `publish_enabled=false`
   — the rollback switch — still expires, recovers leases, reads back schedules
   and reconciles unknowns (receipts are retained, pending external schedules are
   reconciled), but claims nothing and sends nothing.

No database transaction spans a provider request.
"""

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .adapters.base import Resolution, SocialAdapter
from .errors import CapabilityRefused, Deferred, OutcomeUnknown, Refused, SocialPublishError, StoreConflict
from .ledger import SocialEvent, SocialStore
from .model import SocialBinding, SocialRecord
from .states import SocialState

__all__ = ["DEFAULT_LEASE", "DispatchService", "PolicyGate", "SocialTick"]

DEFAULT_LEASE = timedelta(minutes=10)

#: An injected, synchronous re-check of policy just before dispatch (the job
#: wires the instance's policy decision here). Raises `Refused` to hold the send.
PolicyGate = Callable[[SocialBinding, SocialRecord, datetime], Awaitable[None]]


def _no_failures() -> list[dict[str, object]]:
    return []


@dataclass
class SocialTick:
    """What one tick did. Every exit path produces one of these."""

    due: int = 0
    sent: int = 0
    scheduled: int = 0
    published: int = 0
    deferred: int = 0
    unknown: int = 0
    failed: int = 0
    expired: int = 0
    cancelled: int = 0
    reconciled: int = 0
    skipped_capability: int = 0
    disabled: bool = False
    dry_run: bool = False
    failures: list[dict[str, object]] = field(default_factory=_no_failures)

    @property
    def outcome(self) -> str:
        if self.due == 0 and not (self.unknown or self.failed):
            return "no-work"
        if self.unknown or self.failed:
            return "partial"
        return "success"

    def as_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "due": self.due,
            "sent": self.sent,
            "scheduled": self.scheduled,
            "published": self.published,
            "deferred": self.deferred,
            "unknown": self.unknown,
            "failed": self.failed,
            "expired": self.expired,
            "cancelled": self.cancelled,
            "reconciled": self.reconciled,
            "skipped_capability": self.skipped_capability,
            "disabled": self.disabled,
            "dry_run": self.dry_run,
            "failures": self.failures,
        }


class DispatchService:
    """Durable dispatch over any registered adapter."""

    def __init__(
        self,
        store: SocialStore,
        adapters: Mapping[str, SocialAdapter],
        *,
        lease: timedelta = DEFAULT_LEASE,
        actor: str = "scheduler",
        policy_gate: PolicyGate | None = None,
    ) -> None:
        self._store = store
        self._adapters = dict(adapters)
        self._lease = lease
        self._actor = actor
        self._policy_gate = policy_gate

    @property
    def store(self) -> SocialStore:
        return self._store

    def adapter_for(self, binding: SocialBinding) -> SocialAdapter:
        """The binding's adapter, by its trusted name. Never inferred, never a fallback."""
        adapter = self._adapters.get(binding.adapter)
        if adapter is None or binding.platform not in adapter.platforms:
            raise Refused("adapter_unavailable", f"no {binding.adapter} adapter serves {binding.platform} here")
        return adapter

    def require_capability(self, binding: SocialBinding, record: SocialRecord, adapter: SocialAdapter) -> None:
        pair = (record.operation, record.format)
        if pair not in adapter.declared():
            raise CapabilityRefused(
                f"{adapter.name} does not implement {record.operation} {record.format}",
                operation=record.operation,
                format=record.format,
            )
        state = binding.capability_state(record.operation, record.format)
        if state != "enabled":
            raise CapabilityRefused(
                f"{record.operation} {record.format} is {state} on binding {binding.binding_id}; it is enabled "
                "by recorded evidence of its own shape, never by another format",
                operation=record.operation,
                format=record.format,
            )

    # ------------------------------------------------------------------ tick

    async def tick(
        self, binding: SocialBinding, *, now: datetime | None = None, limit: int = 10, dry_run: bool = True
    ) -> SocialTick:
        moment = (now or datetime.now(UTC)).astimezone(UTC)
        result = SocialTick(dry_run=dry_run, disabled=not binding.publish_enabled)
        adapter = self.adapter_for(binding)
        records = await self._store.all_for_binding(binding.binding_id)

        for record in records:
            if record.state == "pending" and record.is_expired(moment):
                result.expired += 1
                if not dry_run:
                    await self._safe(record, "pending", {"state": "expired"}, "expired")
        if dry_run:
            due = await self._store.find_due(binding_id=binding.binding_id, now=moment, limit=limit)
            result.due = len(due)
            for record in due:
                try:
                    self._offline_gates(binding, record, adapter, moment)
                except CapabilityRefused as exc:
                    result.skipped_capability += 1
                    result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
                except Refused as exc:
                    result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
            return result

        await self._recover_leases(records, moment)
        for record in await self._store.all_for_binding(binding.binding_id):
            if record.state == "unknown":
                await self._reconcile_one(binding, adapter, record, moment, result)
            elif record.state == "scheduled":
                await self._read_back_one(binding, adapter, record, moment, result)

        if not binding.publish_enabled:
            return result
        due = await self._store.find_due(binding_id=binding.binding_id, now=moment, limit=limit)
        result.due = len(due)
        for record in due:
            try:
                outcome = await self.dispatch_one(binding, record.publication_id, now=moment)
            except StoreConflict:
                continue  # another replica owns it
            except CapabilityRefused as exc:
                result.skipped_capability += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
                continue
            except Deferred as exc:
                result.deferred += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
                continue
            except OutcomeUnknown as exc:
                result.unknown += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
                continue
            except Refused as exc:
                result.failed += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
                continue
            result.sent += 1
            if outcome == "scheduled":
                result.scheduled += 1
            elif outcome == "published":
                result.published += 1
            elif outcome == "cancelled":
                result.cancelled += 1
            elif outcome == "waiting":
                result.sent -= 1
                result.deferred += 1
        return result

    # ------------------------------------------------------------ one record

    async def dispatch_one(self, binding: SocialBinding, publication_id: str, *, now: datetime | None = None) -> str:
        """Dispatch one stored, approved, due record. Returns the state it reached.

        Returns `waiting` when staging released the claim (an Instagram container
        still processing); the record is `pending` again with its staging kept.
        """
        moment = (now or datetime.now(UTC)).astimezone(UTC)
        record = await self._require(publication_id)
        if record.binding_id != binding.binding_id:
            raise Refused("binding_mismatch", f"{publication_id} belongs to another binding", final=True)
        if not binding.publish_enabled:
            raise Refused("publish_disabled", f"publishing is disabled on binding {binding.binding_id}")
        adapter = self.adapter_for(binding)
        if record.state != "pending":
            raise Refused("not_pending", f"{publication_id} is {record.state}; it is never fresh work")

        token = secrets.token_urlsafe(16)
        claimed = await self._store.compare_and_set(
            publication_id,
            expected_state="pending",
            expected_attempt_token=None,
            updates={
                "state": "claimed",
                "attempt_token": token,
                "lease_deadline": moment + self._lease,
                "attempts": record.attempts + 1,
            },
        )
        await self._event(publication_id, "claimed", {"attempt_token": token})

        try:
            await self._gates(binding, claimed, adapter, moment)
        except SocialPublishError as exc:
            await self._release(claimed, token, exc)
            raise

        async def persist(updates: dict[str, object]) -> None:
            nonlocal claimed
            claimed = await self._store.compare_and_set(
                publication_id, expected_state="claimed", expected_attempt_token=token, updates=updates
            )
            await self._event(publication_id, "staged", {key: str(value) for key, value in updates.items()})

        try:
            staged = await adapter.stage(binding, claimed, moment, persist)
        except SocialPublishError as exc:
            await self._release(claimed, token, exc)
            raise
        if staged.kind == "wait":
            await self._cas(
                claimed, token, "claimed", {"state": "pending", "attempt_token": None, "lease_deadline": None}
            )
            await self._event(publication_id, "staging_wait", {"reason": staged.reason})
            return "waiting"
        if staged.kind == "restage":
            await self._cas(
                claimed,
                token,
                "claimed",
                {"state": "pending", "attempt_token": None, "lease_deadline": None, "container_id": None},
            )
            await self._event(publication_id, "staging_restart", {"reason": staged.reason})
            return "waiting"
        if staged.kind == "failed":
            await self._cas(
                claimed,
                token,
                "claimed",
                {"state": "failed", "failure_code": staged.reason, "attempt_token": None, "lease_deadline": None},
            )
            await self._event(publication_id, "failed", {"code": staged.reason})
            raise Refused(staged.reason or "staging_failed", "the provider rejected the staged media", final=True)
        if staged.kind == "anomaly":
            await self._cas(
                claimed,
                token,
                "claimed",
                {"state": "unknown", "failure_code": staged.reason, "attempt_token": None, "lease_deadline": None},
            )
            await self._event(publication_id, "unknown", {"code": staged.reason})
            raise OutcomeUnknown(f"the provider shows {staged.reason}; held for reconciliation")

        try:
            await adapter.preflight(binding, claimed, moment)
        except SocialPublishError as exc:
            await self._release(claimed, token, exc)
            raise

        # --- durable `sending` BEFORE the write -------------------------------
        claimed = await self._cas(claimed, token, "claimed", {"state": "sending"})
        await self._event(publication_id, "sending", {"attempt_token": token})

        try:
            sent = await adapter.send(binding, claimed, moment)
        except Deferred as exc:
            await self._settle(publication_id, token, "pending", {"not_before": _bounded(exc.not_before, claimed)}, exc)
            raise
        except OutcomeUnknown as exc:
            await self._settle(publication_id, token, "unknown", {"remote_id": exc.remote_id}, exc)
            raise
        except Refused as exc:
            await self._settle(publication_id, token, "failed", {}, exc)
            raise
        except Exception as exc:  # noqa: BLE001 - an unclassified crash is never "did not send"
            unknown = OutcomeUnknown(f"unclassified failure during send: {type(exc).__name__}")
            await self._settle(publication_id, token, "unknown", {}, unknown)
            raise unknown from exc

        target: SocialState = sent.state
        await self._store.compare_and_set(
            publication_id,
            expected_state="sending",
            expected_attempt_token=token,
            updates={
                "state": target,
                "remote_id": sent.remote_id,
                "permalink": sent.permalink,
                "adapter": adapter.name,
                "published_at": sent.published_at if target == "published" else None,
                "failure_code": "readback_mismatch" if target == "cancelled" else None,
                "attempt_token": None,
                "lease_deadline": None,
            },
        )
        await self._event(publication_id, target, {"remote_id": sent.remote_id})
        return target

    async def cancel(self, binding: SocialBinding, publication_id: str, *, now: datetime | None = None) -> str:
        """Withdraw one record. Past the sending boundary the honest answers are
        `outcome_pending`, `already_visible` and `cancel_failed`, never a promise."""
        moment = (now or datetime.now(UTC)).astimezone(UTC)
        record = await self._require(publication_id)
        if record.state in ("pending", "claimed"):
            await self._store.compare_and_set(
                publication_id,
                expected_state=record.state,
                expected_attempt_token=None,
                updates={"state": "cancelled", "attempt_token": None, "lease_deadline": None},
            )
            await self._event(publication_id, "cancelled", {})
            return "cancelled"
        if record.state in ("sending", "unknown"):
            return "outcome_pending"
        if record.state == "published":
            return "already_visible"
        if record.state == "handoff":
            return "withdraw_required"
        if record.state != "scheduled":
            return "too_late"
        adapter = self.adapter_for(binding)
        if await adapter.cancel(binding, record, moment):
            await self._safe(record, "scheduled", {"state": "cancelled"}, "cancelled")
            return "cancelled"
        await self._safe(record, "scheduled", {"failure_code": "cancel_failed"}, "cancel_failed")
        return "cancel_failed"

    # ---------------------------------------------------------------- gates

    def _offline_gates(
        self, binding: SocialBinding, record: SocialRecord, adapter: SocialAdapter, now: datetime
    ) -> None:
        self.require_capability(binding, record, adapter)
        adapter.validate(binding, record, now)

    async def _gates(self, binding: SocialBinding, record: SocialRecord, adapter: SocialAdapter, now: datetime) -> None:
        approval = await self._store.get_approval(record.publication_id)
        if approval is None or not approval.is_active(digest=record.payload_digest, now=now):
            raise Refused("approval_missing", f"{record.publication_id} has no active approval for its payload")
        if approval.binding_id != record.binding_id:
            raise Refused("approval_binding_mismatch", "the approval was issued for a different binding", final=True)
        if not record.is_due(now):
            raise Refused("outside_window", f"{record.publication_id} is outside its approved window")
        self._offline_gates(binding, record, adapter, now)
        if self._policy_gate is not None:
            await self._policy_gate(binding, record, now)

    # ------------------------------------------------------------- recovery

    async def _recover_leases(self, records: list[SocialRecord], now: datetime) -> None:
        for record in records:
            if record.lease_deadline is None or record.lease_deadline > now:
                continue
            if record.state == "claimed":
                # Staging (an Instagram container id) is kept: the next attempt
                # re-polls the same container instead of creating another.
                await self._safe(
                    record,
                    "claimed",
                    {"state": "pending", "attempt_token": None, "lease_deadline": None},
                    "lease_recovered",
                )
            elif record.state == "sending":
                await self._safe(
                    record,
                    "sending",
                    {
                        "state": "unknown",
                        "failure_code": "unknown_outcome",
                        "failure_detail": "attempt lease expired mid-send",
                        "attempt_token": None,
                        "lease_deadline": None,
                    },
                    "lease_expired_unknown",
                )

    async def _reconcile_one(
        self, binding: SocialBinding, adapter: SocialAdapter, record: SocialRecord, now: datetime, result: SocialTick
    ) -> None:
        try:
            verdict = await adapter.reconcile(binding, record, now)
        except SocialPublishError as exc:
            verdict = Resolution(None, reason=exc.code)
        if verdict.state in ("published", "scheduled", "failed"):
            updates: dict[str, object] = {"state": verdict.state}
            if verdict.remote_id is not None:
                updates["remote_id"] = verdict.remote_id
            if verdict.state == "published":
                updates["published_at"] = now
            if verdict.state == "failed":
                updates["failure_code"] = verdict.reason or "reconciled_not_sent"
            await self._safe(record, "unknown", updates, f"reconciled_{verdict.state}")
            result.reconciled += 1
            return
        result.unknown += 1
        result.failures.append(
            {"publication_id": record.publication_id, "code": "unknown_outcome", "reason": verdict.reason}
        )

    async def _read_back_one(
        self, binding: SocialBinding, adapter: SocialAdapter, record: SocialRecord, now: datetime, result: SocialTick
    ) -> None:
        try:
            verdict = await adapter.read_back(binding, record, now)
        except SocialPublishError:
            return
        if verdict.state == "published":
            await self._safe(record, "scheduled", {"state": "published", "published_at": now}, "published")
            result.published += 1
        elif verdict.state == "unknown":
            await self._safe(
                record, "scheduled", {"state": "unknown", "failure_code": verdict.reason}, "readback_unknown"
            )
            result.unknown += 1

    # -------------------------------------------------------------- helpers

    async def _require(self, publication_id: str) -> SocialRecord:
        record = await self._store.get(publication_id)
        if record is None:
            raise Refused("publication_missing", f"unknown publication {publication_id}", final=True)
        return record

    async def _cas(
        self, record: SocialRecord, token: str, expected: SocialState, updates: dict[str, object]
    ) -> SocialRecord:
        return await self._store.compare_and_set(
            record.publication_id, expected_state=expected, expected_attempt_token=token, updates=updates
        )

    async def _release(self, record: SocialRecord, token: str, exc: SocialPublishError) -> None:
        """Undo a claim after a refusal before `sending`: nothing was sent."""
        if isinstance(exc, Refused) and exc.final:
            updates: dict[str, object] = {"state": "failed", "failure_code": exc.code, "failure_detail": exc.message}
        else:
            updates = {"state": "pending", "failure_code": exc.code}
            if isinstance(exc, Deferred):
                updates["not_before"] = _bounded(exc.not_before, record)
        updates.update({"attempt_token": None, "lease_deadline": None})
        try:
            await self._cas(record, token, "claimed", updates)
        except StoreConflict:
            return
        await self._event(record.publication_id, "released", exc.as_dict())

    async def _safe(self, record: SocialRecord, expected: SocialState, updates: dict[str, object], kind: str) -> None:
        try:
            await self._store.compare_and_set(
                record.publication_id, expected_state=expected, expected_attempt_token=None, updates=updates
            )
        except StoreConflict:
            return
        await self._event(record.publication_id, kind, {})

    async def _settle(
        self, publication_id: str, token: str, state: SocialState, updates: dict[str, object], exc: SocialPublishError
    ) -> None:
        payload: dict[str, object] = {"state": state, "failure_code": exc.code, "failure_detail": exc.message}
        payload.update({key: value for key, value in updates.items() if value is not None})
        payload.setdefault("attempt_token", None)
        payload.setdefault("lease_deadline", None)
        try:
            await self._store.compare_and_set(
                publication_id, expected_state="sending", expected_attempt_token=token, updates=payload
            )
        except StoreConflict:
            return
        await self._event(publication_id, state, exc.as_dict())

    async def _event(self, publication_id: str, kind: str, detail: dict[str, object]) -> None:
        await self._store.append_event(
            SocialEvent(
                publication_id=publication_id, at=datetime.now(UTC), kind=kind, actor=self._actor, detail=detail
            )
        )


def _bounded(not_before: datetime | None, record: SocialRecord) -> datetime | None:
    """A deferral stays inside the approval window; past it, the record expires."""
    if not_before is None:
        return None
    return min(not_before, record.expires_at)
