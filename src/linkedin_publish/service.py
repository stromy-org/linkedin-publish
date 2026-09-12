"""The shared publication service.

Both hosted entrypoints — the MCP and the Stromy workflow — call *this*, with
their own trusted subject and their own credential scope. There is no second
implementation and no MCP hop from the workflow: one durable ledger is the only
thing that can promise a post is not sent twice, and two paths into it would be
two chances to bypass it.

The send sequence, in the order that matters:

    claim (CAS)  →  re-read every gate  →  state=sending (durable, committed)
                 →  HTTP  →  persist receipt

`sending` is committed to the database **before** the request leaves. If the
process dies at any point after that, recovery finds a `sending` record with a
dead lease and marks it `unknown` — a human then checks LinkedIn. The
alternative ordering (send, then record) is what produces duplicate posts.

No database transaction spans a LinkedIn request.
"""

from __future__ import annotations

import secrets
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .auth import CredentialProvider
from .client import LinkedInClient
from .errors import (
    AuthorForbidden,
    CapabilityUnavailable,
    CredentialExpired,
    CredentialMissing,
    LinkedInPublishError,
    ProviderRejected,
    PublishOutcomeUnknown,
    QuotaDeferred,
    ValidationFailure,
)
from .media import AssetReader
from .models import AccountBinding, PublishReceipt
from .store import (
    PublicationEvent,
    PublicationRecord,
    PublicationStore,
    StoreConflict,
)

__all__ = ["BindingLoader", "PublicationService", "TickResult"]

#: How long one attempt may hold a claim before another worker may recover it.
DEFAULT_LEASE = timedelta(minutes=10)


class BindingLoader:
    """Resolves a binding by opaque id, with an access check on every lookup.

    The pilot ships a single-binding loader. The check is still performed —
    "there is only one binding" is a deployment fact, not an authorization
    argument, and the moment a second one exists the check has to already be
    here.
    """

    def __init__(self, bindings: Sequence[AccountBinding]) -> None:
        self._bindings = {binding.binding_id: binding for binding in bindings}

    def load(self, binding_id: str, *, subject_kind: str, subject_id: str) -> AccountBinding:
        binding = self._bindings.get(binding_id)
        if binding is None:
            # Deliberately the same failure as "not yours": existence of a
            # binding id is not something an unauthorized caller learns.
            raise AuthorForbidden(f"binding {binding_id!r} is not available to this subject")
        if binding.subject_kind != subject_kind or binding.subject_id != subject_id:
            raise AuthorForbidden(f"binding {binding_id!r} is not available to this subject")
        return binding


def _no_receipts() -> list[PublishReceipt]:
    return []


def _no_failures() -> list[dict[str, object]]:
    return []


@dataclass
class TickResult:
    """What one tick did. Every exit path produces one of these."""

    due: int = 0
    published: int = 0
    deferred: int = 0
    unknown: int = 0
    failed: int = 0
    expired: int = 0
    cancelled: int = 0
    skipped_capability: int = 0
    dry_run: bool = False
    oldest_overdue_seconds: int | None = None
    receipts: list[PublishReceipt] = field(default_factory=_no_receipts)
    failures: list[dict[str, object]] = field(default_factory=_no_failures)

    @property
    def outcome(self) -> str:
        """`no-work` requires positive evidence the inputs were empty."""
        if self.due == 0:
            return "no-work"
        if self.unknown or self.failed:
            return "partial"
        return "success"

    def as_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "due": self.due,
            "published": self.published,
            "deferred": self.deferred,
            "unknown": self.unknown,
            "failed": self.failed,
            "expired": self.expired,
            "cancelled": self.cancelled,
            "skipped_capability": self.skipped_capability,
            "dry_run": self.dry_run,
            "oldest_overdue_seconds": self.oldest_overdue_seconds,
            "failures": self.failures,
        }


class PublicationService:
    """Durable publishing on top of `LinkedInClient`."""

    def __init__(
        self,
        store: PublicationStore,
        client: LinkedInClient,
        credentials: CredentialProvider,
        *,
        asset_reader: AssetReader | None = None,
        lease: timedelta = DEFAULT_LEASE,
        actor: str = "scheduler",
    ) -> None:
        self._store = store
        self._client = client
        self._credentials = credentials
        self._reader = asset_reader
        self._lease = lease
        self._actor = actor

    # ---------------------------------------------------------------- ticking

    async def publish_due(
        self,
        binding: AccountBinding,
        *,
        campaign_id: str | None = None,
        limit: int = 10,
        now: datetime | None = None,
        dry_run: bool = True,
        subject_id: str | None = None,
    ) -> TickResult:
        """Publish every approved, due record for this binding.

        A dry run claims nothing, reserves no quota, mutates no row and makes no
        request — it reports exactly what a live tick *would* attempt. It also
        uses a separate run-key namespace upstream, so a completed dry run is
        never retried as a live one.
        """
        moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        result = TickResult(dry_run=dry_run)

        await self._expire_overdue(binding, moment, result)
        await self._recover_leases(binding, moment)

        due = await self._store.find_due(
            binding_id=binding.binding_id, campaign_id=campaign_id, now=moment, limit=limit
        )
        result.due = len(due)
        if due:
            result.oldest_overdue_seconds = int(
                max((moment - record.scheduled_at).total_seconds() for record in due)
            )

        # Validate the whole eligible batch offline before any external write. A
        # malformed batch fails preflight rather than publishing its good half.
        for record in due:
            try:
                self._client.check(binding, record.draft)
            except CapabilityUnavailable as exc:
                result.skipped_capability += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
            except LinkedInPublishError as exc:
                result.failed += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})

        if dry_run or result.failures:
            return result

        for record in due:
            try:
                receipt = await self.publish_one(
                    record.publication_id,
                    binding,
                    expected_digest=record.payload_digest,
                    now=moment,
                    subject_id=subject_id,
                )
            except QuotaDeferred as exc:
                result.deferred += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
            except PublishOutcomeUnknown as exc:
                result.unknown += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
            except StoreConflict:
                # Another replica owns it. Not an error; not ours to report as one.
                continue
            except LinkedInPublishError as exc:
                result.failed += 1
                result.failures.append({"publication_id": record.publication_id, **exc.as_dict()})
            else:
                result.published += 1
                result.receipts.append(receipt)
        return result

    # --------------------------------------------------------------- one post

    async def publish_one(
        self,
        publication_id: str,
        binding: AccountBinding,
        *,
        expected_digest: str,
        now: datetime | None = None,
        subject_id: str | None = None,
    ) -> PublishReceipt:
        """Send exactly one stored, approved publication.

        `expected_digest` is supplied by the caller and re-checked against the
        stored record: a tool cannot publish "whatever is under this id now".
        """
        moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        record = await self._require(publication_id)

        if record.payload_digest != expected_digest:
            raise ValidationFailure(
                f"publication {publication_id} does not match the expected digest; "
                "the stored payload has been revised and needs fresh approval"
            )
        if record.state in {"published", "unknown"}:
            raise ValidationFailure(
                f"publication {publication_id} is already {record.state}; it is never replayed"
            )

        attempt_token = secrets.token_urlsafe(16)
        claimed = await self._store.compare_and_set(
            publication_id,
            expected_state="pending",
            expected_attempt_token=None,
            updates={
                "state": "claimed",
                "attempt_token": attempt_token,
                "lease_deadline": moment + self._lease,
                "attempts": record.attempts + 1,
            },
        )
        await self._event(publication_id, "claimed", {"attempt_token": attempt_token})

        # --- every gate, re-read at the last possible moment -----------------
        try:
            grant_id = await self._authorize_send(claimed, binding, moment)
        except LinkedInPublishError:
            await self._store.compare_and_set(
                publication_id,
                expected_state="claimed",
                expected_attempt_token=attempt_token,
                updates={"state": "pending", "attempt_token": None, "lease_deadline": None},
            )
            raise

        # --- durable `sending` BEFORE the request ----------------------------
        await self._store.compare_and_set(
            publication_id,
            expected_state="claimed",
            expected_attempt_token=attempt_token,
            updates={"state": "sending"},
        )
        if grant_id is not None:
            await self._store.consume_grant(grant_id, at=moment)
        await self._event(publication_id, "sending", {"attempt_token": attempt_token})

        credentials = await self._credentials.get(binding.binding_id)
        try:
            receipt = await self._client.publish(
                binding,
                credentials,
                claimed.draft,
                publication_id=publication_id,
                reader=self._reader,
                subject_id=subject_id or binding.subject_id,
                now=moment,
            )
        except QuotaDeferred as exc:
            not_before = exc.not_before if isinstance(exc.not_before, datetime) else None
            await self._settle(
                publication_id,
                attempt_token,
                "pending",
                {"not_before": not_before, "attempt_token": None, "lease_deadline": None},
                exc,
            )
            raise
        except PublishOutcomeUnknown as exc:
            await self._settle(publication_id, attempt_token, "unknown", {}, exc)
            raise
        except (ProviderRejected, ValidationFailure, AuthorForbidden, CredentialExpired, CredentialMissing) as exc:
            await self._settle(publication_id, attempt_token, "failed", {}, exc)
            raise
        except Exception as exc:  # noqa: BLE001 - an unclassified crash is never "did not send"
            unknown = PublishOutcomeUnknown(f"unclassified failure during send: {type(exc).__name__}")
            await self._settle(publication_id, attempt_token, "unknown", {}, unknown)
            raise unknown from exc

        # Persist the receipt immediately, before any export or checkpoint. If
        # this write fails the record stays `sending` and recovery marks it
        # unknown — which is correct: LinkedIn has the post, we lost the proof.
        await self._store.compare_and_set(
            publication_id,
            expected_state="sending",
            expected_attempt_token=attempt_token,
            updates={
                "state": "published",
                "post_urn": receipt.post_urn,
                "permalink": receipt.permalink,
                "adapter": receipt.adapter,
                "published_at": receipt.published_at,
                "attempt_token": None,
                "lease_deadline": None,
            },
        )
        await self._event(publication_id, "published", {"post_urn": receipt.post_urn})
        return receipt

    # ------------------------------------------------------------------ gates

    async def _authorize_send(
        self, record: PublicationRecord, binding: AccountBinding, now: datetime
    ) -> str | None:
        """Re-check approval, window, kill switch and capability.

        Returns a commissioning grant id when publishing is disabled but this one
        record carries a valid grant. Raises otherwise.
        """
        approval = await self._store.get_approval(record.publication_id)
        if approval is None or not approval.is_active(digest=record.payload_digest, now=now):
            raise ValidationFailure(
                f"publication {record.publication_id} has no active approval for its current payload"
            )
        if approval.binding_id != record.binding_id:
            raise AuthorForbidden("the approval on this publication was issued for a different binding")
        if not record.is_due(now):
            raise ValidationFailure(
                f"publication {record.publication_id} is outside its approved window"
            )

        self._client.check(binding, record.draft)

        if binding.publish_enabled:
            return None

        grant = await self._store.get_grant(record.publication_id)
        if grant is not None and grant.is_usable(
            publication_id=record.publication_id,
            digest=record.payload_digest,
            binding_id=record.binding_id,
            now=now,
        ):
            return grant.grant_id
        raise CapabilityUnavailable(
            f"publishing is disabled on binding {binding.binding_id} and this publication "
            "carries no valid commissioning grant",
            capability=record.draft.required_capability,
        )

    # ------------------------------------------------------------- recovery

    async def _recover_leases(self, binding: AccountBinding, now: datetime) -> None:
        """Reclaim dead attempts.

        A `claimed` record whose lease died never began a send, so it returns to
        `pending`. A `sending` record whose lease died **may** have been
        accepted, so it becomes `unknown` and waits for a human. These two are
        not symmetric and must never be collapsed.
        """
        for record in await self._all_for(binding):
            if record.lease_deadline is None or record.lease_deadline > now:
                continue
            if record.state == "claimed":
                await self._safe_cas(
                    record,
                    "claimed",
                    {"state": "pending", "attempt_token": None, "lease_deadline": None},
                    "lease_recovered",
                )
            elif record.state == "sending":
                await self._safe_cas(
                    record,
                    "sending",
                    {
                        "state": "unknown",
                        "failure_code": "publish_outcome_unknown",
                        "failure_detail": "attempt lease expired mid-send",
                        "attempt_token": None,
                        "lease_deadline": None,
                    },
                    "lease_expired_unknown",
                )

    async def _expire_overdue(
        self, binding: AccountBinding, now: datetime, result: TickResult
    ) -> None:
        for record in await self._all_for(binding):
            if record.state == "pending" and record.is_expired(now):
                await self._safe_cas(record, "pending", {"state": "expired"}, "expired")
                result.expired += 1

    # --------------------------------------------------------- reconciliation

    async def reconcile(
        self,
        publication_id: str,
        *,
        actor: str,
        post_urn: str | None = None,
        failure_reason: str | None = None,
        now: datetime | None = None,
    ) -> PublicationRecord:
        """Record an operator's authenticated evidence about an unknown send.

        Either attach a verified URN the operator saw on LinkedIn, or mark the
        attempt failed with a reason. There is deliberately no automated
        search-by-text reconciliation: member reads are not available on the
        pilot's scopes, and guessing which post is ours is worse than asking.
        """
        moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        record = await self._require(publication_id)
        if record.state != "unknown":
            raise ValidationFailure(
                f"publication {publication_id} is {record.state}; only unknown records are reconciled"
            )
        if (post_urn is None) == (failure_reason is None):
            raise ValidationFailure("reconcile requires exactly one of post_urn or failure_reason")

        if post_urn is not None:
            updated = await self._store.compare_and_set(
                publication_id,
                expected_state="unknown",
                expected_attempt_token=None,
                updates={
                    "state": "published",
                    "post_urn": post_urn,
                    "permalink": f"https://www.linkedin.com/feed/update/{post_urn}/",
                    "published_at": moment,
                },
            )
            await self._event(publication_id, "reconciled_published", {"post_urn": post_urn}, actor)
            return updated

        updated = await self._store.compare_and_set(
            publication_id,
            expected_state="unknown",
            expected_attempt_token=None,
            updates={"state": "failed", "failure_code": "operator_disposition", "failure_detail": failure_reason},
        )
        await self._event(publication_id, "reconciled_failed", {"reason": failure_reason}, actor)
        return updated

    async def cancel(self, publication_id: str, *, actor: str) -> str:
        """Cancel a publication if it has not passed the sending boundary.

        Past that boundary the honest answers are `too_late` and
        `outcome_pending` — never a promise that the post was stopped.
        """
        record = await self._require(publication_id)
        if record.state in {"published", "failed", "expired", "cancelled"}:
            return "too_late"
        if record.state == "sending":
            return "outcome_pending"
        if record.state == "unknown":
            return "outcome_pending"
        await self._store.compare_and_set(
            publication_id,
            expected_state=record.state,
            expected_attempt_token=None,
            updates={"state": "cancelled", "attempt_token": None, "lease_deadline": None},
        )
        await self._event(publication_id, "cancelled", {}, actor)
        return "cancelled"

    # ----------------------------------------------------------------- helpers

    async def _require(self, publication_id: str) -> PublicationRecord:
        record = await self._store.get_publication(publication_id)
        if record is None:
            raise ValidationFailure(f"unknown publication {publication_id}")
        return record

    async def _all_for(self, binding: AccountBinding) -> list[PublicationRecord]:
        return await self._store.all_for_binding(binding.binding_id)

    async def _safe_cas(
        self,
        record: PublicationRecord,
        expected: str,
        updates: dict[str, object],
        event: str,
    ) -> None:
        try:
            await self._store.compare_and_set(
                record.publication_id,
                expected_state=expected,  # type: ignore[arg-type]
                expected_attempt_token=None,
                updates=updates,
            )
        except StoreConflict:
            return
        await self._event(record.publication_id, event, {})

    async def _settle(
        self,
        publication_id: str,
        attempt_token: str,
        state: str,
        updates: dict[str, object],
        exc: LinkedInPublishError,
    ) -> None:
        payload: dict[str, object] = {
            "state": state,
            "failure_code": exc.code,
            "failure_detail": exc.message,
            **updates,
        }
        payload.setdefault("attempt_token", None)
        payload.setdefault("lease_deadline", None)
        try:
            await self._store.compare_and_set(
                publication_id,
                expected_state="sending",
                expected_attempt_token=attempt_token,
                updates=payload,
            )
        except StoreConflict:
            return
        await self._event(publication_id, state, exc.as_dict())

    async def _event(
        self,
        publication_id: str,
        kind: str,
        detail: dict[str, object],
        actor: str | None = None,
    ) -> None:
        await self._store.append_event(
            PublicationEvent(
                publication_id=publication_id,
                at=datetime.now(timezone.utc),
                kind=kind,
                actor=actor or self._actor,
                detail=detail,
            )
        )
