# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportMissingTypeStubs=false
#
# asyncpg ships no type information (see linkedin_publish/postgres.py); every
# value crossing back out of this module is re-validated through a Pydantic model.
"""Postgres-backed neutral ledger over the existing `linkedin_publish` schema (migration 0006).

Same tables, same roles, same compare-and-set shape as the LinkedIn store:
`UPDATE ... WHERE state = $ AND attempt_token IS NOT DISTINCT FROM $ RETURNING *`.
This store dispatches **Meta** rows. LinkedIn rows stay with
`linkedin_publish.PublicationService`; reading one here is fine (its `post_urn`
is presented as `remote_id`), but the locator constraint refuses a `remote_id`
on a LinkedIn row, so this store can never write a LinkedIn receipt into the
wrong column.

`import_intent` is one transaction: lock the intent's rows, decide (same rule as
the in-memory store), cancel a superseded pending row, insert the publication,
its approval and the `imported` event. It needs the writer role (`approvals`
INSERT); the runtime role's attempt fails in the database.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .errors import ApprovalRefused, StoreConflict
from .ledger import ImportOutcome, SocialApproval, SocialEvent, check_updates, decide_import
from .model import OperationCapability, SocialBinding, SocialRecord
from .states import SocialState, allowed

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg

__all__ = ["PostgresSocialBindingStore", "PostgresSocialStore", "SOCIAL_MIGRATION"]

#: The migration that admits Meta rows. Meta dispatch refuses to start below it.
SOCIAL_MIGRATION = "0006_social_platforms"

_SELECT = "SELECT * FROM linkedin_publish.publications"


def _json(value: Any) -> Any:
    if isinstance(value, (str, bytes)):
        return json.loads(value)
    return value


def _operation_format(platform: str, payload: dict[str, Any]) -> tuple[str, str]:
    if platform == "linkedin":
        media = payload.get("media")
        kind = media.get("kind") if isinstance(media, dict) else None
        return "publish", str(kind or "text")
    return str(payload.get("action")), str(payload.get("format"))


def _as_social(row: Any) -> SocialRecord:
    payload = _json(row["draft"]) or {}
    platform = row["platform"]
    operation, fmt = _operation_format(platform, payload)
    origin = _json(row["origin"])
    return SocialRecord.model_validate(
        {
            "publication_id": row["publication_id"],
            "key": {
                "subject_kind": row["subject_kind"],
                "subject_id": row["subject_id"],
                "campaign_id": row["campaign_id"],
                "post_id": row["post_id"],
                "account_id": row["account_id"],
            },
            "binding_id": row["binding_id"],
            "platform": platform,
            "payload": payload,
            "payload_digest": row["payload_digest"],
            "operation": operation,
            "format": fmt,
            "state": row["state"],
            "scheduled_at": row["scheduled_at"],
            "expires_at": row["expires_at"],
            "not_before": row["not_before"],
            "attempt_token": row["attempt_token"],
            "lease_deadline": row["lease_deadline"],
            "attempts": row["attempts"],
            "remote_id": row["post_urn"] if platform == "linkedin" else row["remote_id"],
            "container_id": row["container_id"],
            "permalink": row["permalink"],
            "adapter": row["adapter"],
            "published_at": row["published_at"],
            "failure_code": row["failure_code"],
            "failure_detail": row["failure_detail"],
            "intent_id": row["intent_id"],
            "origin": origin,
            "replaces": row["replaces"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    )


async def require_social_schema(pool: asyncpg.Pool) -> None:
    """Refuse Meta work against a ledger that has not taken 0006. Issues no DDL."""
    row = await pool.fetchrow("SELECT 1 FROM linkedin_publish.schema_migrations WHERE version = $1", SOCIAL_MIGRATION)
    if row is None:
        raise StoreConflict(
            f"the ledger lacks migration {SOCIAL_MIGRATION}; run `linkedin-publish db migrate` under the "
            "migration identity before any Meta binding is used"
        )


class PostgresSocialStore:
    """The durable neutral ledger. Satisfies `SocialStore`."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def import_intent(
        self, record: SocialRecord, approval: SocialApproval, *, actor: str, at: datetime
    ) -> ImportOutcome:
        import asyncpg as driver

        try:
            async with self._pool.acquire() as connection, connection.transaction():
                rows = await connection.fetch(f"{_SELECT} WHERE intent_id = $1 FOR UPDATE", record.intent_id)
                existing = [_as_social(row) for row in rows]
                same, supersede = decide_import(existing, record)
                if same is not None:
                    return ImportOutcome(record=same, created=False)
                superseded: str | None = None
                if supersede is not None:
                    done = await connection.fetchrow(
                        "UPDATE linkedin_publish.publications SET state = 'cancelled', failure_code = 'superseded', "
                        "updated_at = now() WHERE publication_id = $1 AND state = 'pending' RETURNING publication_id",
                        supersede.publication_id,
                    )
                    if done is None:
                        raise ApprovalRefused("old_action_unresolved", "the earlier row moved during the import")
                    await self._event(
                        connection,
                        SocialEvent(publication_id=supersede.publication_id, at=at, kind="superseded", actor=actor),
                    )
                    superseded = supersede.publication_id
                row = await connection.fetchrow(
                    """
                    INSERT INTO linkedin_publish.publications (
                        publication_id, subject_kind, subject_id, campaign_id, post_id, account_id,
                        binding_id, payload_digest, draft, state, scheduled_at, expires_at,
                        platform, remote_id, container_id, adapter, failure_code, intent_id, origin
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb)
                    RETURNING *
                    """,
                    record.publication_id,
                    record.key.subject_kind,
                    record.key.subject_id,
                    record.key.campaign_id,
                    record.key.post_id,
                    record.key.account_id,
                    record.binding_id,
                    record.payload_digest,
                    json.dumps(record.payload),
                    record.state,
                    record.scheduled_at,
                    record.expires_at,
                    record.platform,
                    record.remote_id,
                    record.container_id,
                    record.adapter,
                    record.failure_code,
                    record.intent_id,
                    json.dumps(record.origin) if record.origin is not None else None,
                )
                await connection.execute(
                    "INSERT INTO linkedin_publish.approvals "
                    "(approval_id, publication_id, payload_digest, binding_id, approved_by, approved_at) "
                    "VALUES ($1,$2,$3,$4,$5,$6)",
                    approval.approval_id,
                    approval.publication_id,
                    approval.payload_digest,
                    approval.binding_id,
                    approval.approved_by,
                    approval.approved_at,
                )
                await self._event(
                    connection,
                    SocialEvent(
                        publication_id=record.publication_id,
                        at=at,
                        kind="imported",
                        actor=actor,
                        detail={"state": record.state, "intent_id": record.intent_id},
                    ),
                )
                return ImportOutcome(record=_as_social(row), created=True, superseded=superseded)
        except driver.exceptions.UniqueViolationError as exc:
            # A concurrent import won the race. Same approval: it is the owner.
            again = await self._pool.fetch(f"{_SELECT} WHERE intent_id = $1", record.intent_id)
            for row in again:
                if row["payload_digest"] == record.payload_digest:
                    return ImportOutcome(record=_as_social(row), created=False)
            raise ApprovalRefused("approval_conflict", "another approval owns this intent or natural key") from exc

    async def get(self, publication_id: str) -> SocialRecord | None:
        row = await self._pool.fetchrow(f"{_SELECT} WHERE publication_id = $1", publication_id)
        return _as_social(row) if row else None

    async def live_for_intent(self, intent_id: str) -> SocialRecord | None:
        row = await self._pool.fetchrow(
            f"{_SELECT} WHERE intent_id = $1 AND state NOT IN ('cancelled','expired','failed')", intent_id
        )
        return _as_social(row) if row else None

    async def all_for_binding(self, binding_id: str) -> list[SocialRecord]:
        rows = await self._pool.fetch(f"{_SELECT} WHERE binding_id = $1", binding_id)
        return [_as_social(row) for row in rows]

    async def find_due(self, *, binding_id: str, now: datetime, limit: int) -> list[SocialRecord]:
        rows = await self._pool.fetch(
            f"""
            {_SELECT}
             WHERE binding_id = $1 AND state = 'pending'
               AND scheduled_at <= $2 AND expires_at > $2
               AND (not_before IS NULL OR not_before <= $2)
             ORDER BY scheduled_at
             LIMIT $3
            """,
            binding_id,
            now,
            limit,
        )
        return [_as_social(row) for row in rows]

    async def compare_and_set(
        self,
        publication_id: str,
        *,
        expected_state: SocialState,
        expected_attempt_token: str | None,
        updates: dict[str, object],
    ) -> SocialRecord:
        check_updates(updates)
        target = updates.get("state")
        if isinstance(target, str) and not allowed(expected_state, target):
            raise StoreConflict(f"illegal transition {expected_state} -> {target}")
        columns = sorted(updates)
        # Column names come from `UPDATABLE`, a closed literal set checked above;
        # every value is a bound parameter.
        assignments = ", ".join(f"{name} = ${index + 4}" for index, name in enumerate(columns))
        sql = (
            "UPDATE linkedin_publish.publications "  # noqa: S608 - identifiers from a closed literal set
            f"SET {assignments}, updated_at = now() "
            "WHERE publication_id = $1 AND state = $2 "
            "AND ($3::text IS NULL OR attempt_token IS NOT DISTINCT FROM $3) "
            "RETURNING *"
        )
        row = await self._pool.fetchrow(
            sql, publication_id, expected_state, expected_attempt_token, *[updates[name] for name in columns]
        )
        if row is None:
            raise StoreConflict(f"publication {publication_id} is not in {expected_state} for this attempt")
        return _as_social(row)

    async def append_event(self, event: SocialEvent) -> None:
        await self._event(self._pool, event)

    @staticmethod
    async def _event(target: Any, event: SocialEvent) -> None:
        await target.execute(
            "INSERT INTO linkedin_publish.publication_events (publication_id, at, kind, actor, detail) "
            "VALUES ($1,$2,$3,$4,$5::jsonb)",
            event.publication_id,
            event.at,
            event.kind,
            event.actor,
            json.dumps(event.detail, default=str),
        )

    async def events(self, publication_id: str) -> list[SocialEvent]:
        rows = await self._pool.fetch(
            "SELECT publication_id, at, kind, actor, detail FROM linkedin_publish.publication_events "
            "WHERE publication_id = $1 ORDER BY at, event_id",
            publication_id,
        )
        return [
            SocialEvent(
                publication_id=row["publication_id"],
                at=row["at"],
                kind=row["kind"],
                actor=row["actor"],
                detail=_json(row["detail"]) or {},
            )
            for row in rows
        ]

    async def get_approval(self, publication_id: str) -> SocialApproval | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM linkedin_publish.approvals WHERE publication_id = $1 ORDER BY approved_at DESC LIMIT 1",
            publication_id,
        )
        return SocialApproval.model_validate(dict(row)) if row else None


def _as_binding(row: Any) -> SocialBinding:
    capabilities = _json(row["capabilities"]) or []
    return SocialBinding.model_validate(
        {
            "binding_id": row["binding_id"],
            "account_id": row["account_id"],
            "subject_kind": row["subject_kind"],
            "subject_id": row["subject_id"],
            "app_id": row["app_id"],
            "platform": row["platform"],
            "adapter": row["adapter"],
            "native_account_id": row["native_account_id"],
            "author_urn": row["author_urn"],
            "credential_ref": row["credential_ref"],
            "credential_version": row["credential_version"],
            "token_expires_at": row["token_expires_at"],
            "token_observed_at": row["token_observed_at"],
            "capabilities": [OperationCapability.model_validate(item) for item in capabilities if "operation" in item],
            "publish_enabled": row["publish_enabled"],
        }
    )


class PostgresSocialBindingStore:
    """Meta bindings on `account_bindings`. Writes need the writer role."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def register(self, binding: SocialBinding) -> bool:
        """Insert a DISABLED binding with every capability unknown. Re-registration is a no-op."""
        if binding.platform == "linkedin":
            raise StoreConflict("LinkedIn bindings register through linkedin_publish (`account register`)")
        row = await self._pool.fetchrow(
            """
            INSERT INTO linkedin_publish.account_bindings (
                binding_id, account_id, subject_kind, subject_id, app_id, author_urn, adapter,
                credential_ref, credential_version, platform, native_account_id
            ) VALUES ($1,$2,$3,$4,$5,NULL,$6,$7,$8,$9,$10)
            ON CONFLICT (binding_id) DO NOTHING
            RETURNING binding_id
            """,
            binding.binding_id,
            binding.account_id,
            binding.subject_kind,
            binding.subject_id,
            binding.app_id,
            binding.adapter,
            binding.credential_ref,
            binding.credential_version,
            binding.platform,
            binding.native_account_id,
        )
        if row is not None:
            return True
        existing = await self.get(binding.binding_id)
        if existing is None or (existing.platform, existing.native_account_id, existing.credential_version) != (
            binding.platform,
            binding.native_account_id,
            binding.credential_version,
        ):
            raise StoreConflict(f"binding {binding.binding_id!r} exists with another identity")
        return False

    async def get(self, binding_id: str) -> SocialBinding | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM linkedin_publish.account_bindings WHERE binding_id = $1", binding_id
        )
        return _as_binding(row) if row else None

    async def disable(self, binding_id: str) -> SocialBinding:
        """The rollback switch: publishing off. Read-back and reconciliation continue."""
        row = await self._pool.fetchrow(
            "UPDATE linkedin_publish.account_bindings SET publish_enabled = false, updated_at = now() "
            "WHERE binding_id = $1 RETURNING *",
            binding_id,
        )
        if row is None:
            raise StoreConflict(f"binding {binding_id!r} is not registered")
        return _as_binding(row)

    async def disable_platform(self, platform: str) -> int:
        """Disable every binding of one Meta platform at once (the C5a rollback)."""
        if platform == "linkedin":
            raise StoreConflict("the Meta rollback never touches LinkedIn bindings")
        status = await self._pool.execute(
            "UPDATE linkedin_publish.account_bindings SET publish_enabled = false, updated_at = now() "
            "WHERE platform = $1 AND publish_enabled",
            platform,
        )
        return int(str(status).rsplit(" ", 1)[-1])
