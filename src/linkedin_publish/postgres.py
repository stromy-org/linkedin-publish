# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportMissingTypeStubs=false
#
# asyncpg ships no type information, so every `Pool`/`Connection` method it
# exposes is Unknown under strict mode. The suppressions above are scoped to this
# one module — the only place the untyped driver is touched — and every value
# crossing back out of here is re-validated through a Pydantic model.
"""Postgres-backed ledger and budgets (the `postgres` extra).

Migrations are explicit and run under an advisory lock with a checksum ledger —
never on first call, and never by the runtime identity. At runtime the code
*checks* compatibility and refuses to start if the schema is behind; it issues no
DDL, because a process that can reshape its own tables is a process whose least
privilege means nothing.

Every compare-and-set is a single `UPDATE ... WHERE state = $ AND attempt_token
IS NOT DISTINCT FROM $ RETURNING *`. That is the whole concurrency story: two
replicas racing for the same row, one row updated, the loser told so. No
transaction here spans a LinkedIn request.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._json import loads_object
from .exceptions import DependencyError
from .limits import BudgetKey, QuotaExhausted
from .store import (
    ApprovalRecord,
    CommissioningGrant,
    PublicationEvent,
    PublicationKey,
    PublicationRecord,
    PublicationState,
    StoreConflict,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg

__all__ = [
    "MIGRATIONS_DIR",
    "PostgresBudgetStore",
    "PostgresPublicationStore",
    "apply_migrations",
    "check_compatible",
    "migration_files",
]

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

#: Bumped when a migration lands. Runtime refuses to start below this.
REQUIRED_MIGRATION = "0002_roles"

#: One fixed key so concurrent `db migrate` invocations serialize rather than
#: racing two DDL streams into the same schema.
_MIGRATION_LOCK_KEY = 0x1_4E_D1_9B


def _require_asyncpg() -> Any:
    try:
        import asyncpg as module
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by the extra test
        raise DependencyError("postgres", "asyncpg") from exc
    return module


def migration_files() -> list[Path]:
    """Migrations in lexical order. The filename *is* the version."""
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def apply_migrations(dsn: str, *, applied_by: str = "migrator") -> list[str]:
    """Apply pending migrations. Returns the versions applied this run.

    Run explicitly, under a migration identity — not the runtime role. A
    previously applied migration whose file has since changed is a hard error:
    silently re-running an edited migration is how two environments diverge while
    both report "up to date".
    """
    asyncpg = _require_asyncpg()
    applied: list[str] = []
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_KEY)
        await connection.execute("CREATE SCHEMA IF NOT EXISTS linkedin_publish")
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS linkedin_publish.schema_migrations (
                version    text PRIMARY KEY,
                checksum   text        NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now(),
                applied_by text        NOT NULL DEFAULT current_user
            )
            """
        )
        recorded = {
            row["version"]: row["checksum"]
            for row in await connection.fetch(
                "SELECT version, checksum FROM linkedin_publish.schema_migrations"
            )
        }

        for path in migration_files():
            version = path.stem
            checksum = _checksum(path)
            if version in recorded:
                if recorded[version] != checksum:
                    raise StoreConflict(
                        f"migration {version} was applied with a different checksum; "
                        "an applied migration is immutable — add a new one instead"
                    )
                continue
            async with connection.transaction():
                await connection.execute(path.read_text())
                await connection.execute(
                    "INSERT INTO linkedin_publish.schema_migrations (version, checksum, applied_by) "
                    "VALUES ($1, $2, $3)",
                    version,
                    checksum,
                    applied_by,
                )
            applied.append(version)
        return applied
    finally:
        await connection.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY)
        await connection.close()


async def check_compatible(pool: asyncpg.Pool) -> None:
    """Assert the schema is at or past `REQUIRED_MIGRATION`. Issues no DDL."""
    row = await pool.fetchrow(
        "SELECT max(version) AS version FROM linkedin_publish.schema_migrations"
    )
    current = row["version"] if row else None
    if current is None or current < REQUIRED_MIGRATION:
        raise StoreConflict(
            f"linkedin_publish schema is at {current!r}; this build requires {REQUIRED_MIGRATION!r}. "
            "Run `linkedin-publish db migrate` under the migration identity."
        )


def _as_record(row: Any) -> PublicationRecord:
    # asyncpg hands back `jsonb` as a STRING unless a type codec is registered,
    # so `draft` arrives as raw JSON text and Pydantic rejects it outright. Decode
    # explicitly rather than installing a global codec: the conversion is visible
    # at the one place it matters, instead of being ambient pool state a reader
    # has to go looking for. Caught by the integration tier on its first run.
    draft = row["draft"]
    if isinstance(draft, (str, bytes)):
        draft = loads_object(draft)

    return PublicationRecord.model_validate(
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
            "payload_digest": row["payload_digest"],
            "draft": draft,
            "state": row["state"],
            "scheduled_at": row["scheduled_at"],
            "expires_at": row["expires_at"],
            "not_before": row["not_before"],
            "attempt_token": row["attempt_token"],
            "lease_deadline": row["lease_deadline"],
            "attempts": row["attempts"],
            "post_urn": row["post_urn"],
            "permalink": row["permalink"],
            "adapter": row["adapter"],
            "published_at": row["published_at"],
            "failure_code": row["failure_code"],
            "failure_detail": row["failure_detail"],
            "replaces": row["replaces"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    )


_SELECT = "SELECT * FROM linkedin_publish.publications"

#: Columns a CAS may set. Mirrors the runtime role's column grants — the database
#: is the real boundary; this list keeps the error message useful.
_UPDATABLE = frozenset(
    {
        "state",
        "not_before",
        "attempt_token",
        "lease_deadline",
        "attempts",
        "post_urn",
        "permalink",
        "adapter",
        "published_at",
        "failure_code",
        "failure_detail",
    }
)


class PostgresPublicationStore:
    """The durable ledger. Satisfies `PublicationStore`."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def upsert_publication(self, record: PublicationRecord) -> PublicationRecord:
        existing = await self.find_by_key(record.key)
        if existing is not None:
            if existing.payload_digest != record.payload_digest:
                raise StoreConflict(
                    f"publication key {record.key} already exists with a different payload digest; "
                    "a revised payload needs a new publication id and its own approval"
                )
            return existing
        row = await self._pool.fetchrow(
            """
            INSERT INTO linkedin_publish.publications (
                publication_id, subject_kind, subject_id, campaign_id, post_id, account_id,
                binding_id, payload_digest, draft, state, scheduled_at, expires_at, replaces
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13)
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
            record.draft.model_dump_json(),
            record.state,
            record.scheduled_at,
            record.expires_at,
            record.replaces,
        )
        return _as_record(row)

    async def get_publication(self, publication_id: str) -> PublicationRecord | None:
        row = await self._pool.fetchrow(f"{_SELECT} WHERE publication_id = $1", publication_id)
        return _as_record(row) if row else None

    async def find_by_key(self, key: PublicationKey) -> PublicationRecord | None:
        row = await self._pool.fetchrow(
            f"{_SELECT} WHERE subject_kind=$1 AND subject_id=$2 AND campaign_id=$3 "
            "AND post_id=$4 AND account_id=$5",
            key.subject_kind,
            key.subject_id,
            key.campaign_id,
            key.post_id,
            key.account_id,
        )
        return _as_record(row) if row else None

    async def find_due(
        self, *, binding_id: str, campaign_id: str | None, now: datetime, limit: int
    ) -> list[PublicationRecord]:
        rows = await self._pool.fetch(
            f"""
            {_SELECT}
             WHERE binding_id = $1
               AND state = 'pending'
               AND ($2::text IS NULL OR campaign_id = $2)
               AND scheduled_at <= $3
               AND expires_at > $3
               AND (not_before IS NULL OR not_before <= $3)
             ORDER BY scheduled_at
             LIMIT $4
            """,
            binding_id,
            campaign_id,
            now,
            limit,
        )
        return [_as_record(row) for row in rows]

    async def all_for_binding(self, binding_id: str) -> list[PublicationRecord]:
        rows = await self._pool.fetch(f"{_SELECT} WHERE binding_id = $1", binding_id)
        return [_as_record(row) for row in rows]

    async def compare_and_set(
        self,
        publication_id: str,
        *,
        expected_state: PublicationState,
        expected_attempt_token: str | None,
        updates: dict[str, object],
    ) -> PublicationRecord:
        unknown = set(updates) - _UPDATABLE
        if unknown:
            raise StoreConflict(f"refusing to update non-delivery columns: {sorted(unknown)}")

        columns = sorted(updates)
        # Column names come from `_UPDATABLE`, a closed literal set validated
        # immediately above; every VALUE is a bound parameter. Nothing
        # caller-supplied reaches the SQL text.
        assignments = ", ".join(f"{name} = ${index + 4}" for index, name in enumerate(columns))
        sql = (
            "UPDATE linkedin_publish.publications "  # noqa: S608 - identifiers from a closed literal set; all values bound
            f"SET {assignments}, updated_at = now() "
            "WHERE publication_id = $1 "
            "AND state = $2 "
            "AND ($3::text IS NULL OR attempt_token IS NOT DISTINCT FROM $3) "
            "RETURNING *"
        )
        row = await self._pool.fetchrow(
            sql,
            publication_id,
            expected_state,
            expected_attempt_token,
            *[updates[name] for name in columns],
        )
        if row is None:
            raise StoreConflict(
                f"publication {publication_id} is not in {expected_state} for this attempt"
            )
        return _as_record(row)

    async def append_event(self, event: PublicationEvent) -> None:
        await self._pool.execute(
            "INSERT INTO linkedin_publish.publication_events (publication_id, at, kind, actor, detail) "
            "VALUES ($1,$2,$3,$4,$5::jsonb)",
            event.publication_id,
            event.at,
            event.kind,
            event.actor,
            PublicationEvent.model_dump_json(event),
        )

    async def events(self, publication_id: str) -> list[PublicationEvent]:
        rows = await self._pool.fetch(
            "SELECT publication_id, at, kind, actor, detail FROM linkedin_publish.publication_events "
            "WHERE publication_id = $1 ORDER BY at, event_id",
            publication_id,
        )
        return [
            PublicationEvent(
                publication_id=row["publication_id"],
                at=row["at"],
                kind=row["kind"],
                actor=row["actor"],
                detail={},
            )
            for row in rows
        ]

    async def get_approval(self, publication_id: str) -> ApprovalRecord | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM linkedin_publish.approvals WHERE publication_id = $1 "
            "ORDER BY approved_at DESC LIMIT 1",
            publication_id,
        )
        return ApprovalRecord.model_validate(dict(row)) if row else None

    async def put_approval(self, approval: ApprovalRecord) -> None:
        await self._pool.execute(
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

    async def get_grant(self, publication_id: str) -> CommissioningGrant | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM linkedin_publish.commissioning_grants "
            "WHERE publication_id = $1 AND consumed_at IS NULL",
            publication_id,
        )
        return CommissioningGrant.model_validate(dict(row)) if row else None

    async def put_grant(self, grant: CommissioningGrant) -> None:
        await self._pool.execute(
            "INSERT INTO linkedin_publish.commissioning_grants "
            "(grant_id, publication_id, payload_digest, binding_id, capability, "
            " issued_by, issued_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            grant.grant_id,
            grant.publication_id,
            grant.payload_digest,
            grant.binding_id,
            grant.capability,
            grant.issued_by,
            grant.issued_at,
            grant.expires_at,
        )

    async def consume_grant(self, grant_id: str, *, at: datetime) -> bool:
        """Consume atomically: the `WHERE consumed_at IS NULL` is the whole guard."""
        row = await self._pool.fetchrow(
            "UPDATE linkedin_publish.commissioning_grants SET consumed_at = $2 "
            "WHERE grant_id = $1 AND consumed_at IS NULL RETURNING grant_id",
            grant_id,
            at,
        )
        return row is not None


class PostgresBudgetStore:
    """Atomic, shared request counters. Satisfies `BudgetStore`."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def reserve(self, reservations: Sequence[tuple[BudgetKey, int]]) -> None:
        """All-or-nothing across every counter, in one transaction.

        Rows are locked in a deterministic order (the caller builds them app,
        member, endpoint) so two replicas reserving the same pair cannot deadlock
        by taking them in opposite orders.
        """
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                for key, limit in reservations:
                    row = await connection.fetchrow(
                        """
                        INSERT INTO linkedin_publish.request_budgets (scope, identity, endpoint, day, used)
                        VALUES ($1,$2,$3,$4,1)
                        ON CONFLICT (scope, identity, endpoint, day)
                        DO UPDATE SET used = linkedin_publish.request_budgets.used + 1,
                                      updated_at = now()
                              WHERE linkedin_publish.request_budgets.used < $5
                        RETURNING used
                        """,
                        key.scope,
                        key.identity,
                        key.endpoint or "",
                        key.day,
                        limit,
                    )
                    if row is None:
                        # The transaction rolls back, so no partial reservation
                        # survives — a refused third request has not silently
                        # spent the first two counters.
                        raise QuotaExhausted(key, limit)

    async def usage(self, key: BudgetKey) -> int:
        row = await self._pool.fetchrow(
            "SELECT used FROM linkedin_publish.request_budgets "
            "WHERE scope=$1 AND identity=$2 AND endpoint=$3 AND day=$4",
            key.scope,
            key.identity,
            key.endpoint or "",
            key.day,
        )
        return int(row["used"]) if row else 0


def utc_day(moment: datetime) -> date:
    """The UTC date a counter is keyed by. Never the local date."""
    return moment.astimezone(timezone.utc).date()
