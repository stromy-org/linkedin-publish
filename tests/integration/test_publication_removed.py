"""Migration 0008 and `publication record-removed` against real Postgres (Fleet Social T29).

Upgrade from a populated 0007 ledger (the state CHECK refuses `removed` until
0008 lands, and the new build says so), then the disposition under the real
writer role: the receipt stays, the event records who/why, and nothing else moves.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from tests.integration.test_social_migration import (
    META,
    PUB_SQL,
    _linkedin_binding,
    as_role,
    populate_linkedin,
    scratch,
)

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)
REMOVED = "0008_publication_removed"
POST = "urn:li:share:7532062215332614145"
DRAFT = '{"author_urn": "urn:li:person:x", "commentary": "Intelligence, orchestrated."}'


async def test_0008_ships_after_0007_and_a_populated_0007_ledger_upgrades(pool, monkeypatch) -> None:  # noqa: ANN001
    from linkedin_publish import postgres as pg

    shipped = {p.stem: hashlib.sha256(p.read_bytes()).hexdigest() for p in pg.migration_files()}
    assert max(shipped) == REMOVED
    async with scratch(pool, monkeypatch, upto=META) as (target, db):
        binding = "bind_mbogl7_D8GJm"
        await populate_linkedin(db)
        await db.execute(
            PUB_SQL, "pub-live", binding, "published", NOW, NOW + timedelta(hours=1), "linkedin", POST, None
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):  # 0007 has no `removed`
            await db.execute("UPDATE linkedin_publish.publications SET state='removed' WHERE publication_id='pub-live'")
        assert await pg.apply_migrations(target, applied_by="pytest") == [REMOVED]
        assert await pg.apply_migrations(target, applied_by="pytest") == []  # idempotent
        await db.execute("UPDATE linkedin_publish.publications SET state='removed' WHERE publication_id='pub-live'")
        row = await db.fetchrow(
            "SELECT state, post_urn FROM linkedin_publish.publications WHERE publication_id='pub-live'"
        )
        assert (row["state"], row["post_urn"]) == ("removed", POST)
        with pytest.raises(asyncpg.exceptions.CheckViolationError):  # still a closed set
            await db.execute("UPDATE linkedin_publish.publications SET state='deleted' WHERE publication_id='pub-live'")


async def test_the_writer_records_a_removal_and_keeps_the_receipt(pool) -> None:  # noqa: ANN001
    from linkedin_publish import ValidationFailure
    from linkedin_publish.postgres import PostgresPublicationStore
    from linkedin_publish.service import record_publication_removed

    await _linkedin_binding(pool)
    window = (NOW, NOW + timedelta(hours=1))
    for pub, state, urn in (("pub-live", "published", POST), ("pub-open", "unknown", None)):
        await pool.execute(PUB_SQL, pub, "bind-li", state, *window, "linkedin", urn, None)
        await pool.execute(
            "UPDATE linkedin_publish.publications SET draft=$2::jsonb WHERE publication_id=$1", pub, DRAFT
        )
    await pool.execute("UPDATE linkedin_publish.publications SET published_at=$1 WHERE publication_id='pub-live'", NOW)
    writer = await as_role(pool, "linkedin_publish_writer")
    try:
        store = PostgresPublicationStore(writer)
        with pytest.raises(ValidationFailure, match="only published records"):
            await record_publication_removed(store, "pub-open", actor="william", reason="gone")
        updated = await record_publication_removed(
            store, "pub-live", actor="william", reason="deleted by the account owner on LinkedIn"
        )
        assert (updated.state, updated.post_urn, updated.published_at) == ("removed", POST, NOW)
        (event,) = await store.events("pub-live")
        assert (event.kind, event.actor) == ("removed_externally", "william")
        stored = json.loads(
            await pool.fetchval(
                "SELECT detail FROM linkedin_publish.publication_events WHERE publication_id='pub-live'"
            )
        )
        assert stored["detail"]["reason"] == "deleted by the account owner on LinkedIn"
        assert stored["detail"]["post_urn"] == POST
        with pytest.raises(ValidationFailure, match="only published records"):  # terminal: a second call refuses
            await record_publication_removed(store, "pub-live", actor="william", reason="again")
    finally:
        await writer.close()
    assert await pool.fetchval("SELECT state FROM linkedin_publish.publications WHERE publication_id='pub-open'") == (
        "unknown"
    )
