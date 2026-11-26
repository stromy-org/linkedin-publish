"""Migration 0007 and Meta commissioning against real Postgres (ORG-PLAN-652 C5a, F32).

Upgrade from a populated 0006 ledger, a failed 0007 that rolls back whole and a
retry, old and new readers, the no-expiry CHECK, and the canary path under the
real roles: the writer issues the grant and records token evidence, the
commissioner spends the grant and writes the Meta locators, and a runtime
identity handed a commissioning actor is refused by the database before
`sending`. Roles are exercised as roles (`SET ROLE`), never as Python checks.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.integration.test_social_migration import (
    META,
    META_BINDING_SQL,
    PUB_SQL,
    SOCIAL,
    _linkedin_binding,
    as_role,
    assert_linkedin_intact,
    populate_linkedin,
    scratch,
)

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)
ACTOR = "fixture-operator"

#: sha256 of 0006 as shipped in v0.8.0 (81d0aee): an applied migration is immutable.
PINNED_0006 = "50607fb02e9a6ed401719a857480c3379c4e1461c5c6dfec26c645be833fdcc4"


def test_0006_is_unchanged_and_0007_ships() -> None:
    from linkedin_publish.postgres import migration_files

    shipped = {p.stem: hashlib.sha256(p.read_bytes()).hexdigest() for p in migration_files()}
    assert shipped[SOCIAL] == PINNED_0006
    assert max(shipped) == META


def evidence(*, never: bool = True, version: str = "v1"):  # noqa: ANN201
    from social_publish import parse_debug_token

    data = {
        "app_id": "app-organic",
        "is_valid": True,
        "expires_at": 0 if never else int((NOW + timedelta(days=50)).timestamp()),
    }
    return parse_debug_token({"data": data}, credential_version=version, observed_at=NOW, expected_app_id="app-organic")


async def test_a_populated_0006_ledger_upgrades_and_both_readers_still_read_it(pool, monkeypatch) -> None:  # noqa: ANN001
    from linkedin_publish import postgres as pg
    from social_publish.errors import StoreConflict
    from social_publish.postgres import PostgresSocialBindingStore

    async with scratch(pool, monkeypatch, upto=SOCIAL) as (target, db):
        before = await populate_linkedin(db)
        await db.execute(META_BINDING_SQL, "bind-fb", None, "meta_graph", "facebook_page", "100000000000001")
        await db.execute(
            PUB_SQL, "fb-1", "bind-fb", "scheduled", NOW, NOW + timedelta(hours=1), "facebook_page", None, "1_2"
        )
        before["fb-1"] = ("d", "{}")  # the Meta row, as 0006 holds it
        # A new reader on the old schema: the column is absent, the evidence is absent.
        old_schema = await PostgresSocialBindingStore(db).get("bind-fb")
        assert old_schema is not None and old_schema.token_never_expires is False
        with pytest.raises(StoreConflict, match=META):  # and nothing writes evidence below 0007
            await PostgresSocialBindingStore(db).record_token_evidence("bind-fb", evidence())

        assert await pg.apply_migrations(target, applied_by="pytest") == [META]

        await assert_linkedin_intact(db, before)  # the LinkedIn reader, unchanged
        upgraded = await PostgresSocialBindingStore(db).get("bind-fb")
        assert upgraded is not None and upgraded.token_never_expires is False
        assert await db.fetchval("SELECT state FROM linkedin_publish.publications WHERE publication_id='fb-1'") == (
            "scheduled"
        )
        assert await pg.apply_migrations(target, applied_by="pytest") == []  # idempotent
        for role in ("linkedin_publish_runtime", "linkedin_publish_writer", "linkedin_publish_commissioner"):
            async with db.acquire() as conn:
                await conn.execute(f"SET ROLE {role}")
                await pg.check_compatible(conn)
                assert await PostgresSocialBindingStore(conn).get("bind-fb") is not None
                await conn.execute("RESET ROLE")


async def test_a_failed_0007_rolls_back_whole_and_a_retry_applies(pool, monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    from linkedin_publish import postgres as pg

    async with scratch(pool, monkeypatch, upto=SOCIAL) as (target, db):
        real = pg.migration_files()
        good = next(p for p in real if p.stem == META)
        broken = tmp_path / good.name
        broken.write_text(good.read_text() + "\nSELECT 1 / 0;\n")
        monkeypatch.setattr(pg, "migration_files", lambda: [broken if p.stem == META else p for p in real])
        with pytest.raises(asyncpg.exceptions.DivisionByZeroError):
            await pg.apply_migrations(target, applied_by="pytest")
        applied = {r["version"] for r in await db.fetch("SELECT version FROM linkedin_publish.schema_migrations")}
        assert META not in applied and SOCIAL in applied
        cols = {
            r["column_name"]
            for r in await db.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='linkedin_publish' "
                "AND table_name='account_bindings'"
            )
        }
        assert "token_never_expires" not in cols  # nothing half-applied
        granted = await db.fetchval(
            "SELECT has_column_privilege('linkedin_publish_commissioner', 'linkedin_publish.publications', "
            "'remote_id', 'UPDATE')"
        )
        assert granted is False  # the grant rolled back with the rest

        monkeypatch.setattr(pg, "migration_files", lambda: real)
        assert await pg.apply_migrations(target, applied_by="pytest") == [META]
        assert await db.fetchval(
            "SELECT has_column_privilege('linkedin_publish_commissioner', 'linkedin_publish.publications', "
            "'container_id', 'UPDATE')"
        )
        async with db.acquire() as conn, conn.transaction():  # the file body re-runs harmlessly
            await conn.execute(good.read_text())


async def test_no_expiry_evidence_is_written_by_the_writer_and_kept_honest(pool) -> None:  # noqa: ANN001
    from social_publish.postgres import PostgresSocialBindingStore

    await _linkedin_binding(pool)
    await pool.execute(META_BINDING_SQL, "bind-fb", None, "meta_graph", "facebook_page", "100000000000001")
    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    commissioner = await as_role(pool, "linkedin_publish_commissioner")
    try:
        recorded = await PostgresSocialBindingStore(writer).record_token_evidence("bind-fb", evidence())
        assert recorded.token_never_expires and recorded.token_expires_at is None
        row = await pool.fetchrow(
            "SELECT token_active, reason, expires_at FROM linkedin_publish.token_observations "
            "WHERE binding_id='bind-fb'"
        )
        assert (row["token_active"], row["reason"], row["expires_at"]) == (True, "never_expires", None)

        measured = await PostgresSocialBindingStore(writer).record_token_evidence("bind-fb", evidence(never=False))
        assert not measured.token_never_expires and measured.token_expires_at == NOW + timedelta(days=50)

        for role_pool in (runtime, commissioner):
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await PostgresSocialBindingStore(role_pool).record_token_evidence("bind-fb", evidence())
        for statement in (
            "UPDATE linkedin_publish.account_bindings SET token_never_expires = true, token_observed_at = now() "
            "WHERE binding_id = 'bind-li'",  # LinkedIn tokens always expire
            "UPDATE linkedin_publish.account_bindings SET token_never_expires = true WHERE binding_id = 'bind-fb'",
            "UPDATE linkedin_publish.account_bindings SET token_never_expires = true, token_expires_at = NULL, "
            "token_observed_at = NULL WHERE binding_id = 'bind-fb'",  # never without its observation time
        ):
            with pytest.raises(asyncpg.exceptions.CheckViolationError):
                await pool.execute(statement)
    finally:
        await writer.close()
        await runtime.close()
        await commissioner.close()


async def test_a_meta_canary_runs_as_the_commissioner_and_a_runtime_spends_no_grant(pool) -> None:  # noqa: ANN001
    """F32 end to end: the writer grants, the commissioner sends and records the receipt
    (0007's locator grants), the runtime is refused at `consumed_at`, the writer enables."""
    from social_publish import DispatchService, Refused, enable_problems, new_grant
    from social_publish.postgres import PostgresSocialBindingStore, PostgresSocialStore
    from tests.social_fakes import IMAGE, FakeGraph, adapter, approval_for, meta_payload, record_for

    for platform in ("facebook_page", "instagram_professional"):
        native = "100000000000001" if platform == "facebook_page" else "170000000000001"
        await pool.execute(META_BINDING_SQL, f"bind-{platform}", None, "meta_graph", platform, native)
    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    commissioner = await as_role(pool, "linkedin_publish_commissioner")
    try:
        bindings = PostgresSocialBindingStore(writer)
        owner = PostgresSocialStore(writer)
        now = datetime.now(UTC)
        records = {}
        for platform, fmt, pub in (
            ("facebook_page", "text", "fb-rt"),
            ("facebook_page", "text", "fb-ok"),
            ("instagram_professional", "feed_image", "ig-ok"),
        ):
            binding = await bindings.record_token_evidence(f"bind-{platform}", evidence())
            payload = meta_payload(
                platform,
                fmt=fmt,
                caption=f"Fixture canary {pub}.",
                media=(IMAGE,) if fmt == "feed_image" else (),
                slot=now + timedelta(hours=2),
            )
            record = record_for(
                binding,
                payload,
                publication_id=pub,
                scheduled_at=now - timedelta(minutes=1),
                expires_at=now + timedelta(hours=1),
            )
            await owner.import_intent(record, approval_for(record), actor=ACTOR, at=now)
            await owner.put_grant(new_grant(record, actor=ACTOR, now=now))
            records[pub] = (binding, record)
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):  # only the writer issues grants
            await PostgresSocialStore(commissioner).put_grant(new_grant(records["fb-ok"][1], actor=ACTOR, now=now))

        graph = FakeGraph()
        graph.status_script = {"900000000000002": ["FINISHED"]}

        def service(role_pool):  # noqa: ANN001, ANN202
            return DispatchService(
                PostgresSocialStore(role_pool), {"meta_graph": adapter(graph)}, actor=ACTOR, commissioning_actor=ACTOR
            )

        binding, _ = records["fb-rt"]
        with pytest.raises(Refused, match="grant_unusable"):
            await service(runtime).dispatch_one(binding, "fb-rt")
        assert await pool.fetchval("SELECT state FROM linkedin_publish.publications WHERE publication_id='fb-rt'") == (
            "pending"
        )
        assert await PostgresSocialStore(pool).get_grant("fb-rt") is not None  # never spent
        assert graph.writes() == []

        binding, _ = records["fb-ok"]
        assert await service(commissioner).dispatch_one(binding, "fb-ok") == "scheduled"
        binding, _ = records["ig-ok"]
        assert await service(commissioner).dispatch_one(binding, "ig-ok") == "published"
        rows = {
            r["publication_id"]: (r["state"], r["remote_id"] is not None, r["container_id"])
            for r in await pool.fetch(
                "SELECT publication_id, state, remote_id, container_id FROM linkedin_publish.publications"
            )
        }
        assert rows["fb-ok"] == ("scheduled", True, None)
        assert rows["ig-ok"] == ("published", True, "900000000000002")
        assert len(graph.writes()) == 3  # feed, media, media_publish — and nothing for fb-rt

        # The writer enables exactly the receipt's pair.
        receipt = await owner.get("fb-ok")
        fb = await bindings.get("bind-facebook_page")
        assert receipt is not None and fb is not None
        assert (
            enable_problems(fb, receipt, await owner.events("fb-ok"), operation="schedule_post", format="text", now=now)
            == []
        )
        enabled = await bindings.enable_capability(
            "bind-facebook_page",
            operation="schedule_post",
            format="text",
            evidence_publication_id="fb-ok",
            observed_at=now,
            expected_version="v1",
        )
        assert enabled.publish_enabled and enabled.capability_state("schedule_post", "text") == "enabled"
        assert enabled.capability_state("schedule_post", "feed_image") == "unknown"
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):  # the commissioner cannot enable
            await PostgresSocialBindingStore(commissioner).enable_capability(
                "bind-instagram_professional",
                operation="publish_post",
                format="feed_image",
                evidence_publication_id="ig-ok",
                observed_at=now,
                expected_version="v1",
            )
    finally:
        await writer.close()
        await runtime.close()
        await commissioner.close()
