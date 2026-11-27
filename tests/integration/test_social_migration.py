"""Migration 0006 against real Postgres: upgrade, retry, old readers, Meta writers, roles, rollback.

Every upgrade test starts from a scratch database migrated to a real earlier
state — the v0.4.2 shape (0001–0004, byte-identical to pinned commit 548021b)
or the v0.7.0 shape (0001–0005) — and populated with LinkedIn rows the way a
live ledger holds them. Roles are exercised as roles (`SET ROLE`), never as
Python checks.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit, urlunsplit

import pytest

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)
PERSON = "urn:li:person:AbC123xyz"
SOCIAL = "0006_social_platforms"
META = "0007_meta_commissioning"

#: sha256 of each migration file at pinned commit 548021b (the v0.4.2 shape) and v0.7.0.
PINNED = {
    "0001_initial": "106e839c6e6437ce118dd8ae43a60016b8df0afd818bf1742310a8140e0b1166",
    "0002_roles": "e5b190138e91d41e1ab58d54e3ec59aced4d38e66c04939635f9163c1cbd77de",
    "0003_writer_reconcile": "cdab0c755a36c0029d321ec143f7d08121cc126140f63b580111231c93b53f41",
    "0004_roles_read_schema_version": "c9640cd4446e46e1ea273c837f47b58434ce514ea0650ccdf5acd7dad19084f5",
    "0005_token_health": "c620d0c1ceedd961ee996adfbe8f839f8ad1fb401cc68d88be07e4cadff24a3b",
}


def test_the_earlier_migrations_are_byte_identical_to_the_pinned_commits() -> None:
    from linkedin_publish.postgres import migration_files

    shipped = {p.stem: hashlib.sha256(p.read_bytes()).hexdigest() for p in migration_files()}
    assert {k: shipped[k] for k in PINNED} == PINNED
    assert SOCIAL in shipped


@asynccontextmanager
async def scratch(pool, monkeypatch, *, upto: str) -> AsyncIterator[tuple[str, object]]:  # noqa: ANN001
    """A fresh database migrated through `upto` (inclusive), and a pool on it."""
    from linkedin_publish import postgres as pg
    from tests.integration.conftest import dsn as base_dsn

    name = f"lp_social_{secrets.token_hex(4)}"
    await pool.execute(f"CREATE DATABASE {name}")
    target = urlunsplit(urlsplit(base_dsn())._replace(path=f"/{name}"))
    everything = pg.migration_files()
    monkeypatch.setattr(pg, "migration_files", lambda: [p for p in everything if p.stem <= upto])
    await pg.apply_migrations(target, applied_by="pytest")
    monkeypatch.setattr(pg, "migration_files", lambda: everything)
    created = await asyncpg.create_pool(target, min_size=1, max_size=8)
    try:
        yield target, created
    finally:
        await created.close()
        await pool.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


async def populate_linkedin(conn) -> dict[str, object]:  # noqa: ANN001
    """LinkedIn rows the way a live ledger holds them: binding, manifest, publications,
    approval, a consumed and an open grant, events, an upload and budgets."""
    draft = json.dumps({"author_urn": PERSON, "commentary": "Intelligence, orchestrated.", "visibility": "PUBLIC"})
    await conn.execute(
        """
        INSERT INTO linkedin_publish.account_bindings
            (binding_id, account_id, subject_kind, subject_id, app_id, author_urn, adapter,
             declared_scopes, observed_scopes, credential_ref, credential_version, capabilities, publish_enabled)
        VALUES ('bind_mbogl7_D8GJm','acct-1','entra_oid','subject-1','app-personal',$1,'share_ugc',
                '{w_member_social}','{w_member_social}','kv://linkedin/member','v1',
                '[{"capability":"text","state":"enabled","observed_at":"2026-10-11T00:00:00Z","evidence_publication_id":"canary-1"}]'::jsonb,
                false)
        """,
        PERSON,
    )
    await conn.execute(
        "INSERT INTO linkedin_publish.manifests VALUES ('man-1','camp-1','bind_mbogl7_D8GJm',$1,$2,now(),'william')",
        "d" * 64,
        b'{"schema_version":"1.0"}',
    )
    for pub, state, urn in (
        ("canary-1", "published", "urn:li:share:7100000000000000000"),
        ("pub-2", "pending", None),
        ("pub-3", "unknown", None),
    ):
        await conn.execute(
            """
            INSERT INTO linkedin_publish.publications
                (publication_id, subject_kind, subject_id, campaign_id, post_id, account_id, binding_id,
                 payload_digest, draft, state, scheduled_at, expires_at, post_urn)
            VALUES ($1,'entra_oid','subject-1','camp-1',$1,'acct-1','bind_mbogl7_D8GJm',$2,$3::jsonb,$4,$5,$6,$7)
            """,
            pub,
            hashlib.sha256(pub.encode()).hexdigest(),
            draft,
            state,
            NOW - timedelta(minutes=5),
            NOW + timedelta(hours=23),
            urn,
        )
        await conn.execute(
            "INSERT INTO linkedin_publish.approvals (approval_id, publication_id, payload_digest, binding_id, "
            "approved_by) VALUES ($1,$2,$3,'bind_mbogl7_D8GJm','william')",
            f"appr-{pub}",
            pub,
            hashlib.sha256(pub.encode()).hexdigest(),
        )
        await conn.execute(
            "INSERT INTO linkedin_publish.publication_events (publication_id, kind, actor) VALUES ($1,'imported','op')",
            pub,
        )
    await conn.execute(
        "INSERT INTO linkedin_publish.commissioning_grants (grant_id, publication_id, payload_digest, binding_id, "
        "capability, issued_by, expires_at, consumed_at) VALUES ('g-used','canary-1',$1,'bind_mbogl7_D8GJm','text',"
        "'william',$2,$3)",
        hashlib.sha256(b"canary-1").hexdigest(),
        NOW + timedelta(hours=1),
        NOW - timedelta(minutes=1),
    )
    await conn.execute(
        "INSERT INTO linkedin_publish.commissioning_grants (grant_id, publication_id, payload_digest, binding_id, "
        "capability, issued_by, expires_at) VALUES ('g-open','pub-2',$1,'bind_mbogl7_D8GJm','text','william',$2)",
        hashlib.sha256(b"pub-2").hexdigest(),
        NOW + timedelta(hours=1),
    )
    await conn.execute(
        "INSERT INTO linkedin_publish.request_budgets (scope, identity, endpoint, day, used) "
        "VALUES ('app','app-personal','',$1,3)",
        NOW.date(),
    )
    rows = await conn.fetch(
        "SELECT publication_id, payload_digest, draft::text AS d FROM linkedin_publish.publications"
    )
    return {r["publication_id"]: (r["payload_digest"], r["d"]) for r in rows}


async def assert_linkedin_intact(conn, before: dict[str, object]) -> None:  # noqa: ANN001
    """The old reader's view: same rows, same digests, same drafts, same states."""
    from linkedin_publish.postgres import PostgresBindingStore, PostgresPublicationStore
    from linkedin_publish.store import PublicationKey

    rows = await conn.fetch(
        "SELECT publication_id, payload_digest, draft::text AS d FROM linkedin_publish.publications"
    )
    assert {r["publication_id"]: (r["payload_digest"], r["d"]) for r in rows} == before
    binding = await PostgresBindingStore(conn).get("bind_mbogl7_D8GJm")
    assert binding is not None and binding.author_urn == PERSON and binding.publish_enabled is False
    assert binding.capability_state("text") == "enabled"
    ledger = PostgresPublicationStore(conn)
    canary = await ledger.get_publication("canary-1")
    assert canary is not None and canary.state == "published" and canary.post_urn.startswith("urn:li:share:")
    key = PublicationKey(
        subject_kind="entra_oid", subject_id="subject-1", campaign_id="camp-1", post_id="pub-2", account_id="acct-1"
    )
    assert (await ledger.find_by_key(key)) is not None
    assert len(await ledger.find_due(binding_id="bind_mbogl7_D8GJm", campaign_id=None, now=NOW, limit=10)) == 1
    open_grant = await ledger.get_grant("pub-2")
    assert open_grant is not None and open_grant.consumed_at is None
    approval = await ledger.get_approval("pub-3")
    assert approval is not None and approval.payload_digest == hashlib.sha256(b"pub-3").hexdigest()


@pytest.mark.parametrize("start", ["0004_roles_read_schema_version", "0005_token_health"], ids=["v0.4.2", "v0.7.0"])
async def test_a_populated_ledger_upgrades_and_old_readers_still_read_it(pool, monkeypatch, start: str) -> None:  # noqa: ANN001
    from linkedin_publish import postgres as pg

    async with scratch(pool, monkeypatch, upto=start) as (target, db):
        before = await populate_linkedin(db)
        applied = await pg.apply_migrations(target, applied_by="pytest")
        assert applied[-3:] == [SOCIAL, META, "0008_publication_removed"]
        await assert_linkedin_intact(db, before)
        row = await db.fetchrow("SELECT platform FROM linkedin_publish.account_bindings")
        assert row["platform"] == "linkedin"
        assert await pg.apply_migrations(target, applied_by="pytest") == []  # idempotent
        for role in ("linkedin_publish_runtime", "linkedin_publish_writer", "linkedin_publish_commissioner"):
            async with db.acquire() as conn:
                await conn.execute(f"SET ROLE {role}")
                await pg.check_compatible(conn)
                await conn.execute("RESET ROLE")


async def test_a_failed_0006_rolls_back_whole_and_a_retry_applies(pool, monkeypatch) -> None:  # noqa: ANN001
    """A row the new rules refuse stops the migration with 0005 intact; fixing it lets it apply."""
    from linkedin_publish import postgres as pg

    async with scratch(pool, monkeypatch, upto="0005_token_health") as (target, db):
        before = await populate_linkedin(db)
        # A LinkedIn binding whose author is not a LinkedIn URN (0001 never checked the format).
        await db.execute(
            "INSERT INTO linkedin_publish.account_bindings (binding_id, account_id, subject_kind, subject_id, app_id, "
            "author_urn, adapter, credential_ref, credential_version) "
            "VALUES ('bind-bad','acct-x','service','s','app','not-a-urn','share_ugc','kv://x','v1')"
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await pg.apply_migrations(target, applied_by="pytest")
        applied = {r["version"] for r in await db.fetch("SELECT version FROM linkedin_publish.schema_migrations")}
        assert SOCIAL not in applied
        cols = {
            r["column_name"]
            for r in await db.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='linkedin_publish' "
                "AND table_name='account_bindings'"
            )
        }
        assert "platform" not in cols  # nothing half-applied

        await db.execute(
            "UPDATE linkedin_publish.account_bindings SET author_urn='urn:li:person:fixed' WHERE binding_id='bind-bad'"
        )
        assert await pg.apply_migrations(target, applied_by="pytest") == [SOCIAL, META, "0008_publication_removed"]
        await assert_linkedin_intact(db, before)
        # Re-running the file body itself is harmless: every statement is idempotent.
        sql = next(p for p in pg.migration_files() if p.stem == SOCIAL).read_text()
        async with db.acquire() as conn, conn.transaction():
            await conn.execute(sql)


# ------------------------------------------------------- Meta rows admitted


META_BINDING_SQL = (
    "INSERT INTO linkedin_publish.account_bindings (binding_id, account_id, subject_kind, subject_id, app_id, "
    "author_urn, adapter, credential_ref, credential_version, platform, native_account_id) "
    "VALUES ($1,'fb-main','service','fixture-brand','app-organic',$2,$3,'kv://meta','v1',$4,$5)"
)


async def as_role(pool, role: str):  # noqa: ANN001, ANN201
    async def init(conn) -> None:  # noqa: ANN001
        await conn.execute(f"SET ROLE {role}")

    from tests.integration.conftest import dsn

    return await asyncpg.create_pool(dsn(), min_size=1, max_size=4, init=init)


async def test_a_new_writer_creates_a_meta_binding_and_the_rules_hold(pool) -> None:  # noqa: ANN001
    from social_publish.postgres import PostgresSocialBindingStore
    from tests.social_fakes import meta_binding

    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    try:
        store = PostgresSocialBindingStore(writer)
        assert await store.register(meta_binding()) is True
        assert await store.register(meta_binding()) is False  # idempotent
        got = await store.get("bind-facebook_page")
        assert got is not None and got.author_urn is None and got.native_account_id == "100000000000001"
        assert got.publish_enabled is False and got.capabilities == ()

        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await PostgresSocialBindingStore(runtime).register(meta_binding(binding_id="bind-rt"))
        bad = [
            ("b1", "urn:li:person:x", "meta_graph", "facebook_page", "100000000000001"),  # an invented URN
            ("b2", None, "meta_graph", "facebook_page", None),  # no native id
            ("b3", None, "meta_graph", "facebook_page", "page-name"),  # not numeric
            ("b4", None, "share_ugc", "facebook_page", "100000000000001"),  # a LinkedIn adapter
            ("b5", None, "share_ugc", "linkedin", None),  # LinkedIn without its author
            ("b6", PERSON, "meta_graph", "linkedin", None),  # LinkedIn on the Meta adapter
            ("b7", None, "meta_graph", "tiktok", "1"),  # an unknown platform
        ]
        for binding_id, author, adapter_name, platform, native in bad:
            with pytest.raises(asyncpg.exceptions.CheckViolationError):
                await pool.execute(META_BINDING_SQL, binding_id, author, adapter_name, platform, native)
    finally:
        await writer.close()
        await runtime.close()


async def _linkedin_binding(pool) -> None:  # noqa: ANN001
    await pool.execute(
        "INSERT INTO linkedin_publish.account_bindings (binding_id, account_id, subject_kind, subject_id, app_id, "
        "author_urn, adapter, credential_ref, credential_version) "
        "VALUES ('bind-li','acct-1','entra_oid','subject-1','app','urn:li:person:x','share_ugc','kv://x','v1')"
    )


PUB_SQL = (
    "INSERT INTO linkedin_publish.publications (publication_id, subject_kind, subject_id, campaign_id, post_id, "
    "account_id, binding_id, payload_digest, draft, state, scheduled_at, expires_at, platform, post_urn, remote_id) "
    "VALUES ($1,'service','s','c',$1,'a',$2,'d','{}'::jsonb,$3,$4,$5,$6,$7,$8)"
)


async def test_meta_rows_keep_their_own_locators_and_states(pool) -> None:  # noqa: ANN001
    from tests.social_fakes import meta_binding  # noqa: F401 - documents the binding shape used

    await _linkedin_binding(pool)
    await pool.execute(META_BINDING_SQL, "bind-fb", None, "meta_graph", "facebook_page", "100000000000001")
    window = (NOW, NOW + timedelta(hours=1))
    await pool.execute(PUB_SQL, "ok-meta", "bind-fb", "scheduled", *window, "facebook_page", None, "100000000000001_9")
    await pool.execute(PUB_SQL, "ok-li", "bind-li", "published", *window, "linkedin", "urn:li:share:1", None)
    refused = [
        (
            "m-urn",
            "bind-fb",
            "published",
            "facebook_page",
            "urn:li:share:1",
            None,
            asyncpg.exceptions.CheckViolationError,
        ),
        ("l-remote", "bind-li", "published", "linkedin", None, "123", asyncpg.exceptions.CheckViolationError),
        ("l-sched", "bind-li", "scheduled", "linkedin", None, None, asyncpg.exceptions.CheckViolationError),
        ("x-plat", "bind-li", "pending", "facebook_page", None, None, asyncpg.exceptions.ForeignKeyViolationError),
        ("x-bogus", "bind-fb", "bogus", "facebook_page", None, None, asyncpg.exceptions.CheckViolationError),
    ]
    for pub, binding_id, state, platform, urn, remote, error in refused:
        with pytest.raises(error):
            await pool.execute(PUB_SQL, pub, binding_id, state, *window, platform, urn, remote)


async def test_the_job_role_still_cannot_insert_an_approval_or_use_a_grant(pool) -> None:  # noqa: ANN001
    await _linkedin_binding(pool)
    await pool.execute(PUB_SQL, "p1", "bind-li", "pending", NOW, NOW + timedelta(hours=1), "linkedin", None, None)
    await pool.execute(
        "INSERT INTO linkedin_publish.commissioning_grants (grant_id, publication_id, payload_digest, binding_id, "
        "capability, issued_by, expires_at) VALUES ('g1','p1','d','bind-li','text','william',$1)",
        NOW + timedelta(hours=1),
    )
    runtime = await as_role(pool, "linkedin_publish_runtime")
    commissioner = await as_role(pool, "linkedin_publish_commissioner")
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await runtime.execute(
                "INSERT INTO linkedin_publish.approvals (approval_id, publication_id, payload_digest, binding_id, "
                "approved_by) VALUES ('a1','p1','d','bind-li','forged')"
            )
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):  # F26: the runtime cannot spend a grant
            await runtime.execute(
                "UPDATE linkedin_publish.commissioning_grants SET consumed_at=now() WHERE grant_id='g1'"
            )
        # The runtime keeps every delivery transition it had.
        await runtime.execute(
            "UPDATE linkedin_publish.publications SET state='claimed', attempt_token='t', remote_id=NULL "
            "WHERE publication_id='p1'"
        )
        # F26: the commissioner may spend the grant and run the send transitions ...
        await commissioner.execute(
            "UPDATE linkedin_publish.commissioning_grants SET consumed_at=now() WHERE grant_id='g1'"
        )
        await commissioner.execute(
            "UPDATE linkedin_publish.publications SET state='sending', lease_deadline=now() WHERE publication_id='p1'"
        )
        await commissioner.execute(
            "INSERT INTO linkedin_publish.request_budgets (scope, identity, endpoint, day) VALUES ('app','x','',$1)",
            NOW.date(),
        )
        # ... and nothing that mints authority or rewrites what was approved.
        for statement in (
            "INSERT INTO linkedin_publish.approvals (approval_id, publication_id, payload_digest, binding_id, "
            "approved_by) VALUES ('a2','p1','d','bind-li','forged')",
            "INSERT INTO linkedin_publish.commissioning_grants (grant_id, publication_id, payload_digest, binding_id, "
            "capability, issued_by, expires_at) VALUES ('g2','p1','d','bind-li','text','x',now() + interval '1 hour')",
            "UPDATE linkedin_publish.account_bindings SET publish_enabled=true",
            "UPDATE linkedin_publish.publications SET payload_digest='x' WHERE publication_id='p1'",
            "UPDATE linkedin_publish.approvals SET revoked_at=now()",
        ):
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await commissioner.execute(statement)
    finally:
        await runtime.close()
        await commissioner.close()


async def test_publish_due_runs_as_the_commissioner_and_a_runtime_sends_nothing(pool, credentials) -> None:  # noqa: ANN001
    """F26 end to end: the canary path works under the narrow role; a runtime identity
    handed a commissioning actor is refused by the database before `sending`."""
    import httpx

    from linkedin_publish import PostDraft, PublicationService, StaticCredentialProvider
    from linkedin_publish.postgres import PostgresPublicationStore
    from linkedin_publish.store import ApprovalRecord, CommissioningGrant, PublicationKey, PublicationRecord
    from tests.conftest import binding as li_binding
    from tests.conftest import make_client

    draft = PostDraft(author_urn="urn:li:person:AbC123xyz", commentary="Canary.")
    native = li_binding(capabilities=(), publish_enabled=False).model_copy(
        update={"token_expires_at": datetime.now(UTC) + timedelta(days=30), "token_observed_at": datetime.now(UTC)}
    )
    await pool.execute(
        "INSERT INTO linkedin_publish.account_bindings (binding_id, account_id, subject_kind, subject_id, app_id, "
        "author_urn, adapter, credential_ref, credential_version) "
        "VALUES ('bind-1','acct-william','entra_oid','subject-1','app-personal',$1,'share_ugc','kv://x','v1')",
        native.author_urn,
    )
    owner = PostgresPublicationStore(pool)
    for pub in ("canary-rt", "canary-ok"):
        record = PublicationRecord(
            publication_id=pub,
            key=PublicationKey(
                subject_kind="entra_oid",
                subject_id="subject-1",
                campaign_id="c",
                post_id=pub,
                account_id="acct-william",
            ),
            binding_id="bind-1",
            payload_digest=f"{pub:0<64}"[:64],
            draft=draft,
            scheduled_at=datetime.now(UTC) - timedelta(minutes=1),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
        await owner.upsert_publication(record)
        await owner.put_approval(
            ApprovalRecord(
                approval_id=f"a-{pub}",
                publication_id=pub,
                payload_digest=record.payload_digest,
                binding_id="bind-1",
                approved_by="william",
                approved_at=datetime.now(UTC),
            )
        )
        await owner.put_grant(
            CommissioningGrant(
                grant_id=f"g-{pub}",
                publication_id=pub,
                payload_digest=record.payload_digest,
                binding_id="bind-1",
                capability="text",
                issued_by="william",
                issued_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )

    sent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request.url.path)
        return httpx.Response(201, json={}, headers={"x-restli-id": "urn:li:share:7100000000000000001"})

    for role, pub in (("linkedin_publish_runtime", "canary-rt"), ("linkedin_publish_commissioner", "canary-ok")):
        role_pool = await as_role(pool, role)
        client, _ = make_client(handler)
        service = PublicationService(
            PostgresPublicationStore(role_pool),
            client,
            StaticCredentialProvider(credentials),
            actor="william",
            commissioning_actor="william",
        )
        try:
            if role.endswith("runtime"):
                from linkedin_publish import CapabilityUnavailable

                with pytest.raises(CapabilityUnavailable):
                    await service.publish_one(pub, native, expected_digest=f"{pub:0<64}"[:64])
            else:
                receipt = await service.publish_one(pub, native, expected_digest=f"{pub:0<64}"[:64])
                assert receipt.post_urn.startswith("urn:li:share:")
        finally:
            await role_pool.close()
    refused = await owner.get_publication("canary-rt")
    assert refused is not None and refused.state == "pending"  # back to pending, never `sending`
    assert sent == ["/v2/ugcPosts"]  # exactly one request: the commissioner's


# ------------------------------------------------- import and dispatch on Postgres


async def _meta_ready(pool, platform: str = "facebook_page", fmt: str = "feed_image"):  # noqa: ANN001, ANN202
    from tests.social_fakes import IG, PAGE

    native = PAGE if platform == "facebook_page" else IG
    op = "schedule_post" if platform == "facebook_page" else "publish_post"
    await pool.execute(META_BINDING_SQL, f"bind-{platform}", None, "meta_graph", platform, native)
    capabilities = [
        {
            "operation": op,
            "format": fmt,
            "state": "enabled",
            "observed_at": NOW.isoformat(),
            "evidence_publication_id": "house-receipt",
        }
    ]
    await pool.execute(
        "UPDATE linkedin_publish.account_bindings SET publish_enabled = true, capabilities = $2::jsonb "
        "WHERE binding_id = $1",
        f"bind-{platform}",
        json.dumps(capabilities),
    )
    from social_publish.postgres import PostgresSocialBindingStore

    return await PostgresSocialBindingStore(pool).get(f"bind-{platform}")


async def test_a_c3_approval_imported_twice_concurrently_has_one_owner_and_one_send(pool) -> None:  # noqa: ANN001
    from social_publish import DispatchService, import_c3_approval
    from social_publish.postgres import PostgresSocialStore
    from tests.contract.test_social_import import CAPTION, INSTANCE, QUIESCED, record, trust
    from tests.social_fakes import FakeGraph, adapter

    binding = await _meta_ready(pool)
    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    try:

        async def once():  # noqa: ANN202
            return await import_c3_approval(
                PostgresSocialStore(writer),
                binding,
                record=record(),
                caption=CAPTION,
                trust=trust(),
                instance=INSTANCE,
                source=QUIESCED,
                actor="fixture-operator",
                now=NOW,
            )

        outcomes = await asyncio.gather(*[once() for _ in range(5)])
        assert sum(o.created for o in outcomes) == 1
        assert len({o.record.publication_id for o in outcomes}) == 1
        count = await pool.fetchval("SELECT count(*) FROM linkedin_publish.publications WHERE intent_id IS NOT NULL")
        assert count == 1

        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):  # the job cannot import approvals
            from social_publish.ledger import SocialApproval

            await PostgresSocialStore(runtime).import_intent(
                outcomes[0].record.model_copy(update={"publication_id": "x", "intent_id": "other"}),
                SocialApproval(
                    approval_id="ax",
                    publication_id="x",
                    payload_digest="d",
                    binding_id=binding.binding_id,
                    approved_by="forged",
                    approved_at=NOW,
                ),
                actor="job",
                at=NOW,
            )

        graph = FakeGraph()
        service = DispatchService(PostgresSocialStore(runtime), {"meta_graph": adapter(graph)})
        await service.tick(binding, now=NOW, dry_run=False)
        await service.tick(binding, now=NOW + timedelta(minutes=5), dry_run=False)
        assert len(graph.writes()) == 1
        state = await pool.fetchval("SELECT state FROM linkedin_publish.publications WHERE intent_id IS NOT NULL")
        assert state == "scheduled"
    finally:
        await writer.close()
        await runtime.close()


async def test_a_lost_instagram_publish_stays_unknown_across_a_restart(pool) -> None:  # noqa: ANN001
    from social_publish import DispatchService
    from social_publish.postgres import PostgresSocialStore
    from tests.social_fakes import IMAGE, FakeGraph, adapter, approval_for, meta_payload, record_for

    binding = await _meta_ready(pool, "instagram_professional")
    writer = await as_role(pool, "linkedin_publish_writer")
    record = record_for(
        binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,)), publication_id="ig-1"
    )
    await PostgresSocialStore(writer).import_intent(record, approval_for(record), actor="op", at=NOW)
    await writer.close()
    graph = FakeGraph()
    graph.status_script = {"900000000000001": ["FINISHED"]}
    graph.faults["media_publish"] = ["lost_before"]

    runtime = await as_role(pool, "linkedin_publish_runtime")
    try:
        await DispatchService(PostgresSocialStore(runtime), {"meta_graph": adapter(graph)}).tick(
            binding, now=NOW, dry_run=False
        )
    finally:
        await runtime.close()
    row = await pool.fetchrow(
        "SELECT state, container_id FROM linkedin_publish.publications WHERE publication_id='ig-1'"
    )
    assert (row["state"], row["container_id"]) == ("unknown", "900000000000001")

    # A restart: a new pool, a new adapter, the same database.
    restarted = await as_role(pool, "linkedin_publish_runtime")
    try:
        service = DispatchService(PostgresSocialStore(restarted), {"meta_graph": adapter(graph)})
        for minutes in (1, 2):
            await service.tick(binding, now=NOW + timedelta(minutes=minutes), dry_run=False)
    finally:
        await restarted.close()
    row = await pool.fetchrow("SELECT state FROM linkedin_publish.publications WHERE publication_id='ig-1'")
    assert row["state"] == "unknown"
    assert len([c for c in graph.writes() if c.path.endswith("media_publish")]) == 1


async def test_rollback_disables_meta_dispatch_and_keeps_reading_back(pool) -> None:  # noqa: ANN001
    """Roll back by disabling Meta dispatch against the expanded schema: no write leaves,
    receipts stay, pending external schedules are still reconciled, LinkedIn is untouched."""
    from linkedin_publish.postgres import PostgresBindingStore
    from social_publish import DispatchService
    from social_publish.postgres import PostgresSocialBindingStore, PostgresSocialStore
    from tests.social_fakes import FakeGraph, adapter, approval_for, meta_payload, record_for

    await _linkedin_binding(pool)
    binding = await _meta_ready(pool, fmt="text")
    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    try:
        social = PostgresSocialStore(writer)
        due = record_for(binding, meta_payload(), publication_id="fb-due")
        await social.import_intent(due, approval_for(due), actor="op", at=NOW)
        graph = FakeGraph()
        remote = f"{graph.page}_42"
        graph.scheduled[remote] = {
            "id": remote,
            "message": "already scheduled",
            "scheduled_publish_time": 1,
            "is_published": False,
        }
        live = record_for(
            binding, meta_payload(caption="already scheduled"), publication_id="fb-live", state="scheduled"
        )
        live = live.model_copy(update={"remote_id": remote})
        await social.import_intent(live, approval_for(live), actor="op", at=NOW)

        assert await PostgresSocialBindingStore(writer).disable_platform("facebook_page") == 1
        disabled = await PostgresSocialBindingStore(runtime).get(binding.binding_id)
        assert disabled is not None and disabled.publish_enabled is False
        graph.go_live(remote)

        tick = await DispatchService(PostgresSocialStore(runtime), {"meta_graph": adapter(graph)}).tick(
            disabled, now=NOW, dry_run=False
        )

        assert graph.writes() == [] and tick.disabled
        states = {
            r["publication_id"]: r["state"]
            for r in await pool.fetch(
                "SELECT publication_id, state FROM linkedin_publish.publications WHERE platform='facebook_page'"
            )
        }
        assert states == {"fb-due": "pending", "fb-live": "published"}  # read back, never sent
        li = await PostgresBindingStore(runtime).get("bind-li")
        assert li is not None and li.adapter == "share_ugc"
        with pytest.raises(Exception, match="LinkedIn"):
            await PostgresSocialBindingStore(writer).disable_platform("linkedin")
    finally:
        await writer.close()
        await runtime.close()
