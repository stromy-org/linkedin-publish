"""Migration 0005 against a real Postgres: rotation, the observation cache, the grants.

What only a database can show: the rotation's compare-and-set on the replaced
version, the cache's monotonic upsert, and the GRANTs — the runtime role may
refresh observations but still cannot rotate, enable or otherwise rewrite a
binding, and the writer may invalidate the cache.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from linkedin_publish.auth import TokenObservation
from linkedin_publish.postgres import (
    TOKEN_HEALTH_MIGRATION,
    PostgresBindingStore,
    PostgresObservationCache,
    has_migration,
    migration_files,
)
from linkedin_publish.store import StoreConflict

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 11, 26, 9, 0, tzinfo=timezone.utc)
PERSON = "urn:li:person:AbC123xyz"


async def seed(pool, *, enabled: bool = True) -> None:  # noqa: ANN001
    await pool.execute(
        """
        INSERT INTO linkedin_publish.account_bindings
            (binding_id, account_id, subject_kind, subject_id, app_id, author_urn,
             adapter, declared_scopes, observed_scopes, credential_ref, credential_version,
             token_expires_at, token_observed_at, publish_enabled)
        VALUES ('bind-1','acct-1','entra_oid','subject-1','app-personal',$1,
                'share_ugc','{openid,profile,w_member_social}','{openid,profile,w_member_social}',
                'kv://x','v1',$2,$3,$4)
        """,
        PERSON,
        NOW + timedelta(days=5),
        NOW - timedelta(days=50),
        enabled,
    )


def observation(version: str = "v1", *, at: datetime = NOW, active: bool | None = True) -> TokenObservation:
    return TokenObservation(
        observed_at=at,
        credential_version=version,
        token_active=active,
        scopes_ok=True,
        identity_verified=True,
        observed_scopes=("openid", "profile", "w_member_social"),
        expires_at=at + timedelta(days=59),
        app_id="app-personal",
        member_sub="AbC123xyz",
    )


async def test_the_token_health_migration_is_shipped_and_applied(pool) -> None:  # noqa: ANN001
    assert TOKEN_HEALTH_MIGRATION in [path.stem for path in migration_files()]
    assert await has_migration(pool, TOKEN_HEALTH_MIGRATION)
    columns = {
        row["column_name"]
        for row in await pool.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='linkedin_publish' AND table_name='account_bindings'"
        )
    }
    assert {"rotated_at", "rotated_from_version", "reenable_after_measure"} <= columns


async def test_rotate_is_a_compare_and_set_and_disables(pool) -> None:  # noqa: ANN001
    await seed(pool)
    store = PostgresBindingStore(pool)
    moved = await store.rotate(
        "bind-1", expected_version="v1", credential_ref="kv://y", credential_version="v2", at=NOW
    )
    assert moved.credential_version == "v2"
    assert moved.publish_enabled is False
    assert moved.token_expires_at is None
    assert moved.rotated_from_version == "v1"
    assert moved.reenable_after_measure is True

    with pytest.raises(StoreConflict):  # a second rotation from the now-stale v1 loses
        await store.rotate("bind-1", expected_version="v1", credential_ref="kv://z", credential_version="v3", at=NOW)

    with pytest.raises(StoreConflict):  # the stale probe of v1 cannot land on v2
        await store.record_observation("bind-1", observation("v1"))
    await store.record_observation("bind-1", observation("v2", at=NOW + timedelta(minutes=5)))

    enabled = await store.reenable("bind-1", expected_version="v2")
    assert enabled.publish_enabled is True
    assert enabled.reenable_after_measure is False
    with pytest.raises(StoreConflict):  # the hold is consumed; a second reenable has nothing to restore
        await store.reenable("bind-1", expected_version="v2")


async def test_reenable_does_nothing_for_a_never_enabled_binding(pool) -> None:  # noqa: ANN001
    await seed(pool, enabled=False)
    store = PostgresBindingStore(pool)
    await store.rotate("bind-1", expected_version="v1", credential_ref="kv://y", credential_version="v2", at=NOW)
    with pytest.raises(StoreConflict):
        await store.reenable("bind-1", expected_version="v2")


async def test_the_cache_is_monotonic_and_keyed_by_version(pool) -> None:  # noqa: ANN001
    await seed(pool)
    cache = PostgresObservationCache(pool)
    await cache.put("bind-1", observation("v1"))
    await cache.put("bind-1", observation("v1", at=NOW - timedelta(hours=1), active=False))
    kept = await cache.get("bind-1", "v1")
    assert kept is not None and kept.token_active is True
    assert await cache.get("bind-1", "v2") is None
    assert await cache.invalidate("bind-1") == 1
    assert await cache.get("bind-1", "v1") is None


async def _as(pool, role: str):  # noqa: ANN001, ANN202
    connection = await pool.acquire()
    await connection.execute(f"SET ROLE {role}")
    return connection


async def test_the_runtime_role_refreshes_observations_but_cannot_touch_bindings(pool) -> None:  # noqa: ANN001
    await seed(pool)
    connection = await _as(pool, "linkedin_publish_runtime")
    try:
        await connection.execute(
            "INSERT INTO linkedin_publish.token_observations (binding_id, credential_version, observed_at) "
            "VALUES ('bind-1','v1',$1)",
            NOW,
        )
        await connection.execute(
            "UPDATE linkedin_publish.token_observations SET token_active = true WHERE binding_id='bind-1'"
        )
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM linkedin_publish.token_observations WHERE binding_id='bind-1'")
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.account_bindings SET credential_version='v9' WHERE binding_id='bind-1'"
            )
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.account_bindings SET publish_enabled=true WHERE binding_id='bind-1'"
            )
    finally:
        try:
            await connection.execute("RESET ROLE")
        finally:
            await pool.release(connection)


async def test_the_writer_role_may_rotate_and_invalidate(pool) -> None:  # noqa: ANN001
    await seed(pool)
    await PostgresObservationCache(pool).put("bind-1", observation("v1"))
    connection = await _as(pool, "linkedin_publish_writer")
    try:
        await connection.execute(
            "UPDATE linkedin_publish.account_bindings SET credential_version='v2', publish_enabled=false, "
            "reenable_after_measure=true WHERE binding_id='bind-1'"
        )
        await connection.execute("DELETE FROM linkedin_publish.token_observations WHERE binding_id='bind-1'")
    finally:
        try:
            await connection.execute("RESET ROLE")
        finally:
            await pool.release(connection)


async def test_an_unreadable_cache_is_reported_unavailable(pool) -> None:  # noqa: ANN001
    """A cache the role cannot read is unavailable (reported unknown), never 'no token'.

    A ledger with no 0005 at all raises UndefinedTableError through the same path.
    """
    from linkedin_publish.health import ObservationCacheUnavailable

    await seed(pool)
    connection = await pool.acquire()
    try:
        await connection.execute("REVOKE SELECT ON linkedin_publish.token_observations FROM linkedin_publish_runtime")
        await connection.execute("SET ROLE linkedin_publish_runtime")
        cache = PostgresObservationCache(connection)  # type: ignore[arg-type] - a connection quacks like a pool here
        with pytest.raises(ObservationCacheUnavailable):
            await cache.get("bind-1", "v1")
    finally:
        try:
            await connection.execute("RESET ROLE")
            await connection.execute(
                "GRANT SELECT ON linkedin_publish.token_observations TO linkedin_publish_runtime"
            )
        finally:
            await pool.release(connection)


async def test_0005_upgrades_a_ledger_that_already_holds_a_binding(pool, monkeypatch) -> None:  # noqa: ANN001
    """The live path: a ledger at 0004 with a registered binding takes 0005 in place.

    Applied to a scratch database so the upgrade starts from a real 0004 state,
    the way the live ledger does, rather than from a fresh install.
    """
    import secrets as _secrets
    from urllib.parse import urlsplit, urlunsplit

    from linkedin_publish import postgres as pg
    from tests.integration.conftest import dsn as base_dsn

    name = f"lp_upgrade_{_secrets.token_hex(4)}"
    await pool.execute(f"CREATE DATABASE {name}")
    parts = urlsplit(base_dsn())
    target = urlunsplit(parts._replace(path=f"/{name}"))
    try:
        everything = pg.migration_files()
        monkeypatch.setattr(pg, "migration_files", lambda: [p for p in everything if p.stem < TOKEN_HEALTH_MIGRATION])
        assert TOKEN_HEALTH_MIGRATION not in await pg.apply_migrations(target, applied_by="pytest")
        upgraded = await asyncpg.create_pool(target, min_size=1, max_size=2)
        try:
            await seed(upgraded)
            before = await PostgresBindingStore(upgraded).get("bind-1")
            assert before is not None and before.reenable_after_measure is False  # pre-0005 row loads

            # Up to and including 0005 only: later migrations have their own upgrade tests.
            monkeypatch.setattr(
                pg, "migration_files", lambda: [p for p in everything if p.stem <= TOKEN_HEALTH_MIGRATION]
            )
            assert await pg.apply_migrations(target, applied_by="pytest") == [TOKEN_HEALTH_MIGRATION]
            after = await PostgresBindingStore(upgraded).get("bind-1")
            assert after is not None
            assert (after.credential_version, after.publish_enabled, after.token_expires_at) == (
                before.credential_version, before.publish_enabled, before.token_expires_at,
            )  # fmt: skip
            assert after.rotated_at is None and after.reenable_after_measure is False
            assert await pg.apply_migrations(target, applied_by="pytest") == []  # idempotent
        finally:
            await upgraded.close()
    finally:
        await pool.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
