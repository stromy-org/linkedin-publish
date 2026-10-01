"""Database role permissions, exercised as permissions — not as Python checks.

The claim under test is that the runtime identity *cannot* create an approval or
rewrite an approved payload. A Python guard proves nothing about that: the whole
point of the column-level GRANTs in migration 0002 is that the boundary holds
even when the code above it is wrong.

So these tests connect **as the runtime role** and assert Postgres refuses.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc)
PERSON = "urn:li:person:AbC123xyz"
DIGEST = "d" * 64


async def seed(pool) -> None:  # noqa: ANN001
    await pool.execute(
        """
        INSERT INTO linkedin_publish.account_bindings
            (binding_id, account_id, subject_kind, subject_id, app_id, author_urn,
             adapter, credential_ref, credential_version)
        VALUES ('bind-1','acct-1','entra_oid','subject-1','app-personal',$1,
                'share_ugc','kv://x','v1')
        ON CONFLICT (binding_id) DO NOTHING
        """,
        PERSON,
    )
    await pool.execute(
        """
        INSERT INTO linkedin_publish.publications
            (publication_id, subject_kind, subject_id, campaign_id, post_id, account_id,
             binding_id, payload_digest, draft, scheduled_at, expires_at)
        VALUES ('pub-1','entra_oid','subject-1','camp-1','post-1','acct-1',
                'bind-1',$1,'{}'::jsonb,$2,$3)
        ON CONFLICT (publication_id) DO NOTHING
        """,
        DIGEST,
        NOW - timedelta(minutes=5),
        NOW + timedelta(hours=23),
    )


async def as_runtime(pool):  # noqa: ANN001, ANN201
    """A connection acting with only the runtime role's rights."""
    connection = await pool.acquire()
    await connection.execute("SET ROLE linkedin_publish_runtime")
    return connection


async def test_the_runtime_role_may_advance_delivery_state(pool) -> None:  # noqa: ANN001
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        await connection.execute(
            "UPDATE linkedin_publish.publications SET state='claimed', attempt_token='t' "
            "WHERE publication_id='pub-1'"
        )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_cannot_create_an_approval(pool) -> None:  # noqa: ANN001
    """An agent cannot manufacture an approval, whatever the code above it does."""
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "INSERT INTO linkedin_publish.approvals "
                "(approval_id, publication_id, payload_digest, binding_id, approved_by) "
                "VALUES ('a1','pub-1',$1,'bind-1','forged')",
                DIGEST,
            )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_cannot_rewrite_an_approved_payload(pool) -> None:  # noqa: ANN001
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.publications SET payload_digest='0' WHERE publication_id='pub-1'"
            )
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.publications SET draft='{\"x\":1}'::jsonb "
                "WHERE publication_id='pub-1'"
            )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_cannot_repoint_a_publication_at_another_account(pool) -> None:  # noqa: ANN001
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.publications SET binding_id='bind-other' "
                "WHERE publication_id='pub-1'"
            )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_cannot_author_a_binding(pool) -> None:  # noqa: ANN001
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.account_bindings SET publish_enabled=true "
                "WHERE binding_id='bind-1'"
            )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_cannot_issue_a_commissioning_grant(pool) -> None:  # noqa: ANN001
    """A scheduler can consume a grant. It can never create one."""
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "INSERT INTO linkedin_publish.commissioning_grants "
                "(grant_id, publication_id, payload_digest, binding_id, capability, "
                " issued_by, expires_at) "
                "VALUES ('g1','pub-1',$1,'bind-1','text','forged',$2)",
                DIGEST,
                NOW + timedelta(minutes=30),
            )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_cannot_revoke_an_approval(pool) -> None:  # noqa: ANN001
    """Revocation is the writer's. A runtime that could revoke could also un-revoke."""
    await seed(pool)
    await pool.execute(
        "INSERT INTO linkedin_publish.approvals "
        "(approval_id, publication_id, payload_digest, binding_id, approved_by) "
        "VALUES ('a1','pub-1',$1,'bind-1','william')",
        DIGEST,
    )
    connection = await as_runtime(pool)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE linkedin_publish.approvals SET revoked_at=now() WHERE approval_id='a1'"
            )
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


async def test_the_runtime_role_may_read_what_it_needs(pool) -> None:  # noqa: ANN001
    await seed(pool)
    connection = await as_runtime(pool)
    try:
        assert await connection.fetchval("SELECT count(*) FROM linkedin_publish.approvals") is not None
        assert await connection.fetchval("SELECT count(*) FROM linkedin_publish.account_bindings") == 1
    finally:
        await connection.execute("RESET ROLE")
        await pool.release(connection)


# ------------------------------------------------- writer: reconcile (0003)


async def _writer_pool():  # noqa: ANN202
    """A pool whose every connection acts with only the writer role's rights."""
    from tests.integration.conftest import dsn

    async def as_writer(connection) -> None:  # noqa: ANN001
        await connection.execute("SET ROLE linkedin_publish_writer")

    return await asyncpg.create_pool(dsn(), min_size=1, max_size=2, init=as_writer)


async def test_the_writer_role_can_record_the_outcome_of_an_unknown_send(pool) -> None:  # noqa: ANN001
    """`publication reconcile` runs as the operator; 0002 left it no way to write."""
    from linkedin_publish.postgres import PostgresPublicationStore
    from linkedin_publish.service import reconcile_publication

    await seed(pool)
    # The shared seed stores an empty draft; reading a record back needs a real one.
    await pool.execute(
        "UPDATE linkedin_publish.publications SET state='unknown', draft=$1::jsonb WHERE publication_id='pub-1'",
        json.dumps({"author_urn": PERSON, "commentary": "Intelligence, orchestrated."}),
    )
    writer = await _writer_pool()
    try:
        updated = await reconcile_publication(
            PostgresPublicationStore(writer),
            "pub-1",
            actor="william",
            post_urn="urn:li:share:7100000000000000000",
        )
    finally:
        await writer.close()
    assert updated.state == "published"
    assert updated.post_urn == "urn:li:share:7100000000000000000"


async def test_the_writer_role_still_cannot_rewrite_an_approved_payload_or_rearm_a_send(pool) -> None:  # noqa: ANN001
    await seed(pool)
    writer = await _writer_pool()
    try:
        for statement in (
            "UPDATE linkedin_publish.publications SET payload_digest='0' WHERE publication_id='pub-1'",
            "UPDATE linkedin_publish.publications SET draft='{\"x\":1}'::jsonb WHERE publication_id='pub-1'",
            "UPDATE linkedin_publish.publications SET binding_id='bind-other' WHERE publication_id='pub-1'",
            "UPDATE linkedin_publish.publications SET attempt_token='t' WHERE publication_id='pub-1'",
        ):
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await writer.execute(statement)
    finally:
        await writer.close()
