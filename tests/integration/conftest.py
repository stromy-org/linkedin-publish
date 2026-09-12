"""Postgres integration fixtures.

These tests need a **real** Postgres. An in-memory stand-in cannot exercise what
they are for: two processes racing a compare-and-set, a transaction rolling back
a partial quota reservation, and column-level GRANTs actually refusing a write.

They skip when `LINKEDIN_PUBLISH_TEST_DSN` is unset. A skip here is a NOT-RUN,
not a pass — the plan's acceptance criterion requires these to have executed
before the durable guarantees may be claimed.

    docker run --rm -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:16
    export LINKEDIN_PUBLISH_TEST_DSN=postgres://postgres:pg@localhost:5432/postgres
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

DSN_ENV = "LINKEDIN_PUBLISH_TEST_DSN"

asyncpg = pytest.importorskip("asyncpg", reason="the `postgres` extra is not installed")


def dsn() -> str:
    value = os.environ.get(DSN_ENV)
    if not value:
        pytest.skip(f"{DSN_ENV} is not set — NOT-RUN, not a pass")
    return value


@pytest.fixture
async def pool() -> AsyncIterator[object]:
    """A migrated, empty schema, torn down after each test."""
    from linkedin_publish.postgres import apply_migrations

    target = dsn()
    await apply_migrations(target, applied_by="pytest")
    created = await asyncpg.create_pool(target, min_size=1, max_size=8)
    assert created is not None
    try:
        await created.execute(
            "TRUNCATE linkedin_publish.publication_events, linkedin_publish.approvals, "
            "linkedin_publish.commissioning_grants, linkedin_publish.publications, "
            "linkedin_publish.media_uploads, linkedin_publish.request_budgets, "
            "linkedin_publish.account_bindings RESTART IDENTITY CASCADE"
        )
        yield created
    finally:
        await created.close()
