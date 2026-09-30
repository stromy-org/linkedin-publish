"""`PostgresBindingStore` against a real Postgres, acting as the roles that use it.

Registration and recording observations are operator acts, so the store is
driven through a pool whose every connection runs `SET ROLE
linkedin_publish_writer` — proving the writer's grants are sufficient — and the
runtime role is shown to be unable to register at all.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import pytest

from linkedin_publish.auth import TokenObservation
from linkedin_publish.models import AccountBinding
from linkedin_publish.postgres import PostgresBindingStore
from linkedin_publish.store import StoreConflict

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 9, 30, 9, 0, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(days=59)
DECLARED = ("openid", "profile", "w_member_social")


def binding(**overrides: object) -> AccountBinding:
    fields: dict[str, object] = {
        "binding_id": "bind-reg",
        "account_id": "william-personal",
        "subject_kind": "entra_oid",
        "subject_id": "oid-1",
        "app_id": "app-personal",
        "author_urn": "urn:li:person:AbC123",
        "allowed_organization_urns": ("urn:li:organization:42",),
        "adapter": "share_ugc",
        "declared_scopes": DECLARED,
        "credential_ref": "linkedin-member-token",
        "credential_version": "v1",
    }
    fields.update(overrides)
    return AccountBinding.model_validate(fields)


@pytest.fixture
async def writer(pool) -> AsyncIterator[PostgresBindingStore]:  # noqa: ANN001 - `pool` migrates + truncates
    async def as_writer(connection) -> None:  # noqa: ANN001
        await connection.execute("SET ROLE linkedin_publish_writer")

    created = await asyncpg.create_pool(
        os.environ["LINKEDIN_PUBLISH_TEST_DSN"], min_size=1, max_size=2, init=as_writer
    )
    try:
        yield PostgresBindingStore(created)
    finally:
        await created.close()


async def test_the_writer_registers_a_disabled_binding_that_round_trips(writer: PostgresBindingStore) -> None:
    assert await writer.register(binding(publish_enabled=True)) is True
    stored = await writer.get("bind-reg")
    assert stored is not None
    assert stored.publish_enabled is False
    assert stored.capabilities == ()
    assert stored.declared_scopes == DECLARED
    assert stored.allowed_organization_urns == ("urn:li:organization:42",)


async def test_identical_reregistration_is_a_noop_and_a_change_is_a_conflict(writer: PostgresBindingStore) -> None:
    await writer.register(binding())
    assert await writer.register(binding()) is False
    with pytest.raises(StoreConflict, match="app_id"):
        await writer.register(binding(app_id="app-other"))


async def test_the_writer_records_an_observation_for_the_current_version_only(writer: PostgresBindingStore) -> None:
    await writer.register(binding())
    observation = TokenObservation(
        observed_at=NOW, credential_version="v1", token_active=True, observed_scopes=DECLARED, expires_at=EXPIRY
    )
    updated = await writer.record_observation("bind-reg", observation)
    assert updated.token_expires_at == EXPIRY
    assert updated.token_observed_at == NOW
    assert updated.observed_scopes == DECLARED

    stale = observation.model_copy(update={"credential_version": "v0"})
    with pytest.raises(StoreConflict, match="stale"):
        await writer.record_observation("bind-reg", stale)
    with pytest.raises(StoreConflict, match="not registered"):
        await writer.record_observation("bind-missing", observation)


async def test_the_runtime_role_cannot_register_a_binding(pool) -> None:  # noqa: ANN001
    async def as_runtime(connection) -> None:  # noqa: ANN001
        await connection.execute("SET ROLE linkedin_publish_runtime")

    runtime = await asyncpg.create_pool(
        os.environ["LINKEDIN_PUBLISH_TEST_DSN"], min_size=1, max_size=1, init=as_runtime
    )
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await PostgresBindingStore(runtime).register(binding())
    finally:
        await runtime.close()
