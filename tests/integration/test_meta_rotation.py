"""Meta register, rotate and re-enable under the real roles; rights evidence in `origin` (F41, F39, F42).

No migration ships with these: the writer's table-level UPDATE on
`account_bindings` (0002) already covers 0005's rotation columns and 0007's
`token_never_expires`, and `origin` is free-form jsonb. These tests prove that
under `SET ROLE`, never as Python checks: the writer may rotate and re-enable,
the runtime and the commissioner may not.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.integration.test_social_migration import as_role

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")

NOW = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)


def evidence(version: str, *, never: bool = True):  # noqa: ANN201
    from social_publish import parse_debug_token

    data = {
        "app_id": "app-organic",
        "is_valid": True,
        "expires_at": 0 if never else int((NOW + timedelta(days=50)).timestamp()),
    }
    return parse_debug_token({"data": data}, credential_version=version, observed_at=NOW, expected_app_id="app-organic")


async def test_register_rotate_and_reenable_run_as_the_writer_and_nobody_else(pool) -> None:  # noqa: ANN001
    from social_publish import OperationCapability, reenable_problems
    from social_publish.errors import StoreConflict
    from social_publish.postgres import PostgresSocialBindingStore
    from tests.social_fakes import meta_binding

    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    commissioner = await as_role(pool, "linkedin_publish_commissioner")
    try:
        store = PostgresSocialBindingStore(writer)
        # An "enabled" binding handed to register still lands DISABLED with nothing commissioned.
        sneaky = meta_binding(enabled=(("schedule_post", "text"),), publish_enabled=True)
        assert await store.register(sneaky) is True
        row = await store.get(sneaky.binding_id)
        assert row is not None and row.publish_enabled is False and row.capabilities == ()
        assert (await store.find_account("facebook_page", "100000000000001")).binding_id == sneaky.binding_id  # type: ignore[union-attr]
        assert await store.find_account("facebook_page", "999") is None

        # Bring it to a commissioned, measured, enabled state the way production does.
        await store.record_token_evidence(sneaky.binding_id, evidence("v1"))
        await pool.execute(
            "UPDATE linkedin_publish.account_bindings SET publish_enabled = true, capabilities = $2::jsonb "
            "WHERE binding_id = $1",
            sneaky.binding_id,
            "["
            + OperationCapability(
                operation="schedule_post",
                format="text",
                state="enabled",
                observed_at=NOW,
                evidence_publication_id="house-receipt",
            ).model_dump_json()
            + "]",
        )

        for role_pool in (runtime, commissioner):
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await PostgresSocialBindingStore(role_pool).rotate(
                    sneaky.binding_id, expected_version="v1", credential_ref="kv-new", credential_version="v2", at=NOW
                )
        with pytest.raises(StoreConflict, match="not 'v0'"):
            await store.rotate(
                sneaky.binding_id, expected_version="v0", credential_ref="kv-new", credential_version="v2", at=NOW
            )
        held = await store.rotate(
            sneaky.binding_id, expected_version="v1", credential_ref="kv-new", credential_version="v2", at=NOW
        )
        assert held.credential_version == "v2" and held.credential_ref == "kv-new"
        assert held.publish_enabled is False and held.reenable_after_measure is True and held.rotated_at == NOW
        assert held.token_observed_at is None and not held.token_never_expires
        assert held.capability_state("schedule_post", "text") == "enabled"
        # Another rotation from the version already replaced cannot land.
        with pytest.raises(StoreConflict, match="names credential version 'v2'"):
            await store.rotate(
                sneaky.binding_id, expected_version="v1", credential_ref="kv-x", credential_version="v3", at=NOW
            )

        # Held and unmeasured: the store refuses to re-enable, under its own row lock.
        with pytest.raises(StoreConflict, match="no token evidence"):
            await store.reenable(sneaky.binding_id, expected_version="v2", now=NOW)
        # The old secret's reading is refused outright by the version-matched write.
        with pytest.raises(StoreConflict, match="credential version"):
            await store.record_token_evidence(sneaky.binding_id, evidence("v1"))

        measured = await store.record_token_evidence(sneaky.binding_id, evidence("v2"))
        assert (
            reenable_problems(measured.model_copy(update={"token_observed_at": NOW + timedelta(minutes=1)}), now=NOW)
            == []
        )
        for role_pool in (runtime, commissioner):
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await PostgresSocialBindingStore(role_pool).reenable(
                    sneaky.binding_id, expected_version="v2", now=NOW + timedelta(minutes=1)
                )
        back = await store.reenable(sneaky.binding_id, expected_version="v2", now=NOW + timedelta(minutes=1))
        assert back.publish_enabled is True and back.reenable_after_measure is False
        assert back.capability_state("schedule_post", "text") == "enabled"
        with pytest.raises(StoreConflict, match="already enabled"):
            await store.reenable(sneaky.binding_id, expected_version="v2", now=NOW + timedelta(minutes=1))
    finally:
        await writer.close()
        await runtime.close()
        await commissioner.close()


async def test_rights_evidence_survives_the_postgres_round_trip_and_the_runtime_still_reads_it(pool) -> None:  # noqa: ANN001
    from social_publish import import_c3_approval, require_rights, rights_evidence_of
    from social_publish.postgres import PostgresSocialBindingStore, PostgresSocialStore
    from tests.contract.test_social_import import FB, FB_BINDING, IMPORT_AT, INSTANCE, QUIESCED, record, trust

    writer = await as_role(pool, "linkedin_publish_writer")
    runtime = await as_role(pool, "linkedin_publish_runtime")
    try:
        await PostgresSocialBindingStore(writer).register(FB_BINDING)
        outcome = await import_c3_approval(
            PostgresSocialStore(writer),
            FB_BINDING,
            record=record(),
            caption=FB["inputs"]["post"]["caption"],
            trust=trust(),
            instance=INSTANCE,
            source=QUIESCED,
            actor="fixture-operator",
            now=IMPORT_AT,
        )
        for pool_ in (writer, runtime):
            stored = await PostgresSocialStore(pool_).get(outcome.record.publication_id)
            assert stored is not None
            assert rights_evidence_of(stored) == FB["envelope"]["rights_evidence"]
            require_rights(stored, now=IMPORT_AT)
        assert outcome.record.payload_digest == ("6c9af2bfc88f7088b227a30c91114383428b3b4c270aab956fd31864d3794eb1")
    finally:
        await writer.close()
        await runtime.close()
