"""Durable ledger against a real Postgres.

Every test here needs genuine database semantics: concurrent compare-and-set,
transactional rollback, and unique constraints. They are the reason the
in-memory store is a convenience and not the contract.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from linkedin_publish import PostDraft, PublicationKey, PublicationRecord, StoreConflict
from linkedin_publish.postgres import (
    REQUIRED_MIGRATION,
    PostgresBudgetStore,
    PostgresPublicationStore,
    apply_migrations,
    check_compatible,
)

pytestmark = [pytest.mark.integration]

NOW = datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc)
PERSON = "urn:li:person:AbC123xyz"
DIGEST = "d" * 64


async def seed_binding(pool) -> None:  # noqa: ANN001
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


def record(publication_id: str = "pub-1", *, post_id: str = "post-1", digest: str = DIGEST) -> PublicationRecord:
    return PublicationRecord(
        publication_id=publication_id,
        key=PublicationKey(
            subject_kind="entra_oid",
            subject_id="subject-1",
            campaign_id="camp-1",
            post_id=post_id,
            account_id="acct-1",
        ),
        binding_id="bind-1",
        payload_digest=digest,
        draft=PostDraft(author_urn=PERSON, commentary="Intelligence, orchestrated."),
        scheduled_at=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(hours=23),
    )


async def test_migrations_are_idempotent_and_checksum_verified(pool) -> None:  # noqa: ANN001
    from tests.integration.conftest import dsn

    assert await apply_migrations(dsn(), applied_by="pytest") == []
    await check_compatible(pool)
    row = await pool.fetchrow("SELECT max(version) AS v FROM linkedin_publish.schema_migrations")
    assert row["v"] >= REQUIRED_MIGRATION


async def test_a_publication_round_trips(pool) -> None:  # noqa: ANN001
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    stored = await store.upsert_publication(record())
    assert stored.publication_id == "pub-1"

    fetched = await store.get_publication("pub-1")
    assert fetched is not None
    assert fetched.draft.commentary == "Intelligence, orchestrated."
    assert fetched.state == "pending"


async def test_the_natural_key_is_unique_and_a_new_digest_is_a_conflict(pool) -> None:  # noqa: ANN001
    """Same key, different payload: a conflict, never a second post."""
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    await store.upsert_publication(record())

    same = await store.upsert_publication(record("pub-2"))
    assert same.publication_id == "pub-1"

    with pytest.raises(StoreConflict, match="different payload digest"):
        await store.upsert_publication(record("pub-3", digest="9" * 64))


async def test_two_processes_cannot_both_claim_the_same_row(pool) -> None:  # noqa: ANN001
    """The real contention test. One winner, one `StoreConflict`."""
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    await store.upsert_publication(record())

    async def claim(token: str) -> bool:
        try:
            await store.compare_and_set(
                "pub-1",
                expected_state="pending",
                expected_attempt_token=None,
                updates={"state": "claimed", "attempt_token": token},
            )
        except StoreConflict:
            return False
        return True

    results = await asyncio.gather(*[claim(f"worker-{index}") for index in range(8)])
    assert sum(results) == 1


async def test_a_cas_refuses_to_touch_non_delivery_columns(pool) -> None:  # noqa: ANN001
    """The approved payload is not the runtime's to rewrite."""
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    await store.upsert_publication(record())

    with pytest.raises(StoreConflict, match="non-delivery columns"):
        await store.compare_and_set(
            "pub-1",
            expected_state="pending",
            expected_attempt_token=None,
            updates={"payload_digest": "0" * 64},
        )


async def test_an_attempt_token_scopes_the_transition(pool) -> None:  # noqa: ANN001
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    await store.upsert_publication(record())
    await store.compare_and_set(
        "pub-1",
        expected_state="pending",
        expected_attempt_token=None,
        updates={"state": "claimed", "attempt_token": "mine"},
    )
    with pytest.raises(StoreConflict):
        await store.compare_and_set(
            "pub-1",
            expected_state="claimed",
            expected_attempt_token="not-mine",
            updates={"state": "sending"},
        )


async def test_a_grant_is_consumed_exactly_once_under_contention(pool) -> None:  # noqa: ANN001
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    await store.upsert_publication(record())

    from linkedin_publish import CommissioningGrant

    await store.put_grant(
        CommissioningGrant(
            grant_id="grant-1",
            publication_id="pub-1",
            payload_digest=DIGEST,
            binding_id="bind-1",
            capability="text",
            issued_by="william",
            issued_at=NOW,
            expires_at=NOW + timedelta(minutes=30),
        )
    )
    results = await asyncio.gather(*[store.consume_grant("grant-1", at=NOW) for _ in range(6)])
    assert sum(results) == 1


async def test_the_open_grant_index_permits_only_one_unconsumed_grant(pool) -> None:  # noqa: ANN001
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    await store.upsert_publication(record())
    from linkedin_publish import CommissioningGrant

    def grant(grant_id: str) -> CommissioningGrant:
        return CommissioningGrant(
            grant_id=grant_id,
            publication_id="pub-1",
            payload_digest=DIGEST,
            binding_id="bind-1",
            capability="text",
            issued_by="william",
            issued_at=NOW,
            expires_at=NOW + timedelta(minutes=30),
        )

    await store.put_grant(grant("grant-1"))
    with pytest.raises(Exception, match="commissioning_grants_one_open"):
        await store.put_grant(grant("grant-2"))


async def test_the_window_constraint_refuses_an_inverted_schedule(pool) -> None:  # noqa: ANN001
    await seed_binding(pool)
    store = PostgresPublicationStore(pool)
    bad = record().model_copy(update={"expires_at": NOW - timedelta(days=1)})
    with pytest.raises(Exception, match="publications_window"):
        await store.upsert_publication(bad)


# ------------------------------------------------------------------ budgets ---


async def test_budget_reservations_are_atomic_across_replicas(pool) -> None:  # noqa: ANN001
    """Ten concurrent callers, a ceiling of three, exactly three reservations."""
    from linkedin_publish.limits import BudgetKey, QuotaExhausted

    store = PostgresBudgetStore(pool)
    key = BudgetKey("app", "app-personal", NOW.date())

    async def attempt() -> bool:
        try:
            await store.reserve([(key, 3)])
        except QuotaExhausted:
            return False
        return True

    results = await asyncio.gather(*[attempt() for _ in range(10)])
    assert sum(results) == 3
    assert await store.usage(key) == 3


async def test_a_refused_reservation_rolls_back_its_earlier_counters(pool) -> None:  # noqa: ANN001
    """The third request at a limit of two must not spend the app counter."""
    from linkedin_publish.limits import BudgetKey, QuotaExhausted

    store = PostgresBudgetStore(pool)
    app_key = BudgetKey("app", "app-personal", NOW.date())
    member_key = BudgetKey("member", PERSON, NOW.date())

    for _ in range(2):
        await store.reserve([(app_key, 100), (member_key, 2)])
    with pytest.raises(QuotaExhausted):
        await store.reserve([(app_key, 100), (member_key, 2)])

    assert await store.usage(app_key) == 2, "the app counter was spent by a refused reservation"
    assert await store.usage(member_key) == 2


async def test_budgets_are_keyed_by_utc_day(pool) -> None:  # noqa: ANN001
    from linkedin_publish.limits import BudgetKey

    store = PostgresBudgetStore(pool)
    today = BudgetKey("app", "app-personal", NOW.date())
    tomorrow = BudgetKey("app", "app-personal", (NOW + timedelta(days=1)).date())
    await store.reserve([(today, 1)])
    await store.reserve([(tomorrow, 1)])
    assert await store.usage(today) == 1
    assert await store.usage(tomorrow) == 1
