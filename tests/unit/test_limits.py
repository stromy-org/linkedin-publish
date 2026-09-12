"""Request budgets: atomic, shared, UTC-keyed, and never reset in-process."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from linkedin_publish import InMemoryBudgetStore, QuotaLimiter
from linkedin_publish.errors import QuotaDeferred
from linkedin_publish.limits import BudgetKey, QuotaProfile, next_utc_midnight, profile_for

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 15, 23, 30, tzinfo=timezone.utc)

TINY = QuotaProfile(name="tiny", app_daily=100, member_daily=2, per_endpoint={"create": 2})


def limiter(profile: QuotaProfile = TINY) -> QuotaLimiter:
    return QuotaLimiter(InMemoryBudgetStore(), profile)


async def test_the_third_request_at_a_limit_of_two_is_refused() -> None:
    quota = limiter()
    for _ in range(2):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    with pytest.raises(QuotaDeferred, match="exhausted"):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)


async def test_the_refusal_names_the_next_utc_midnight_rather_than_sleeping() -> None:
    quota = limiter()
    for _ in range(2):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    with pytest.raises(QuotaDeferred) as caught:
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    assert caught.value.not_before == datetime(2026, 9, 16, 0, 0, tzinfo=timezone.utc)


async def test_separate_members_hold_separate_budgets() -> None:
    quota = limiter(QuotaProfile(name="t", app_daily=100, member_daily=1, per_endpoint={}))
    await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    await quota.reserve(app_id="app", member_urn="urn:li:person:B", endpoint="create", now=NOW)
    with pytest.raises(QuotaDeferred):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)


async def test_separate_apps_hold_separate_budgets() -> None:
    quota = limiter(QuotaProfile(name="t", app_daily=1, member_daily=100, per_endpoint={}))
    await quota.reserve(app_id="app-1", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    await quota.reserve(app_id="app-2", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    with pytest.raises(QuotaDeferred):
        await quota.reserve(app_id="app-1", member_urn="urn:li:person:A", endpoint="create", now=NOW)


async def test_endpoints_are_counted_separately() -> None:
    profile = QuotaProfile(
        name="t", app_daily=100, member_daily=100, per_endpoint={"create": 1, "upload": 3}
    )
    quota = limiter(profile)
    await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    for _ in range(3):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="upload", now=NOW)
    with pytest.raises(QuotaDeferred):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)


async def test_the_budget_refills_at_utc_midnight_not_local_midnight() -> None:
    quota = limiter()
    for _ in range(2):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    tomorrow = NOW + timedelta(hours=1)
    assert tomorrow.date() != NOW.date()
    await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=tomorrow)


async def test_a_refused_reservation_leaves_no_partial_state() -> None:
    """All-or-nothing: a refusal on the member counter must not spend the app one."""
    store = InMemoryBudgetStore()
    quota = QuotaLimiter(store, QuotaProfile(name="t", app_daily=10, member_daily=1, per_endpoint={}))
    await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    with pytest.raises(QuotaDeferred):
        await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
    assert await store.usage(BudgetKey("app", "app", NOW.date())) == 1


async def test_concurrent_reservations_do_not_oversubscribe() -> None:
    """Ten racing callers against a ceiling of three: exactly three succeed."""
    quota = limiter(QuotaProfile(name="t", app_daily=3, member_daily=100, per_endpoint={}))

    async def attempt() -> bool:
        try:
            await quota.reserve(app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW)
        except QuotaDeferred:
            return False
        return True

    results = await asyncio.gather(*[attempt() for _ in range(10)])
    assert sum(results) == 3


async def test_a_new_limiter_over_the_same_store_grants_no_new_quota() -> None:
    """A restart is not a reset. The counters live in the injected store."""
    store = InMemoryBudgetStore()
    for _ in range(2):
        await QuotaLimiter(store, TINY).reserve(
            app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW
        )
    with pytest.raises(QuotaDeferred):
        await QuotaLimiter(store, TINY).reserve(
            app_id="app", member_urn="urn:li:person:A", endpoint="create", now=NOW
        )


def test_each_adapter_has_its_own_documented_profile() -> None:
    """Product limits are per product; 150/member is not one universal allowance."""
    assert profile_for("share_ugc").member_daily == 150
    assert profile_for("rest_posts").member_daily == 100
    assert profile_for("share_ugc").name != profile_for("rest_posts").name


def test_next_utc_midnight() -> None:
    assert next_utc_midnight(NOW) == datetime(2026, 9, 16, tzinfo=timezone.utc)
