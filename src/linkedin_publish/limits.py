"""Request budgets and retry policy.

Two things this module refuses to do, both of which look like conveniences:

* **It never counts a publication as one request.** An image post is an
  `initializeUpload`, a `PUT` and a `create` — three counted requests against
  three different endpoints. Budgeting the post rather than the requests is how
  a quota is crossed while the counter still reads healthy.
* **It never resets in-process.** Reservations live in an injected store shared
  by every replica and by both the workflow and the MCP. Restarting a job grants
  no new quota, and a reservation made for an uncertain send is *kept* — we may
  well have spent it.

Product limits differ per product and per endpoint; the published figures are a
ceiling that portal observation may tighten, never a guarantee.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Final, Literal, Protocol, runtime_checkable

from .errors import QuotaDeferred
from .models import Adapter

__all__ = [
    "BudgetKey",
    "BudgetStore",
    "Endpoint",
    "InMemoryBudgetStore",
    "QuotaLimiter",
    "QuotaProfile",
    "SHARE_UGC_PROFILE",
    "REST_CMA_DEV_PROFILE",
    "profile_for",
]

Endpoint = Literal["create", "upload", "register", "read", "delete", "introspect"]

BudgetScope = Literal["app", "member", "endpoint"]


@dataclass(frozen=True, slots=True)
class BudgetKey:
    """One counter: a scope, the identity it counts, and a UTC day."""

    scope: BudgetScope
    identity: str
    day: date
    endpoint: Endpoint | None = None


@dataclass(frozen=True, slots=True)
class QuotaProfile:
    """Documented per-product daily ceilings.

    `per_endpoint` is deliberately sparse: an endpoint with no entry is bounded
    only by the app and member ceilings, which is the documented behaviour, not
    an oversight.
    """

    name: str
    app_daily: int
    member_daily: int
    per_endpoint: dict[Endpoint, int]


#: Share on LinkedIn: 150/member/day and 100,000/app/day are the published
#: figures. The pilot caps the app far below its ceiling — nothing in this
#: release should be making thousands of calls, so a runaway is a bug we want to
#: hit a wall rather than a bill.
SHARE_UGC_PROFILE: Final = QuotaProfile(
    name="share_ugc",
    app_daily=500,
    member_daily=150,
    per_endpoint={"create": 50, "upload": 150, "register": 150},
)

#: CMA Development Tier: 100/member and 500/app.
REST_CMA_DEV_PROFILE: Final = QuotaProfile(
    name="rest_posts_dev",
    app_daily=500,
    member_daily=100,
    per_endpoint={"create": 50, "upload": 100, "register": 100},
)


def profile_for(adapter: Adapter) -> QuotaProfile:
    """The conservative default profile for an adapter."""
    return SHARE_UGC_PROFILE if adapter == "share_ugc" else REST_CMA_DEV_PROFILE


@runtime_checkable
class BudgetStore(Protocol):
    """Durable, shared, atomic request counters."""

    async def reserve(self, reservations: Sequence[tuple[BudgetKey, int]]) -> None:
        """Reserve one unit against every key, all-or-nothing.

        Raises `QuotaExhausted` naming the first key that would exceed its limit.
        A partial reservation is never left behind.
        """
        ...

    async def usage(self, key: BudgetKey) -> int:
        """Current count for `key`."""
        ...


class QuotaExhausted(Exception):
    """Internal signal from a store; `QuotaLimiter` converts it to `QuotaDeferred`."""

    def __init__(self, key: BudgetKey, limit: int) -> None:
        super().__init__(f"{key.scope} budget exhausted ({limit})")
        self.key = key
        self.limit = limit


class InMemoryBudgetStore:
    """Process-local counters for fixtures and single-process tests.

    Correct for its scope and honestly *not* durable: a hosted deployment must
    inject the Postgres store, or two replicas will each grant a full quota.
    """

    def __init__(self) -> None:
        self._counts: dict[BudgetKey, int] = {}
        self._lock = asyncio.Lock()

    async def reserve(self, reservations: Sequence[tuple[BudgetKey, int]]) -> None:
        async with self._lock:
            for key, limit in reservations:
                if self._counts.get(key, 0) + 1 > limit:
                    raise QuotaExhausted(key, limit)
            for key, _ in reservations:
                self._counts[key] = self._counts.get(key, 0) + 1

    async def usage(self, key: BudgetKey) -> int:
        return self._counts.get(key, 0)


def next_utc_midnight(now: datetime) -> datetime:
    """Start of the next UTC day — when a daily budget refills."""
    tomorrow = (now.astimezone(timezone.utc) + timedelta(days=1)).date()
    return datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)


class QuotaLimiter:
    """Reserves budget before a counted request, or defers it."""

    def __init__(self, store: BudgetStore, profile: QuotaProfile) -> None:
        self._store = store
        self._profile = profile

    @property
    def profile(self) -> QuotaProfile:
        return self._profile

    async def reserve(
        self,
        *,
        app_id: str,
        member_urn: str,
        endpoint: Endpoint,
        now: datetime | None = None,
    ) -> None:
        """Reserve one request. Raises `QuotaDeferred` when a ceiling is reached.

        The deferral says *when* the budget refills — the next UTC midnight —
        rather than sleeping. A job never blocks on a daily window.
        """
        moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        day = moment.date()

        reservations: list[tuple[BudgetKey, int]] = [
            (BudgetKey("app", app_id, day), self._profile.app_daily),
            (BudgetKey("member", member_urn, day), self._profile.member_daily),
        ]
        endpoint_limit = self._profile.per_endpoint.get(endpoint)
        if endpoint_limit is not None:
            reservations.append(
                (BudgetKey("endpoint", f"{app_id}:{endpoint}", day, endpoint), endpoint_limit)
            )

        try:
            await self._store.reserve(reservations)
        except QuotaExhausted as exc:
            raise QuotaDeferred(
                f"{self._profile.name}: {exc.key.scope} daily budget of {exc.limit} is exhausted",
                not_before=next_utc_midnight(moment),
            ) from exc
