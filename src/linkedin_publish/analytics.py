"""Organization share statistics (CMA-gated).

Follows the documented Organization Share Statistics surface: organic statistics
only, millisecond UTC intervals with **start inclusive and end exclusive**, day
or month granularity, inside the provider's rolling 12-month window.

Three rules that are easy to get wrong and expensive to get wrong:

* **A single request is bounded to 90 days here.** The provider's window is
  longer, but an unbounded history scan is how a reporting call turns into a
  quota incident. Callers page deliberately.
* **A missing metric is `None` with a reason, never `0`.** "LinkedIn did not
  report impressions" and "there were no impressions" are different facts.
* **Negative counts are preserved.** LinkedIn does emit them when a retraction
  lands inside a bucket; clamping to zero silently inflates the report.

This capability needs `rw_organization_admin` *and* an ADMINISTRATOR role on the
organization. Post-write scope proves nothing about it, which is why it is gated
on its own commissioning evidence rather than on a successful publish.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Final, Literal, cast

import httpx

from ._http import map_read_failure, response_object
from ._json import as_object
from .errors import ValidationFailure
from .models import ShareStatistics, ShareStatisticsPoint
from .urns import validate_urn
from .version import API_BASE, LINKEDIN_VERSION, REST_SHARE_STATISTICS_PATH, rest_headers

__all__ = ["MAX_QUERY_DAYS", "Granularity", "build_query", "fetch_share_statistics"]

Granularity = Literal["DAY", "MONTH"]

#: v0.1 bound on one request. Not a provider limit — a deliberate ceiling.
MAX_QUERY_DAYS: Final = 90

#: The provider's documented rolling window. Requests outside it return nothing
#: useful, so refusing early is cheaper and clearer than an empty result.
ROLLING_WINDOW_DAYS: Final = 365


def _ms(moment: datetime) -> int:
    return int(moment.astimezone(timezone.utc).timestamp() * 1000)


def build_query(
    organization_urn: str,
    *,
    start: datetime,
    end: datetime,
    granularity: Granularity = "DAY",
    now: datetime | None = None,
) -> dict[str, str]:
    """Validate the window and render the request parameters.

    Separated from the request so the encoding is testable without a transport —
    Rest.li parameter encoding is fiddly enough to deserve its own fixtures.
    """
    validate_urn(organization_urn, {"organization"}, field="organization_urn")

    if start.tzinfo is None or end.tzinfo is None:
        raise ValidationFailure("share-statistics window must use timezone-aware timestamps")
    if end <= start:
        raise ValidationFailure("share-statistics window end must be after start")

    span = end - start
    if span > timedelta(days=MAX_QUERY_DAYS):
        raise ValidationFailure(
            f"share-statistics window is {span.days} days; this build caps one request at "
            f"{MAX_QUERY_DAYS}. Page explicitly rather than scanning history."
        )

    reference = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if start < reference - timedelta(days=ROLLING_WINDOW_DAYS):
        raise ValidationFailure(
            f"share-statistics start is outside the provider's rolling "
            f"{ROLLING_WINDOW_DAYS}-day window"
        )

    return {
        "q": "organizationalEntity",
        "organizationalEntity": organization_urn,
        "timeIntervals": (
            f"(timeRange:(start:{_ms(start)},end:{_ms(end)}),timeGranularityType:{granularity})"
        ),
    }


def _metric(bucket: dict[str, object], key: str) -> int | None:
    """Read one count, preserving zero and negative values, else None."""
    value = bucket.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _engagement(bucket: dict[str, object]) -> float | None:
    value = bucket.get("engagement")
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _parse_point(element: dict[str, object]) -> ShareStatisticsPoint | None:
    time_range = as_object(as_object(element.get("timeRange")))
    raw_start = time_range.get("start")
    raw_end = time_range.get("end")
    if not isinstance(raw_start, (int, float)) or not isinstance(raw_end, (int, float)):
        return None

    stats = as_object(element.get("totalShareStatistics")) or element
    missing = not any(
        key in stats
        for key in ("impressionCount", "uniqueImpressionsCount", "clickCount", "likeCount")
    )
    return ShareStatisticsPoint(
        start=datetime.fromtimestamp(float(raw_start) / 1000, tz=timezone.utc),
        end=datetime.fromtimestamp(float(raw_end) / 1000, tz=timezone.utc),
        impressions=_metric(stats, "impressionCount"),
        unique_impressions=_metric(stats, "uniqueImpressionsCount"),
        clicks=_metric(stats, "clickCount"),
        likes=_metric(stats, "likeCount"),
        comments=_metric(stats, "commentCount"),
        shares=_metric(stats, "shareCount"),
        engagement=_engagement(stats),
        reason="provider reported no metrics for this bucket" if missing else None,
    )


async def fetch_share_statistics(
    http: httpx.AsyncClient,
    access_token: str,
    organization_urn: str,
    *,
    start: datetime,
    end: datetime,
    granularity: Granularity = "DAY",
    api_base: str = API_BASE,
    linkedin_version: str = LINKEDIN_VERSION,
    now: datetime | None = None,
) -> ShareStatistics:
    """Read organic share statistics for one organization over a bounded window."""
    params = build_query(
        organization_urn, start=start, end=end, granularity=granularity, now=now
    )
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    response = await http.get(
        f"{api_base}{REST_SHARE_STATISTICS_PATH}",
        params=params,
        headers=rest_headers(access_token, version=linkedin_version),
    )
    if response.status_code >= 400:
        raise map_read_failure(response, what="share statistics")

    payload = response_object(response)
    raw_elements = payload.get("elements")
    if not isinstance(raw_elements, list):
        return ShareStatistics(
            organization_urn=organization_urn,
            granularity=granularity,
            start=start.astimezone(timezone.utc),
            end=end.astimezone(timezone.utc),
            observed_at=observed_at,
            reason="provider returned no elements array",
        )

    points: list[ShareStatisticsPoint] = []
    for raw in cast("list[object]", raw_elements):
        parsed = _parse_point(as_object(raw))
        if parsed is not None:
            points.append(parsed)

    return ShareStatistics(
        organization_urn=organization_urn,
        granularity=granularity,
        start=start.astimezone(timezone.utc),
        end=end.astimezone(timezone.utc),
        observed_at=observed_at,
        points=tuple(points),
        reason=None if points else "provider returned an empty result for this window",
    )
