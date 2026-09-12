"""Retry-After handling and the rule that an ambiguous write never retries."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from linkedin_publish._http import (
    DEFAULT_RATE_LIMIT_BACKOFF,
    map_read_failure,
    map_write_failure,
    parse_retry_after,
)
from linkedin_publish.errors import (
    AuthorForbidden,
    CredentialExpired,
    ProviderRejected,
    PublishOutcomeUnknown,
    QuotaDeferred,
    TransientReadFailure,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc)


def response(status: int, **headers: str) -> httpx.Response:
    return httpx.Response(status, json={}, headers=headers)


def test_retry_after_seconds() -> None:
    assert parse_retry_after(response(429, **{"retry-after": "90"}), now=NOW) == NOW + timedelta(seconds=90)


def test_retry_after_http_date() -> None:
    later = parse_retry_after(
        response(429, **{"retry-after": "Tue, 15 Sep 2026 07:05:00 GMT"}), now=NOW
    )
    assert later == NOW + timedelta(minutes=5)


def test_retry_after_in_the_past_is_clamped_to_now() -> None:
    """A provider clock behind ours is not a licence to spin."""
    assert parse_retry_after(response(429, **{"retry-after": "-30"}), now=NOW) == NOW
    past = parse_retry_after(response(429, **{"retry-after": "Tue, 15 Sep 2026 06:00:00 GMT"}), now=NOW)
    assert past == NOW


def test_retry_after_missing_or_invalid_is_none() -> None:
    assert parse_retry_after(response(429), now=NOW) is None
    assert parse_retry_after(response(429, **{"retry-after": "soon please"}), now=NOW) is None


def test_429_without_a_usable_header_falls_back_to_a_bounded_policy() -> None:
    failure = map_write_failure(response(429), what="create")
    assert isinstance(failure, QuotaDeferred)
    assert failure.not_before is not None
    assert failure.not_before <= datetime.now(timezone.utc) + DEFAULT_RATE_LIMIT_BACKOFF + timedelta(seconds=5)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_write_5xx_is_unknown_never_a_retry(status: int) -> None:
    """A gateway can fail *after* the post was created. Nothing proves it did not."""
    failure = map_write_failure(response(status), what="create")
    assert isinstance(failure, PublishOutcomeUnknown)
    assert failure.retryable is False


@pytest.mark.parametrize("status", [500, 503])
def test_a_read_5xx_is_a_bounded_retry(status: int) -> None:
    """Reads are safe to repeat; that asymmetry is the whole point."""
    failure = map_read_failure(response(status), what="get")
    assert isinstance(failure, TransientReadFailure)
    assert failure.retryable is True


def test_401_stops_the_account() -> None:
    assert isinstance(map_write_failure(response(401), what="create"), CredentialExpired)


def test_403_is_an_author_problem_not_an_adapter_hint() -> None:
    assert isinstance(map_write_failure(response(403), what="create"), AuthorForbidden)


@pytest.mark.parametrize("status", [400, 404, 422])
def test_definite_4xx_rejections_are_provider_rejected(status: int) -> None:
    failure = map_write_failure(response(status), what="create")
    assert isinstance(failure, ProviderRejected)
    assert failure.retryable is False


def test_no_failure_is_retryable_except_a_safe_read() -> None:
    """One line, the whole invariant: writes never auto-retry."""
    for status in (400, 401, 403, 404, 429, 500, 503):
        assert map_write_failure(response(status), what="create").retryable is False
