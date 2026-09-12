"""Shared response interpretation for both transport adapters.

The single most consequential rule in this file: **a write whose outcome cannot
be proven is `PublishOutcomeUnknown`, never a retry.** A timeout, a disconnect
and a 5xx all mean LinkedIn may already hold the post. Only a response that
proves non-acceptance (a 4xx rejection) is safe to treat as "did not publish".

The second rule: nothing from an upstream body reaches a caller. The provider
echoes submitted commentary in some error shapes, so only `serviceErrorCode` and
the request id survive, and only when they look like machine tokens.
"""

from __future__ import annotations

import email.utils
from datetime import datetime, timedelta, timezone
from typing import Final, cast

import httpx

from ._json import as_object
from .errors import (
    AuthorForbidden,
    CredentialExpired,
    LinkedInPublishError,
    ProviderRejected,
    PublishOutcomeUnknown,
    QuotaDeferred,
    TransientReadFailure,
)

__all__ = [
    "REQUEST_ID_HEADERS",
    "map_read_failure",
    "map_write_failure",
    "parse_retry_after",
    "provider_code",
    "request_id",
    "response_object",
    "restli_id",
    "write_transport_failure",
]

REQUEST_ID_HEADERS: Final = ("x-li-uuid", "x-li-fabric", "x-li-pop", "x-request-id")

#: Fallback deferral when a 429 arrives with no usable Retry-After.
DEFAULT_RATE_LIMIT_BACKOFF: Final = timedelta(minutes=15)


def response_object(response: httpx.Response) -> dict[str, object]:
    """The response body as a string-keyed mapping, or empty if it is not one."""
    try:
        return as_object(cast(object, response.json()))
    except ValueError:
        return {}


def request_id(response: httpx.Response) -> str | None:
    """First provider request id present, for support escalation."""
    for header in REQUEST_ID_HEADERS:
        value = response.headers.get(header)
        if value:
            return value
    return None


def provider_code(response: httpx.Response) -> str | None:
    """LinkedIn's own short error code, when the body carries one.

    Only `serviceErrorCode`/`code`/`status` are read. `message` is deliberately
    ignored: it is prose, and on a content rejection it quotes the post back.
    """
    payload = response_object(response)
    for key in ("serviceErrorCode", "code", "status"):
        value = payload.get(key)
        if isinstance(value, (str, int)):
            return str(value)
    return None


def restli_id(response: httpx.Response) -> str | None:
    """The created entity's URN from `x-restli-id`, if present and plausible."""
    value = response.headers.get("x-restli-id") or response.headers.get("X-RestLi-Id")
    if value and value.startswith("urn:li:"):
        return value
    return None


def parse_retry_after(response: httpx.Response, *, now: datetime | None = None) -> datetime | None:
    """Interpret `Retry-After` as either delta-seconds or an HTTP-date.

    Returns an aware UTC instant, or None when the header is absent or
    unparseable. A value in the past is normalised to `now` — a provider clock
    slightly behind ours is not a licence to retry immediately in a loop.
    """
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    reference = now or datetime.now(timezone.utc)
    raw = raw.strip()

    try:
        seconds = int(raw)
    except ValueError:
        pass
    else:
        if seconds < 0:
            return reference
        return reference + timedelta(seconds=seconds)

    try:
        parsed = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(parsed.astimezone(timezone.utc), reference)


def _deferral(response: httpx.Response) -> datetime:
    return parse_retry_after(response) or (datetime.now(timezone.utc) + DEFAULT_RATE_LIMIT_BACKOFF)


def map_write_failure(response: httpx.Response, *, what: str) -> LinkedInPublishError:
    """Classify a failed write response.

    A 5xx is `PublishOutcomeUnknown`: the gateway may have failed *after* the
    post was created. Nothing about a 500 proves non-acceptance.
    """
    status = response.status_code
    code = provider_code(response)
    rid = request_id(response)

    if status == 401:
        return CredentialExpired(
            f"{what}: credential rejected; stop this account and repair it",
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    if status == 403:
        return AuthorForbidden(
            f"{what}: this credential may not act for that author",
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    if status == 429:
        return QuotaDeferred(
            f"{what}: rate limited by the provider",
            not_before=_deferral(response),
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    if 400 <= status < 500:
        return ProviderRejected(
            f"{what}: rejected by the provider",
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    return PublishOutcomeUnknown(
        f"{what}: provider returned {status}; the post may or may not exist",
        http_status=status,
        provider_code=code,
        request_id=rid,
    )


def map_read_failure(response: httpx.Response, *, what: str) -> LinkedInPublishError:
    """Classify a failed read response. Reads are safe to retry; writes are not."""
    status = response.status_code
    code = provider_code(response)
    rid = request_id(response)

    if status == 401:
        return CredentialExpired(
            f"{what}: credential rejected", http_status=status, provider_code=code, request_id=rid
        )
    if status == 403:
        return AuthorForbidden(
            f"{what}: this credential lacks the required read permission",
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    if status == 429:
        return QuotaDeferred(
            f"{what}: rate limited by the provider",
            not_before=_deferral(response),
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    if 400 <= status < 500:
        return ProviderRejected(
            f"{what}: rejected by the provider",
            http_status=status,
            provider_code=code,
            request_id=rid,
        )
    return TransientReadFailure(
        f"{what}: provider returned {status}",
        http_status=status,
        provider_code=code,
        request_id=rid,
    )


def write_transport_failure(exc: httpx.HTTPError, *, what: str) -> LinkedInPublishError:
    """Classify a transport-level exception raised during a write.

    Every case is unknown. A connect-timeout arguably never reached LinkedIn, but
    distinguishing "connect" from "read" timeouts across proxies is not something
    this library is willing to bet a duplicate post on.
    """
    return PublishOutcomeUnknown(f"{what}: transport failure ({type(exc).__name__}); outcome unknown")
