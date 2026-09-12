"""Typed failure taxonomy.

Every way this library can decline or fail is one of `FailureCode`. Callers
branch on the code, never on a message or an HTTP status they re-derive.

Nothing here ever carries a raw upstream body. A provider response can echo the
submitted commentary back, and an authorization header can appear in a redirect
chain, so only three sanitized facts survive: the HTTP status, the provider's own
short error code, and the request id it returned for support. `sanitize_detail`
is the single chokepoint that enforces that.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Final, Literal

__all__ = [
    "AuthorForbidden",
    "CapabilityUnavailable",
    "CredentialExpired",
    "CredentialMissing",
    "FailureCode",
    "LinkedInPublishError",
    "ProviderRejected",
    "PublishOutcomeUnknown",
    "QuotaDeferred",
    "TransientReadFailure",
    "ValidationFailure",
    "sanitize_detail",
]

FailureCode = Literal[
    "validation",
    "credential_missing",
    "credential_expired",
    "author_forbidden",
    "capability_unavailable",
    "quota_deferred",
    "transient_read_failure",
    "publish_outcome_unknown",
    "provider_rejected",
]

# Provider error codes are short machine tokens; anything longer is prose that
# may quote the submitted post, so it is dropped rather than truncated.
_MAX_PROVIDER_CODE = 64
_MAX_REQUEST_ID = 128
_SAFE_TOKEN: Final = re.compile(r"^[A-Za-z0-9_.:\-]+$")


def sanitize_detail(value: object, *, limit: int) -> str | None:
    """Return `value` as a short safe token, or None when it is not one.

    Deliberately conservative: a value that is not already a bounded token of
    identifier characters is discarded entirely. Truncating instead would keep a
    prefix of whatever the provider echoed back, which is the thing this exists
    to prevent.
    """
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    text = text.strip()
    if not text or len(text) > limit:
        return None
    if not _SAFE_TOKEN.match(text):
        return None
    return text


class LinkedInPublishError(Exception):
    """Base class for every typed failure this library raises.

    Attributes:
        code: the stable `FailureCode` a caller branches on.
        http_status: upstream status when the failure came from a request.
        provider_code: LinkedIn's own short error code, when it returned one.
        request_id: the provider request id, for support escalation.
        retryable: whether re-issuing the identical request is known to be safe.
            False for every write whose outcome is unknown.
    """

    code: FailureCode = "validation"

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.http_status = http_status
        self.provider_code = sanitize_detail(provider_code, limit=_MAX_PROVIDER_CODE)
        self.request_id = sanitize_detail(request_id, limit=_MAX_REQUEST_ID)
        self.retryable = retryable

    def as_dict(self) -> dict[str, object]:
        """A log/export-safe rendering. Contains no submitted content."""
        return {
            "code": self.code,
            "message": self.message,
            "http_status": self.http_status,
            "provider_code": self.provider_code,
            "request_id": self.request_id,
            "retryable": self.retryable,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code!r}, http_status={self.http_status!r})"


class ValidationFailure(LinkedInPublishError):
    """The request is malformed or unsupported. No network call was made."""

    code: FailureCode = "validation"


class CredentialMissing(LinkedInPublishError):
    """No credential is configured for the selected binding."""

    code: FailureCode = "credential_missing"


class CredentialExpired(LinkedInPublishError):
    """The credential is known expired, revoked, or bound to another app."""

    code: FailureCode = "credential_expired"


class AuthorForbidden(LinkedInPublishError):
    """The credential may not post as the requested author."""

    code: FailureCode = "author_forbidden"


class CapabilityUnavailable(LinkedInPublishError):
    """The binding has not been commissioned for this capability.

    Raised *before* any network call. A capability is enabled by recorded
    commissioning evidence, never by a successful call of a different shape.
    """

    code: FailureCode = "capability_unavailable"

    def __init__(
        self,
        message: str,
        *,
        capability: str,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(
            message,
            http_status=http_status,
            provider_code=provider_code,
            request_id=request_id,
            retryable=False,
        )
        self.capability = capability

    def as_dict(self) -> dict[str, object]:
        detail = super().as_dict()
        detail["capability"] = self.capability
        return detail


class QuotaDeferred(LinkedInPublishError):
    """A quota is exhausted or the provider asked us to wait.

    `not_before` is an aware UTC instant. The caller defers the record; it does
    not sleep a job across the delay.
    """

    code: FailureCode = "quota_deferred"

    def __init__(
        self,
        message: str,
        *,
        not_before: datetime | None = None,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(
            message,
            http_status=http_status,
            provider_code=provider_code,
            request_id=request_id,
            retryable=False,
        )
        self.not_before = not_before

    def as_dict(self) -> dict[str, object]:
        detail = super().as_dict()
        detail["not_before"] = None if self.not_before is None else self.not_before.isoformat()
        return detail


class TransientReadFailure(LinkedInPublishError):
    """A safe read failed in a way that is worth a bounded retry."""

    code: FailureCode = "transient_read_failure"

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(
            message,
            http_status=http_status,
            provider_code=provider_code,
            request_id=request_id,
            retryable=True,
        )


class PublishOutcomeUnknown(LinkedInPublishError):
    """A write may or may not have been accepted.

    This is never retryable. The durable record stays `unknown` until a human
    reconciles it against the actual LinkedIn surface.
    """

    code: FailureCode = "publish_outcome_unknown"

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(
            message,
            http_status=http_status,
            provider_code=provider_code,
            request_id=request_id,
            retryable=False,
        )


class ProviderRejected(LinkedInPublishError):
    """LinkedIn definitively rejected the request; it was not published."""

    code: FailureCode = "provider_rejected"
