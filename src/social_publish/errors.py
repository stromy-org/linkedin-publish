"""The platform-neutral failure taxonomy.

Three outcomes matter to the ledger, and every adapter maps its provider's
failures onto exactly one of them:

* `Refused` — definitely not sent. Raised before transport (a gate, a capability,
  a format) or after a provider's definite rejection. Never retried blindly.
* `Deferred` — not sent, try again later: a rate limit or a publishing quota.
* `OutcomeUnknown` — a write may have been accepted. Never retried; reconciled
  read-only against the provider, and held for a human when that is not enough.

`code` is the stable refusal name a caller branches on (the names follow the
`social-ops` refusal taxonomy, so a C3 refusal and a ledger refusal read the
same). Nothing here carries an upstream body: only a sanitized status, the
provider's short code and its request id survive `sanitize_detail`.

A leaf module: `linkedin_publish.errors` imports `sanitize_detail` from here, so
there is one chokepoint for both namespaces.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Final

__all__ = [
    "ApprovalRefused",
    "CapabilityRefused",
    "Deferred",
    "OutcomeUnknown",
    "Refused",
    "SocialPublishError",
    "StoreConflict",
    "sanitize_detail",
]

# Provider error codes are short machine tokens; anything longer is prose that
# may quote the submitted post, so it is dropped rather than truncated.
MAX_PROVIDER_CODE: Final = 64
MAX_REQUEST_ID: Final = 128
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


class StoreConflict(Exception):
    """A compare-and-set lost, or a key was reused with a different payload.

    One class for both namespaces: `linkedin_publish.store.StoreConflict` is this.
    """


class SocialPublishError(Exception):
    """Base class. `code` is the refusal name; `message` never quotes content."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.http_status = http_status
        self.provider_code = sanitize_detail(provider_code, limit=MAX_PROVIDER_CODE)
        self.request_id = sanitize_detail(request_id, limit=MAX_REQUEST_ID)

    def as_dict(self) -> dict[str, object]:
        """A log/export-safe rendering. Contains no submitted content."""
        return {
            "code": self.code,
            "message": self.message,
            "http_status": self.http_status,
            "provider_code": self.provider_code,
            "request_id": self.request_id,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code!r}, http_status={self.http_status!r})"


class Refused(SocialPublishError):
    """Definitely not sent.

    `final` closes the record (`failed`): the target is gone or the provider
    definitively rejected the request. A non-final refusal is a gate that may
    pass later; the record stays `pending` until its window ends.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        final: bool = False,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(code, message, http_status=http_status, provider_code=provider_code, request_id=request_id)
        self.final = final


class CapabilityRefused(Refused):
    """The binding has not commissioned this exact operation and format.

    Raised before any network call. The core never answers this by trying a
    "nearby" format: a carousel is not degraded into an image, and an image is
    not degraded into text.
    """

    def __init__(self, message: str, *, operation: str, format: str) -> None:  # noqa: A002 - the domain word
        super().__init__("capability_unavailable", message, final=False)
        self.operation = operation
        self.format = format

    def as_dict(self) -> dict[str, object]:
        detail = super().as_dict()
        detail["operation"] = self.operation
        detail["format"] = self.format
        return detail


class ApprovalRefused(Refused):
    """An approval could not be imported or no longer licenses the send."""


class Deferred(SocialPublishError):
    """Not sent; try again no earlier than `not_before` (aware UTC) when given."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        not_before: datetime | None = None,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(code, message, http_status=http_status, provider_code=provider_code, request_id=request_id)
        self.not_before = not_before

    def as_dict(self) -> dict[str, object]:
        detail = super().as_dict()
        detail["not_before"] = None if self.not_before is None else self.not_before.isoformat()
        return detail


class OutcomeUnknown(SocialPublishError):
    """A write may or may not have been accepted. Never retried.

    `remote_id` is set when the provider returned an id but the result could not
    be confirmed (a created post that did not read back): the ledger keeps it so
    reconciliation reads that object instead of searching.
    """

    def __init__(
        self,
        message: str,
        *,
        remote_id: str | None = None,
        http_status: int | None = None,
        provider_code: object = None,
        request_id: object = None,
    ) -> None:
        super().__init__(
            "unknown_outcome", message, http_status=http_status, provider_code=provider_code, request_id=request_id
        )
        self.remote_id = remote_id
