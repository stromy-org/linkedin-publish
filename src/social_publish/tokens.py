# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
#
# A debug_token response is untrusted JSON; each field is type-checked where it
# is read, and only dates, a boolean, the app id and scope names leave.
"""Meta token-expiry evidence: measured from `debug_token`, never assumed.

A Meta binding publishes with an organic system-user token (or a Page token
derived from it). Meta issues these either with a 60-day expiry or with none,
so "no expiry recorded" is ambiguous: never measured, or measured as never
expiring. This module makes the second an explicit, recorded fact.

**Source.** Graph `GET /debug_token?input_token=<token>`
(https://developers.facebook.com/docs/graph-api/reference/debug_token/)
returns `data.is_valid`, `data.app_id`, `data.scopes`, and `data.expires_at`
and `data.data_access_expires_at` as Unix timestamps. Meta documents
system-user tokens that never expire next to 60-day ones
(https://developers.facebook.com/docs/marketing-api/system-users/install-apps-and-generate-tokens)
and long-lived Page tokens without an expiration date
(https://developers.facebook.com/docs/facebook-login/guides/access-tokens/get-long-lived/).
For such a token `debug_token` reports `expires_at: 0` (the Access Token
Debugger shows it as "Never"). The reference page does not spell out the `0`,
so the rule here is deliberately narrow:

* `expires_at` a positive timestamp → a **measured expiry**;
* `expires_at` exactly `0` on a **valid** token of the **expected app** →
  explicit **no-expiry evidence**;
* anything else (missing, malformed, invalid token, another app) → nothing is
  inferred; the evidence carries a reason and the expiry stays unmeasured.

`inspect_meta_token` sends the token being inspected in `input_token` (the
endpoint takes it nowhere else) and the inspecting token in the
`Authorization` header. A transport error is reported by class name only,
never with the request URL.

`meta_token_status` is what `meta status` and a tick report. It uses the same
`expiry_warning` the LinkedIn path uses (14-day window, measured expiry only),
so an approaching or past Meta expiry warns exactly like a LinkedIn one.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, model_validator

from .adapters.meta import GRAPH_BASE, GRAPH_VERSION, MetaCredentials
from .model import SocialBinding

__all__ = [
    "DEBUG_TOKEN_REFERENCE",
    "MetaTokenEvidence",
    "inspect_meta_token",
    "meta_token_status",
    "parse_debug_token",
]

DEBUG_TOKEN_REFERENCE = "https://developers.facebook.com/docs/graph-api/reference/debug_token/"  # noqa: S105 - a URL
_MAX_REASON = 200


class MetaTokenEvidence(BaseModel):
    """One sanitized `debug_token` reading for one credential version. Holds no token."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    credential_version: str
    observed_at: datetime
    is_valid: bool | None = None
    app_id: str | None = None
    expires_at: datetime | None = None
    never_expires: bool = False
    data_access_expires_at: datetime | None = None
    scopes: tuple[str, ...] = ()
    reason: str | None = None

    @model_validator(mode="after")
    def _consistent(self) -> MetaTokenEvidence:
        if self.never_expires and (self.expires_at is not None or self.is_valid is not True):
            raise ValueError("no-expiry evidence needs a valid token and no measured expiry")
        if self.reason is not None and len(self.reason) > _MAX_REASON:
            raise ValueError("reason is a short code")
        return self

    @property
    def recordable(self) -> bool:
        """Whether this reading carries an expiry fact worth storing on the binding."""
        return self.never_expires or self.expires_at is not None

    def as_dict(self) -> dict[str, object]:
        return {
            "credential_version": self.credential_version,
            "observed_at": self.observed_at.isoformat(),
            "is_valid": self.is_valid,
            "app_id": self.app_id,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "never_expires": self.never_expires,
            "data_access_expires_at": self.data_access_expires_at.isoformat() if self.data_access_expires_at else None,
            "scopes": list(self.scopes),
            "reason": self.reason,
        }


def _timestamp(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def parse_debug_token(
    body: object, *, credential_version: str, observed_at: datetime, expected_app_id: str | None
) -> MetaTokenEvidence:
    """Read one `debug_token` response body into evidence. Never raises on provider content."""
    observed = observed_at.astimezone(UTC)
    base: dict[str, Any] = {"credential_version": credential_version, "observed_at": observed}
    if not isinstance(body, Mapping):
        return MetaTokenEvidence(**base, reason="debug_token_malformed")
    error = body.get("error")
    if isinstance(error, Mapping):
        code = error.get("code")
        return MetaTokenEvidence(**base, reason=f"debug_token_error:{code if isinstance(code, int) else '?'}")
    data = body.get("data")
    if not isinstance(data, Mapping):
        return MetaTokenEvidence(**base, reason="debug_token_malformed")
    valid = data.get("is_valid")
    is_valid = valid if isinstance(valid, bool) else None
    raw_app = data.get("app_id")
    app_id = str(raw_app) if isinstance(raw_app, (str, int)) and not isinstance(raw_app, bool) else None
    scopes_raw = data.get("scopes")
    scopes = tuple(sorted(s for s in scopes_raw if isinstance(s, str))) if isinstance(scopes_raw, list) else ()
    access = _timestamp(data.get("data_access_expires_at"))
    data_access = datetime.fromtimestamp(access, UTC) if access else None
    fields: dict[str, Any] = {
        **base,
        "is_valid": is_valid,
        "app_id": app_id,
        "scopes": scopes,
        "data_access_expires_at": data_access,
    }
    if is_valid is not True:
        return MetaTokenEvidence(**fields, reason="token_invalid" if is_valid is False else "validity_not_reported")
    if expected_app_id is not None and app_id != expected_app_id:
        return MetaTokenEvidence(**fields, reason="app_mismatch")
    expires = _timestamp(data.get("expires_at"))
    if expires is None or expires < 0:
        return MetaTokenEvidence(**fields, reason="expiry_not_reported")
    if expires == 0:
        return MetaTokenEvidence(**fields, never_expires=True)
    return MetaTokenEvidence(**fields, expires_at=datetime.fromtimestamp(expires, UTC))


async def inspect_meta_token(
    http: httpx.AsyncClient,
    credentials: MetaCredentials,
    *,
    expected_app_id: str | None,
    now: datetime,
    inspector_token: str | None = None,
    api_base: str = GRAPH_BASE,
) -> MetaTokenEvidence:
    """One read-only `debug_token` call. `inspector_token` defaults to the token itself."""
    url = f"{api_base.rstrip('/')}/{GRAPH_VERSION}/debug_token"
    base: dict[str, Any] = {"credential_version": credentials.credential_version, "observed_at": now.astimezone(UTC)}
    try:
        response = await http.get(
            url,
            params={"input_token": credentials.access_token},
            headers={"Authorization": f"Bearer {inspector_token or credentials.access_token}"},
        )
    except httpx.HTTPError as exc:
        return MetaTokenEvidence(**base, reason=f"probe_transport:{type(exc).__name__}"[:_MAX_REASON])
    try:
        body: object = response.json()
    except ValueError:
        return MetaTokenEvidence(**base, reason=f"debug_token_http_{response.status_code}")
    return parse_debug_token(
        body,
        credential_version=credentials.credential_version,
        observed_at=now,
        expected_app_id=expected_app_id,
    )


TokenStatus = Literal["healthy", "blocked", "unknown"]


def meta_token_status(binding: SocialBinding, *, now: datetime) -> dict[str, object]:
    """The stored token verdict of a Meta binding and its expiry warning. No request, no credential."""
    # Imported here, not at module level: `linkedin_publish` imports leaf modules
    # of this package while it initializes, so a top-level import would cycle.
    from linkedin_publish.health import expiry_warning

    status: TokenStatus
    if binding.token_observed_at is None:
        status, reason = "unknown", "no token evidence recorded; run `meta inspect --record`"
    elif binding.token_never_expires:
        status, reason = "healthy", "never_expires (debug_token expires_at=0)"
    elif binding.token_expires_at is None:
        status, reason = "unknown", "expiry not measured"
    elif binding.token_expires_at <= now:
        status, reason = "blocked", "token_expired"
    else:
        status, reason = "healthy", None
    warning = expiry_warning(binding, None, now=now)
    return {
        "status": status,
        "reason": reason,
        "credential_version": binding.credential_version,
        "token_expires_at": binding.token_expires_at.isoformat() if binding.token_expires_at else None,
        "token_never_expires": binding.token_never_expires,
        "token_observed_at": binding.token_observed_at.isoformat() if binding.token_observed_at else None,
        "expiry_warning": warning.as_dict() if warning is not None else None,
    }
