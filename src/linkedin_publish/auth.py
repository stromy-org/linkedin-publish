"""Credentials, token introspection and OIDC identity.

Three facts are measured *independently* and reported independently, because
each fails differently and conflating them is how a dead credential reads green:

* `token_active`     — introspection says the token is live.
* `scopes_ok`        — the observed scopes cover what the binding declares.
* `identity_verified` — `/v2/userinfo` returns the app-scoped member this
  binding expects.

A 200 from the public OIDC discovery document proves none of the three. Neither
does a successful read. A probe that *fails* yields `unknown`, never "no token" —
an outage and a revocation are different states and are never merged.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Protocol, Self, runtime_checkable

import httpx
from pydantic import BaseModel, ConfigDict

from ._http import response_object
from .errors import CredentialMissing, TransientReadFailure
from .version import INTROSPECT_PATH, OAUTH_BASE, USERINFO_PATH

__all__ = [
    "Credentials",
    "CredentialProvider",
    "StaticCredentialProvider",
    "TokenObservation",
    "introspect_token",
    "verify_identity",
]


@dataclass(frozen=True, slots=True)
class Credentials:
    """One app's secrets plus the member token it issued.

    `repr` is overridden so a credential cannot reach a log, a traceback frame
    dump or an exception message by accident. There is no `__str__` that reveals
    the values and no serializer — read the attributes explicitly or not at all.
    """

    access_token: str = field(repr=False)
    client_id: str
    client_secret: str = field(repr=False)
    credential_version: str = "unversioned"

    def __repr__(self) -> str:
        return f"Credentials(client_id={self.client_id!r}, credential_version={self.credential_version!r})"


@runtime_checkable
class CredentialProvider(Protocol):
    """Resolves the credential for one binding.

    The hosted MCP implements this over versioned Key Vault references. This
    library never reads an environment variable or a vault itself, which is what
    keeps it client-neutral and testable without secrets.
    """

    async def get(self, binding_id: str) -> Credentials: ...


class StaticCredentialProvider:
    """A single in-memory credential. For fixtures and single-binding jobs."""

    def __init__(self, credentials: Credentials, *, binding_id: str | None = None) -> None:
        self._credentials = credentials
        self._binding_id = binding_id

    async def get(self, binding_id: str) -> Credentials:
        if self._binding_id is not None and binding_id != self._binding_id:
            raise CredentialMissing(f"no credential configured for binding {binding_id!r}")
        return self._credentials


class TokenObservation(BaseModel):
    """What one health probe actually measured.

    `token_active is None` means the probe did not complete. It is never coerced
    to False: "we could not reach LinkedIn" and "LinkedIn says this token is
    dead" lead to different operator actions.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    observed_at: datetime
    credential_version: str
    token_active: bool | None = None
    scopes_ok: bool | None = None
    identity_verified: bool | None = None
    observed_scopes: tuple[str, ...] = ()
    expires_at: datetime | None = None
    app_id: str | None = None
    member_sub: str | None = None
    reason: str | None = None

    @property
    def healthy(self) -> bool:
        """True only when all three measurements are affirmatively true."""
        return bool(self.token_active and self.scopes_ok and self.identity_verified)

    @property
    def known_bad(self) -> bool:
        """True when the provider affirmatively said the token is not usable."""
        return self.token_active is False

    def expires_within(self, delta: timedelta, *, now: datetime | None = None) -> bool | None:
        """Whether the token expires inside `delta`; None when expiry is unknown."""
        if self.expires_at is None:
            return None
        reference = now or datetime.now(timezone.utc)
        return self.expires_at - reference <= delta

    def with_reason(self, reason: str) -> Self:
        return self.model_copy(update={"reason": reason})


def _scopes(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, str):
        return ()
    return tuple(sorted({part for part in raw.replace(",", " ").split() if part}))


async def introspect_token(
    http: httpx.AsyncClient,
    credentials: Credentials,
    *,
    declared_scopes: tuple[str, ...] = (),
    now: datetime | None = None,
) -> TokenObservation:
    """POST the documented introspection endpoint and record what it said.

    The token travels in the form body, never in the URL or argv, so it cannot
    land in a proxy log or a shell history. This is a read-only inspection: it
    creates nothing and changes nothing.
    """
    observed_at = now or datetime.now(timezone.utc)
    base = TokenObservation(observed_at=observed_at, credential_version=credentials.credential_version)
    try:
        response = await http.post(
            f"{OAUTH_BASE}{INTROSPECT_PATH}",
            data={
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
                "token": credentials.access_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    except httpx.HTTPError as exc:
        return base.with_reason(f"introspection_unreachable: {type(exc).__name__}")

    if response.status_code != 200:
        return base.with_reason(f"introspection_http_{response.status_code}")

    payload = response_object(response)
    if not payload:
        return base.with_reason("introspection_unparseable")

    active = payload.get("status") == "active" or payload.get("active") is True
    observed = _scopes(payload.get("scope"))
    expires_at: datetime | None = None
    raw_expiry = payload.get("expires_at")
    if isinstance(raw_expiry, (int, float)) and raw_expiry > 0:
        expires_at = datetime.fromtimestamp(float(raw_expiry), tz=timezone.utc)

    app_id = payload.get("client_id")
    return base.model_copy(
        update={
            "token_active": active,
            "observed_scopes": observed,
            "scopes_ok": set(declared_scopes).issubset(observed) if declared_scopes else None,
            "expires_at": expires_at,
            "app_id": app_id if isinstance(app_id, str) else None,
            "reason": None if active else "introspection_inactive",
        }
    )


async def verify_identity(
    http: httpx.AsyncClient,
    credentials: Credentials,
    *,
    expected_member_sub: str | None,
    api_base: str,
) -> tuple[bool | None, str | None, str | None]:
    """Call `/v2/userinfo` and compare `sub` against the binding's expectation.

    Returns `(identity_verified, member_sub, reason)`. A 403 here means the app
    lacks the OIDC product — reported as unknown with a named reason, not as a
    mismatch, because those need different fixes.

    Only meaningful for an app that actually has Sign In with LinkedIn. A CMA
    binding without OIDC uses operator-verified author mapping instead; an absent
    identity check prevents activation rather than being waved through.
    """
    try:
        response = await http.get(
            f"{api_base}{USERINFO_PATH}",
            headers={"Authorization": f"Bearer {credentials.access_token}"},
        )
    except httpx.HTTPError as exc:
        return None, None, f"userinfo_unreachable: {type(exc).__name__}"

    if response.status_code in (401, 403):
        return None, None, f"userinfo_forbidden_{response.status_code}"
    if response.status_code != 200:
        return None, None, f"userinfo_http_{response.status_code}"

    payload = response_object(response)
    if not payload:
        return None, None, "userinfo_unparseable"

    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub:
        return None, None, "userinfo_missing_sub"
    if expected_member_sub is None:
        return None, sub, "userinfo_no_expectation_configured"
    if sub != expected_member_sub:
        return False, sub, "userinfo_member_mismatch"
    return True, sub, None


class HealthCache:
    """A short, credential-version-keyed cache of the last observation.

    Rotation invalidates by construction: the key includes the credential
    version, so a rotated secret can never be reported healthy on the strength of
    the previous one's probe.
    """

    def __init__(self, ttl_seconds: float = 900.0) -> None:
        self._ttl = ttl_seconds
        self._entries: dict[tuple[str, str], tuple[float, TokenObservation]] = {}

    def get(self, binding_id: str, credential_version: str) -> TokenObservation | None:
        entry = self._entries.get((binding_id, credential_version))
        if entry is None:
            return None
        stored_at, observation = entry
        if time.monotonic() - stored_at > self._ttl:
            return None
        return observation

    def put(self, binding_id: str, observation: TokenObservation) -> None:
        self._entries[(binding_id, observation.credential_version)] = (time.monotonic(), observation)

    def invalidate(self, binding_id: str) -> None:
        for key in [key for key in self._entries if key[0] == binding_id]:
            del self._entries[key]


def require_fresh(observation: TokenObservation | None) -> TokenObservation:
    """Raise rather than let a missing observation read as a healthy one."""
    if observation is None:
        raise TransientReadFailure("no token observation available; probe before publishing")
    return observation
