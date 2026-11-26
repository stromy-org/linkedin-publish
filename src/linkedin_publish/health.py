"""Token health: a measured verdict, an expiry warning, and an observation cache.

Three rules, and every helper here exists to keep one of them:

* **Expiry is measured, never assumed.** The only expiry this module reads is
  one a probe returned: the cached observation's `expires_at`, or the
  `token_expires_at` that `account inspect --record` stored on the binding. A
  token's nominal lifetime (`now + 60 days`) is never an input, so a restart
  cannot push the warning date out.
* **Known bad blocks; a failed probe is unknown.** An expired, inactive
  (revoked), wrong-app or wrong-member credential is `blocked`. A probe that
  could not complete is `unknown` with its reason — an outage is not a missing
  token, and the two need different fixes.
* **A cached observation belongs to one credential version.** The cache key is
  `(binding_id, credential_version)`, so a rotated secret is never reported on
  the strength of the previous secret's probe. Rotation also deletes the old
  rows, so nothing stale survives to be read by mistake.

What is cached is a `TokenObservation`: dates, booleans, scope names, the app
id, the app-scoped member id and a short reason. It holds no token, no secret
and no request body — there is nothing in it to sanitize except an unbounded
provider reason string, which `sanitize` caps.

Nothing in this module makes a network request. Probing is the caller's
choice; `refresh_reason` says when one is owed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

from .auth import TokenObservation
from .models import AccountBinding

__all__ = [
    "DAILY_REFRESH",
    "DUE_BATCH_MAX_AGE",
    "EXPIRY_WARNING_WINDOW",
    "NEAR_EXPIRY",
    "ExpiryWarning",
    "InMemoryObservationCache",
    "ObservationCache",
    "ObservationCacheUnavailable",
    "TokenHealth",
    "assess",
    "expiry_warning",
    "measured_expiry",
    "refresh_reason",
    "sanitize",
]

#: Warn this far ahead of a measured expiry. Matches the credential-expiry
#: registry's first LinkedIn lead time, so the tick and the calendar agree.
EXPIRY_WARNING_WINDOW = timedelta(days=14)

#: The scheduled health path re-probes at least this often, even with no work.
DAILY_REFRESH = timedelta(hours=24)

#: Before a due batch, a cached observation older than this is re-probed.
DUE_BATCH_MAX_AGE = timedelta(minutes=15)

#: Before a due batch, a token expiring inside this window is re-probed.
NEAR_EXPIRY = timedelta(days=1)

#: Provider reasons are short codes; anything longer is cut, never stored whole.
_MAX_REASON = 200

TokenStatus = Literal["healthy", "blocked", "unknown"]
Source = Literal["probe", "cache", "none"]

#: Observation reasons that name a wrong credential rather than a dead one.
_IDENTITY_REASONS = {
    "introspection_app_mismatch": "wrong_app",
    "userinfo_member_mismatch": "wrong_identity",
}


class ObservationCacheUnavailable(RuntimeError):
    """The cache could not be read or written (e.g. its migration is not applied).

    Callers report this as `unknown`. It is never read as "no token".
    """


class ObservationCache(Protocol):
    """Sanitized token observations, keyed by `(binding_id, credential_version)`."""

    async def get(self, binding_id: str, credential_version: str) -> TokenObservation | None: ...

    async def put(self, binding_id: str, observation: TokenObservation) -> None:
        """Store `observation` under its own credential version. Never moves backwards in time."""
        ...

    async def invalidate(self, binding_id: str) -> int:
        """Drop every cached observation for `binding_id`. Returns how many went."""
        ...


class InMemoryObservationCache:
    """Process-local `ObservationCache` for tests and single-process tools."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], TokenObservation] = {}

    async def get(self, binding_id: str, credential_version: str) -> TokenObservation | None:
        return self._rows.get((binding_id, credential_version))

    async def put(self, binding_id: str, observation: TokenObservation) -> None:
        key = (binding_id, observation.credential_version)
        existing = self._rows.get(key)
        if existing is not None and existing.observed_at > observation.observed_at:
            return
        self._rows[key] = sanitize(observation)

    async def invalidate(self, binding_id: str) -> int:
        doomed = [key for key in self._rows if key[0] == binding_id]
        for key in doomed:
            del self._rows[key]
        return len(doomed)


def sanitize(observation: TokenObservation) -> TokenObservation:
    """The observation as the cache stores it: bounded reason, UTC times."""
    reason = observation.reason
    if reason is not None and len(reason) > _MAX_REASON:
        reason = reason[:_MAX_REASON]
    return observation.model_copy(
        update={
            "reason": reason,
            "observed_at": observation.observed_at.astimezone(timezone.utc),
            "expires_at": None if observation.expires_at is None else observation.expires_at.astimezone(timezone.utc),
        }
    )


def measured_expiry(
    binding: AccountBinding, observation: TokenObservation | None = None
) -> tuple[datetime | None, Source | Literal["binding"]]:
    """The best measured expiry for the binding's CURRENT credential version.

    A newer observation for the current version wins; otherwise the expiry that
    `inspect --record` stored on the binding. An observation of another version
    is ignored — it measured a different secret.
    """
    if (
        observation is not None
        and observation.credential_version == binding.credential_version
        and observation.expires_at is not None
    ):
        return observation.expires_at, "cache"
    if binding.token_expires_at is not None:
        return binding.token_expires_at, "binding"
    return None, "none"


class ExpiryWarning(BaseModel):
    """A measured expiry inside the warning window (or already past)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: Literal["token_expires_soon", "token_expired"]
    expires_at: datetime
    days_remaining: int
    window_days: int
    source: Literal["cache", "binding"]

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "expires_at": self.expires_at.isoformat(),
            "days_remaining": self.days_remaining,
            "window_days": self.window_days,
            "source": self.source,
        }


def expiry_warning(
    binding: AccountBinding,
    observation: TokenObservation | None = None,
    *,
    now: datetime,
    window: timedelta = EXPIRY_WARNING_WINDOW,
) -> ExpiryWarning | None:
    """A warning when the MEASURED expiry is within `window` of `now`, else None.

    No measured expiry means no warning here — `assess` reports the missing
    measurement as `unknown`, which is its own signal. `days_remaining` is
    floored and goes negative once the token has expired.
    """
    expires_at, source = measured_expiry(binding, observation)
    if expires_at is None or source == "none":
        return None
    remaining = expires_at - now
    if remaining > window:
        return None
    return ExpiryWarning(
        code="token_expired" if remaining <= timedelta(0) else "token_expires_soon",
        expires_at=expires_at,
        days_remaining=remaining.days,
        window_days=window.days,
        source=source,  # type: ignore[arg-type] - "none" returned above
    )


class TokenHealth(BaseModel):
    """One verdict over one credential version, with the facts behind it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: TokenStatus
    reason: str | None
    source: Source
    credential_version: str
    observed_at: datetime | None = None
    expires_at: datetime | None = None
    token_active: bool | None = None
    scopes_ok: bool | None = None
    identity_verified: bool | None = None
    expiry_warning: ExpiryWarning | None = None

    @property
    def blocks_writes(self) -> bool:
        """A known-bad credential stops every write for its binding."""
        return self.status == "blocked"

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reason": self.reason,
            "source": self.source,
            "credential_version": self.credential_version,
            "observed_at": None if self.observed_at is None else self.observed_at.isoformat(),
            "expires_at": None if self.expires_at is None else self.expires_at.isoformat(),
            "token_active": self.token_active,
            "scopes_ok": self.scopes_ok,
            "identity_verified": self.identity_verified,
            "expiry_warning": None if self.expiry_warning is None else self.expiry_warning.as_dict(),
        }


def assess(
    binding: AccountBinding,
    observation: TokenObservation | None,
    *,
    now: datetime,
    source: Source = "cache",
    window: timedelta = EXPIRY_WARNING_WINDOW,
) -> TokenHealth:
    """Turn the latest observation into `healthy`, `blocked` or `unknown`.

    Order matters: a measured expiry in the past blocks even when the last probe
    said active, because that probe is older than the expiry it reported.
    """
    warning = expiry_warning(binding, observation, now=now, window=window)
    expires_at, _ = measured_expiry(binding, observation)

    def verdict(status: TokenStatus, reason: str | None, obs: TokenObservation | None) -> TokenHealth:
        return TokenHealth(
            status=status,
            reason=reason,
            source=source if obs is not None else "none",
            credential_version=binding.credential_version,
            observed_at=None if obs is None else obs.observed_at,
            expires_at=expires_at,
            token_active=None if obs is None else obs.token_active,
            scopes_ok=None if obs is None else obs.scopes_ok,
            identity_verified=None if obs is None else obs.identity_verified,
            expiry_warning=warning,
        )

    if observation is not None and observation.credential_version != binding.credential_version:
        return verdict("blocked", "credential_version_mismatch", observation)
    if warning is not None and warning.code == "token_expired":
        return verdict("blocked", "token_expired", observation)
    if observation is None:
        return verdict("unknown", "not_measured", None)
    if observation.token_active is False:
        return verdict("blocked", "token_inactive", observation)
    if observation.identity_verified is False:
        mapped = _IDENTITY_REASONS.get(observation.reason or "", "wrong_identity")
        return verdict("blocked", mapped, observation)
    if observation.scopes_ok is False:
        return verdict("blocked", "scopes_missing", observation)
    if observation.token_active is None:
        return verdict("unknown", f"probe_failed: {observation.reason or 'no reason given'}", observation)
    if observation.healthy:
        return verdict("healthy", None, observation)
    return verdict("unknown", observation.reason or "not_fully_verified", observation)


def refresh_reason(
    observation: TokenObservation | None, *, now: datetime, due_work: bool
) -> str | None:
    """Why a probe is owed now, or None when the cached observation suffices.

    * `missing`          — nothing cached for this credential version;
    * `daily`            — the cached probe is a day old (owed even with no work);
    * `due_batch_stale`  — work is due and the cached probe is older than 15 minutes;
    * `near_expiry`      — work is due and the token expires within a day.
    """
    if observation is None:
        return "missing"
    age = now - observation.observed_at
    if age >= DAILY_REFRESH:
        return "daily"
    if due_work:
        if age > DUE_BATCH_MAX_AGE:
            return "due_batch_stale"
        if observation.expires_at is not None and observation.expires_at - now <= NEAR_EXPIRY:
            return "near_expiry"
    return None
