"""Attempt states and the transitions between them — one owner for every platform.

The promise is narrower than "exactly once": **no automatic replay after an
ambiguous send**. Two asymmetries carry it and must never be collapsed:

* a dead `claimed` lease returns to `pending` (nothing left the process);
* a dead `sending` lease becomes `unknown` (the request may have been accepted).

Two states exist only for providers that hold work remotely:

* `scheduled` — the provider holds a native schedule (a Facebook Page scheduled
  post). Read-back moves it to `published`; a withdrawal to `cancelled`. It is
  never fresh work: the dispatcher only reads it.
* `handoff` — a human took the approved packet into the provider's own planner
  (the Instagram v1 fallback). The ledger records it and never sends it.

`linkedin_publish.store` takes its table from `LINKEDIN_TRANSITIONS`, which is
this table restricted to the eight states a LinkedIn row can be in — so a
LinkedIn row can never enter `scheduled` or `handoff`.

A leaf module: it imports nothing from this package or from `linkedin_publish`.
"""

from __future__ import annotations

from typing import Literal

__all__ = [
    "LINKEDIN_STATES",
    "LINKEDIN_TRANSITIONS",
    "NEVER_FRESH",
    "SocialState",
    "TRANSITIONS",
    "allowed",
]

SocialState = Literal[
    "pending",
    "claimed",
    "sending",
    "scheduled",
    "handoff",
    "published",
    "failed",
    "unknown",
    "expired",
    "cancelled",
]

#: The states a LinkedIn publication may hold (migration 0001's CHECK, unchanged).
LINKEDIN_STATES: frozenset[str] = frozenset(
    {"pending", "claimed", "sending", "published", "failed", "unknown", "expired", "cancelled"}
)

TRANSITIONS: dict[str, frozenset[str]] = {
    "pending": frozenset({"claimed", "expired", "cancelled"}),
    # A claimed attempt may persist provider staging (an Instagram container id)
    # and stay claimed; it returns to pending only when nothing was sent.
    "claimed": frozenset({"pending", "sending", "cancelled", "failed", "unknown"}),
    # A lease that expires mid-send becomes `unknown`, never `pending`: the send
    # may have been accepted, and `pending` is a licence to retry.
    "sending": frozenset({"published", "scheduled", "pending", "failed", "unknown", "cancelled"}),
    "scheduled": frozenset({"published", "cancelled", "unknown"}),
    "handoff": frozenset({"published", "cancelled", "unknown"}),
    "unknown": frozenset({"published", "scheduled", "failed"}),
    "published": frozenset(),
    "failed": frozenset(),
    "expired": frozenset(),
    "cancelled": frozenset(),
}

#: The LinkedIn table, exactly as `linkedin_publish.store` has always declared it.
LINKEDIN_TRANSITIONS: dict[str, frozenset[str]] = {
    "pending": frozenset({"claimed", "expired", "cancelled"}),
    "claimed": frozenset({"pending", "sending", "cancelled"}),
    "sending": frozenset({"published", "pending", "failed", "unknown"}),
    "unknown": frozenset({"published", "failed"}),
    "published": frozenset(),
    "failed": frozenset(),
    "expired": frozenset(),
    "cancelled": frozenset(),
}

#: States an import or a tick may never turn into fresh work: something may
#: already exist at the provider, or a human holds it.
NEVER_FRESH: frozenset[str] = frozenset({"sending", "scheduled", "handoff", "published", "unknown"})


def allowed(current: str, target: str, *, table: dict[str, frozenset[str]] | None = None) -> bool:
    """Whether `current -> target` is a legal transition (a no-op always is)."""
    if current == target:
        return True
    return target in (table or TRANSITIONS).get(current, frozenset())


def _check_linkedin_subset() -> None:
    """The LinkedIn table must stay a subset of the neutral one (asserted at import)."""
    for state, targets in LINKEDIN_TRANSITIONS.items():
        if not targets <= TRANSITIONS[state] or state not in LINKEDIN_STATES:
            raise AssertionError(f"LinkedIn transition table is not a subset at {state!r}")


_check_linkedin_subset()
