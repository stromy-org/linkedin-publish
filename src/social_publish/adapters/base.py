"""The contract every provider adapter implements.

The core owns intent, approval, windows, claims, attempt state, receipts and
reconciliation. An adapter owns what only the provider knows: native ids,
credentials, format and visibility rules, media staging, rate-limit
interpretation and provider read-back. The split is enforced by what an adapter
is handed — a binding and a record, never the store — and by what it may answer:

* `declared()` — every (operation, format) it implements at all. The core
  refuses anything else before transport; it never asks for a nearby format.
* `validate()` — offline checks; raises `Refused`. No network.
* `stage()` — repeat-safe preparation before the one write (an Instagram media
  container). It reports staging through `persist` *before* acting on it, so a
  crash leaves the container id durable.
* `preflight()` — live, read-only checks just before the write (a reply's parent,
  the Instagram publishing quota). Raises `Refused` or `Deferred`.
* `send()` — the one write. Raises `Refused` (definitely not sent), `Deferred`
  (definitely not sent, later) or `OutcomeUnknown`.
* `read_back()` / `reconcile()` — read-only. They answer `Resolution`; an
  unresolved answer keeps the record `unknown`. They never write to the
  provider and never call `send()`.
* `cancel()` — withdraw a remotely scheduled object; True only when a read-back
  shows it gone.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from ..model import SocialBinding, SocialRecord

__all__ = ["Persist", "Resolution", "SendResult", "SocialAdapter", "StageResult"]

#: Durably record staging on the claimed row before acting on it.
Persist = Callable[[dict[str, object]], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class StageResult:
    """`ready` may send now; `wait` releases the claim and keeps the staging;
    `restage` releases it and discards the staging (an expired container);
    `failed` is a definite provider rejection (nothing was published);
    `anomaly` means the provider shows something we did not do — held as unknown."""

    kind: Literal["ready", "wait", "restage", "failed", "anomaly"]
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class SendResult:
    """The provider accepted the write. `scheduled` means it holds a native schedule;
    `cancelled` means the read-back did not match what was approved, and the
    adapter withdrew it and read the withdrawal back."""

    state: Literal["published", "scheduled", "cancelled"]
    remote_id: str
    permalink: str | None = None
    published_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Resolution:
    """A read-only verdict. `state=None` is unresolved: the record stays as it is."""

    state: Literal["published", "scheduled", "failed", "cancelled", "unknown"] | None
    remote_id: str | None = None
    reason: str | None = None
    permalink: str | None = None


class SocialAdapter(Protocol):
    """One provider product, selected from the trusted binding's `adapter` name."""

    name: str
    platforms: frozenset[str]

    def declared(self) -> frozenset[tuple[str, str]]: ...

    def validate(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> None: ...

    async def stage(
        self, binding: SocialBinding, record: SocialRecord, now: datetime, persist: Persist
    ) -> StageResult: ...

    async def preflight(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> None: ...

    async def send(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> SendResult: ...

    async def read_back(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> Resolution: ...

    async def reconcile(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> Resolution: ...

    async def cancel(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> bool: ...
