"""Meta binding storage: the protocol the operator CLI uses, and an in-memory store.

The Postgres implementation is `social_publish.postgres.PostgresSocialBindingStore`.
Every write here needs the writer role there; the runtime may only read.

* `record_token_evidence` stores a `debug_token` reading on the binding — the
  measured expiry, or explicit no-expiry evidence — only for the binding's
  current credential version. A reading of another version measured another
  secret and is refused.
* `register` creates a DISABLED binding, every capability unknown (a duplicate
  id is a no-op). `rotate` moves a binding to a new credential version, clears
  its measurement, turns publishing off and sets the hold; `reenable` clears the
  hold only after a fresh healthy measurement of the new version
  (`social_publish.commissioning.reenable_problems`), re-checked by the store.
* `enable_capability` is the first enablement of one (operation, format) from
  its canary receipt (`social_publish.commissioning.enabled_binding`).
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Protocol, runtime_checkable

from .commissioning import enabled_binding, reenable_problems, rotated_binding
from .errors import StoreConflict
from .model import SocialBinding
from .tokens import MetaTokenEvidence

__all__ = ["InMemorySocialBindingStore", "SocialBindingStore", "token_fields"]


def token_fields(binding: SocialBinding, evidence: MetaTokenEvidence) -> dict[str, object]:
    """The binding columns a recordable reading sets. Raises `StoreConflict` otherwise."""
    if binding.platform == "linkedin":
        raise StoreConflict("a LinkedIn binding records its token with `account inspect --record`")
    if evidence.credential_version != binding.credential_version:
        raise StoreConflict(
            f"the reading is of credential version {evidence.credential_version!r}; binding "
            f"{binding.binding_id!r} names {binding.credential_version!r}"
        )
    if not evidence.recordable:
        raise StoreConflict(
            f"the reading carries no expiry fact ({evidence.reason or 'no reason'}); nothing is recorded, "
            "and the binding's expiry stays unmeasured"
        )
    return {
        "token_expires_at": evidence.expires_at,
        "token_observed_at": evidence.observed_at,
        "token_never_expires": evidence.never_expires,
    }


@runtime_checkable
class SocialBindingStore(Protocol):
    async def register(self, binding: SocialBinding) -> bool: ...

    async def get(self, binding_id: str) -> SocialBinding | None: ...

    async def find_account(self, platform: str, native_account_id: str) -> SocialBinding | None: ...

    async def rotate(
        self,
        binding_id: str,
        *,
        expected_version: str,
        credential_ref: str,
        credential_version: str,
        at: datetime,
    ) -> SocialBinding: ...

    async def reenable(self, binding_id: str, *, expected_version: str, now: datetime) -> SocialBinding: ...

    async def record_token_evidence(self, binding_id: str, evidence: MetaTokenEvidence) -> SocialBinding: ...

    async def enable_capability(
        self,
        binding_id: str,
        *,
        operation: str,
        format: str,  # noqa: A002 - the domain word
        evidence_publication_id: str,
        observed_at: datetime,
        expected_version: str,
    ) -> SocialBinding: ...


class InMemorySocialBindingStore:
    """Process-local Meta bindings with the Postgres store's rules. For tests and offline use."""

    def __init__(self, bindings: list[SocialBinding] | None = None) -> None:
        self._bindings: dict[str, SocialBinding] = {b.binding_id: b for b in bindings or []}
        self._lock = asyncio.Lock()

    async def register(self, binding: SocialBinding) -> bool:
        async with self._lock:
            if binding.binding_id in self._bindings:
                return False
            self._bindings[binding.binding_id] = binding.model_copy(
                update={"publish_enabled": False, "capabilities": ()}
            )
            return True

    async def get(self, binding_id: str) -> SocialBinding | None:
        return self._bindings.get(binding_id)

    async def find_account(self, platform: str, native_account_id: str) -> SocialBinding | None:
        for binding in self._bindings.values():
            if (binding.platform, binding.native_account_id) == (platform, native_account_id):
                return binding
        return None

    async def rotate(
        self,
        binding_id: str,
        *,
        expected_version: str,
        credential_ref: str,
        credential_version: str,
        at: datetime,
    ) -> SocialBinding:
        async with self._lock:
            binding = self._bindings.get(binding_id)
            if binding is None:
                raise StoreConflict(f"binding {binding_id!r} is not registered")
            updated = rotated_binding(
                binding,
                expected_version=expected_version,
                credential_ref=credential_ref,
                credential_version=credential_version,
                at=at,
            )
            self._bindings[binding_id] = updated
            return updated

    async def reenable(self, binding_id: str, *, expected_version: str, now: datetime) -> SocialBinding:
        async with self._lock:
            binding = self._bindings.get(binding_id)
            if binding is None:
                raise StoreConflict(f"binding {binding_id!r} is not registered")
            if binding.credential_version != expected_version:
                raise StoreConflict(f"binding {binding_id!r} changed since it was checked; re-run the check")
            problems = reenable_problems(binding, now=now)
            if problems:
                raise StoreConflict("; ".join(problems))
            updated = binding.model_copy(update={"publish_enabled": True, "reenable_after_measure": False})
            self._bindings[binding_id] = updated
            return updated

    async def record_token_evidence(self, binding_id: str, evidence: MetaTokenEvidence) -> SocialBinding:
        async with self._lock:
            binding = self._bindings.get(binding_id)
            if binding is None:
                raise StoreConflict(f"binding {binding_id!r} is not registered")
            updated = SocialBinding.model_validate({**binding.model_dump(), **token_fields(binding, evidence)})
            self._bindings[binding_id] = updated
            return updated

    async def enable_capability(
        self,
        binding_id: str,
        *,
        operation: str,
        format: str,  # noqa: A002 - the domain word
        evidence_publication_id: str,
        observed_at: datetime,
        expected_version: str,
    ) -> SocialBinding:
        async with self._lock:
            binding = self._bindings.get(binding_id)
            if binding is None:
                raise StoreConflict(f"binding {binding_id!r} is not registered")
            if binding.credential_version != expected_version:
                raise StoreConflict(f"binding {binding_id!r} changed since it was checked; re-run the check")
            updated = enabled_binding(
                binding,
                operation=operation,
                format=format,
                evidence_publication_id=evidence_publication_id,
                observed_at=observed_at,
            )
            self._bindings[binding_id] = updated
            return updated

    async def disable(self, binding_id: str) -> SocialBinding:
        async with self._lock:
            binding = self._bindings.get(binding_id)
            if binding is None:
                raise StoreConflict(f"binding {binding_id!r} is not registered")
            updated = binding.model_copy(update={"publish_enabled": False})
            self._bindings[binding_id] = updated
            return updated
