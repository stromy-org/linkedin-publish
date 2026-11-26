"""The LinkedIn adapter: `linkedin_publish.LinkedInClient` behind the neutral contract.

The LinkedIn implementation is unchanged — its models, URN rules, both
transports, quota limiter and the `PublicationService` the hosted job runs. This
module only presents it to the neutral core so both adapters pass one contract
test, and so a future core-driven LinkedIn tick has nothing left to port:

* operation `publish`, format = the draft's required capability (`text`,
  `article`, `image`, `document`);
* the binding is the original `AccountBinding`; the neutral view is derived from
  it (`neutral_binding`) and never written back;
* LinkedIn's typed failures map one-to-one: `QuotaDeferred` → `Deferred`,
  `PublishOutcomeUnknown` → `OutcomeUnknown`, everything else → `Refused`;
* there is no automated reconciliation: the pilot scopes cannot read member
  posts, so an `unknown` LinkedIn send stays unknown until the operator's
  `publication reconcile` (exactly the v0.7.0 behaviour).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from linkedin_publish.auth import CredentialProvider
from linkedin_publish.client import LinkedInClient
from linkedin_publish.errors import LinkedInPublishError, PublishOutcomeUnknown, QuotaDeferred
from linkedin_publish.media import AssetReader
from linkedin_publish.models import AccountBinding, PostDraft

from ..errors import Deferred, OutcomeUnknown, Refused
from ..model import OperationCapability, SocialBinding, SocialRecord
from .base import Persist, Resolution, SendResult, StageResult

__all__ = ["LINKEDIN_DECLARED", "LinkedInAdapter", "neutral_binding"]

LINKEDIN_DECLARED: frozenset[tuple[str, str]] = frozenset(
    {("publish", "text"), ("publish", "article"), ("publish", "image"), ("publish", "document")}
)


def neutral_binding(binding: AccountBinding) -> SocialBinding:
    """The neutral view of a LinkedIn binding. Capabilities keep their evidence."""
    return SocialBinding(
        binding_id=binding.binding_id,
        account_id=binding.account_id,
        subject_kind=binding.subject_kind,
        subject_id=binding.subject_id,
        app_id=binding.app_id,
        platform="linkedin",
        adapter=binding.adapter,
        author_urn=binding.author_urn,
        credential_ref=binding.credential_ref,
        credential_version=binding.credential_version,
        token_expires_at=binding.token_expires_at,
        token_observed_at=binding.token_observed_at,
        publish_enabled=binding.publish_enabled,
        capabilities=tuple(
            OperationCapability(
                operation="publish",
                format=status.capability,
                state=status.state,
                observed_at=status.observed_at,
                evidence_publication_id=status.evidence_publication_id,
                reason=status.reason,
            )
            for status in binding.capabilities
            if ("publish", status.capability) in LINKEDIN_DECLARED
        ),
    )


def _map(exc: LinkedInPublishError) -> Refused | Deferred | OutcomeUnknown:
    if isinstance(exc, QuotaDeferred):
        not_before = exc.not_before if isinstance(exc.not_before, datetime) else None
        return Deferred(
            "quota_deferred",
            exc.message,
            not_before=not_before,
            http_status=exc.http_status,
            provider_code=exc.provider_code,
            request_id=exc.request_id,
        )
    if isinstance(exc, PublishOutcomeUnknown):
        return OutcomeUnknown(
            exc.message, http_status=exc.http_status, provider_code=exc.provider_code, request_id=exc.request_id
        )
    return Refused(
        exc.code,
        exc.message,
        final=exc.code == "provider_rejected",
        http_status=exc.http_status,
        provider_code=exc.provider_code,
        request_id=exc.request_id,
    )


class LinkedInAdapter:
    """Adapter names `share_ugc` and `rest_posts`: one instance per adapter name."""

    platforms: frozenset[str] = frozenset({"linkedin"})

    def __init__(
        self,
        name: str,
        client: LinkedInClient,
        credentials: CredentialProvider,
        bindings: Mapping[str, AccountBinding],
        *,
        reader: AssetReader | None = None,
    ) -> None:
        if name not in ("share_ugc", "rest_posts"):
            raise ValueError(f"not a LinkedIn adapter name: {name!r}")
        self.name = name
        self._client = client
        self._credentials = credentials
        self._bindings = dict(bindings)
        self._reader = reader

    def declared(self) -> frozenset[tuple[str, str]]:
        return LINKEDIN_DECLARED

    def _native(self, binding: SocialBinding) -> AccountBinding:
        native = self._bindings.get(binding.binding_id)
        if native is None or native.adapter != self.name:
            raise Refused(
                "binding_mismatch", f"binding {binding.binding_id} is not a {self.name} binding here", final=True
            )
        return native

    @staticmethod
    def _draft(record: SocialRecord) -> PostDraft:
        return PostDraft.model_validate(record.payload)

    def validate(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> None:
        try:
            self._client.check(self._native(binding), self._draft(record))
        except LinkedInPublishError as exc:
            raise _map(exc) from exc

    async def stage(self, binding: SocialBinding, record: SocialRecord, now: datetime, persist: Persist) -> StageResult:
        return StageResult("ready")

    async def preflight(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> None:
        return None

    async def send(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> SendResult:
        native = self._native(binding)
        try:
            receipt = await self._client.publish(
                native,
                await self._credentials.get(native.binding_id),
                self._draft(record),
                publication_id=record.publication_id,
                reader=self._reader,
                subject_id=native.subject_id,
                now=now,
            )
        except LinkedInPublishError as exc:
            raise _map(exc) from exc
        return SendResult(
            "published", remote_id=receipt.post_urn, permalink=receipt.permalink, published_at=receipt.published_at
        )

    async def read_back(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> Resolution:
        return Resolution(None)

    async def reconcile(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> Resolution:
        return Resolution(None, reason="operator_reconcile_required")

    async def cancel(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> bool:
        return False
