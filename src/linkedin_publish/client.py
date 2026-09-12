"""The official LinkedIn client.

Usable entirely on its own — no database, no approval ledger — which is what
keeps the transport testable against fixtures. Every hosted publishing path
nonetheless goes through `service.PublicationService`, because only the durable
ledger can promise that an accepted post is never sent twice.

Order of operations is load-bearing and is the same on every call:

1. Select the adapter **from the binding**. Never from the draft, never from a
   previous failure.
2. Check the author against the binding.
3. Check the capability is commissioned. This happens before any network call,
   so an unavailable capability costs nothing and leaks nothing.
4. Resolve and validate media bytes.
5. Reserve quota, then upload, then reserve again, then create.

A dry run stops after step 4 and has made no request of any kind.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from .adapters import PostAdapter, RestPostsAdapter, ShareUgcAdapter
from .adapters.base import PublishOutcome
from .analytics import Granularity, fetch_share_statistics
from .auth import Credentials
from .errors import AuthorForbidden, CapabilityUnavailable, QuotaDeferred, ValidationFailure
from .limits import Endpoint, QuotaLimiter
from .media import AssetReader, ResolvedAsset, media_kind, sniff_and_validate
from .models import (
    AccountBinding,
    ArticleDraft,
    Capability,
    DeleteOutcome,
    DocumentDraft,
    ImageDraft,
    MediaRef,
    PostDraft,
    PostSnapshot,
    PublishReceipt,
    ShareStatistics,
)
from .version import API_BASE, LINKEDIN_VERSION

__all__ = ["LinkedInClient", "PreparedPost", "default_timeout"]


def default_timeout() -> httpx.Timeout:
    """Explicit connect/read/write/pool timeouts.

    httpx's default is a single 5s value for all four. A large document upload
    legitimately takes far longer to write than to connect, and one shared
    number either throttles uploads or lets a dead connection hang a job.
    """
    return httpx.Timeout(connect=10.0, read=30.0, write=120.0, pool=10.0)


def default_limits() -> httpx.Limits:
    """A bounded pool. A publishing job is not a crawler."""
    return httpx.Limits(max_connections=8, max_keepalive_connections=4)


@dataclass(frozen=True, slots=True)
class PreparedPost:
    """A validated post that has made no network call. The dry-run result."""

    draft: PostDraft
    adapter: str
    asset: ResolvedAsset | None
    capability: str


class LinkedInClient:
    """Owns an `httpx.AsyncClient` and both adapters.

    Use as an async context manager so the pool is closed deterministically:

        async with LinkedInClient() as client:
            await client.publish(...)
    """

    def __init__(
        self,
        *,
        http: httpx.AsyncClient | None = None,
        api_base: str = API_BASE,
        linkedin_version: str = LINKEDIN_VERSION,
        limiter: QuotaLimiter | None = None,
    ) -> None:
        self._owns_http = http is None
        self._http = http or httpx.AsyncClient(
            timeout=default_timeout(),
            limits=default_limits(),
            follow_redirects=False,
        )
        self._limiter = limiter
        self._api_base = api_base
        self._linkedin_version = linkedin_version
        self._ugc = ShareUgcAdapter(self._http, api_base=api_base)
        self._rest = RestPostsAdapter(self._http, api_base=api_base, linkedin_version=linkedin_version)

    async def __aenter__(self) -> LinkedInClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the pool, if this client created it."""
        if self._owns_http:
            await self._http.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        return self._http

    def adapter_for(self, binding: AccountBinding) -> PostAdapter:
        """The adapter this binding is configured for. Never inferred."""
        return self._ugc if binding.adapter == "share_ugc" else self._rest

    def check(self, binding: AccountBinding, draft: PostDraft) -> str:
        """Assert the binding may publish this draft. Returns the capability name.

        Raises before any network call, so a refusal costs nothing.
        """
        if not binding.may_author(draft.author_urn):
            raise AuthorForbidden(
                f"binding {binding.binding_id} is not authorized to publish as {draft.author_urn}"
            )
        capability = draft.required_capability
        state = binding.capability_state(capability)
        if state != "enabled":
            raise CapabilityUnavailable(
                f"capability {capability!r} is {state} on binding {binding.binding_id}; "
                "it is enabled by recorded commissioning evidence, not by a successful call of another shape",
                capability=capability,
            )
        if isinstance(draft.media, DocumentDraft) and binding.adapter == "share_ugc":
            raise CapabilityUnavailable(
                "document posts require a rest_posts binding; the Share (UGC) product has no document surface",
                capability="document",
            )
        return capability

    async def prepare(
        self,
        binding: AccountBinding,
        draft: PostDraft,
        *,
        reader: AssetReader | None = None,
        subject_id: str | None = None,
    ) -> PreparedPost:
        """Validate everything that can be validated offline.

        This is the dry-run path in full: zero LinkedIn requests, zero uploads,
        zero quota consumed, zero durable mutation.
        """
        capability = self.check(binding, draft)
        asset: ResolvedAsset | None = None
        media = draft.media
        ref = _asset_ref(media) if media is not None else None
        if ref is not None:
            if reader is None or subject_id is None:
                raise ValidationFailure("this post carries media; an asset reader and subject are required")
            assert media is not None  # noqa: S101 - a ref only exists when media does
            asset = await sniff_and_validate(reader, subject_id, ref, kind=media_kind(media))
        return PreparedPost(draft=draft, adapter=binding.adapter, asset=asset, capability=capability)

    async def publish(
        self,
        binding: AccountBinding,
        credentials: Credentials,
        draft: PostDraft,
        *,
        publication_id: str,
        reader: AssetReader | None = None,
        subject_id: str | None = None,
        now: datetime | None = None,
    ) -> PublishReceipt:
        """Upload any media, then create the post. One attempt, no fallback.

        The caller is responsible for the durable state transition *before*
        calling this — by the time we are here, a crash must leave an `unknown`
        record, not a `pending` one.
        """
        prepared = await self.prepare(binding, draft, reader=reader, subject_id=subject_id)
        adapter = self.adapter_for(binding)

        moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

        media_urn: str | None = None
        if prepared.asset is not None:
            assert reader is not None and subject_id is not None  # noqa: S101 - established by prepare()
            await self._reserve(binding, draft, endpoint="register", now=now)
            await self._reserve(binding, draft, endpoint="upload", now=now)
            try:
                if isinstance(draft.media, DocumentDraft):
                    uploaded = await adapter.upload_document(
                        credentials, draft.author_urn, reader, subject_id, prepared.asset
                    )
                else:
                    uploaded = await adapter.upload_image(
                        credentials, draft.author_urn, reader, subject_id, prepared.asset
                    )
            except QuotaDeferred as exc:
                raise _rebase_deferral(exc, moment) from exc
            media_urn = uploaded.urn

        await self._reserve(binding, draft, endpoint="create", now=now)
        try:
            outcome: PublishOutcome = await adapter.create_post(credentials, draft, media_urn)
        except QuotaDeferred as exc:
            raise _rebase_deferral(exc, moment) from exc
        return PublishReceipt(
            publication_id=publication_id,
            post_urn=outcome.post_urn,
            permalink=outcome.permalink,
            adapter=outcome.adapter,
            published_at=moment,
        )

    async def get_post(
        self,
        binding: AccountBinding,
        credentials: Credentials,
        post_urn: str,
        *,
        now: datetime | None = None,
    ) -> PostSnapshot:
        """Read a post back from LinkedIn, if this binding may read at all.

        Gated on the `read_post` capability, checked before the network. The
        pilot's `w_member_social` is write-only, so for most bindings this raises
        `capability_unavailable` — which is the honest answer. Returning an empty
        result instead would read as "the post is gone".
        """
        self._require_capability(binding, "read_post")
        await self._reserve_urn(binding, post_urn, endpoint="read", now=now)
        return await self.adapter_for(binding).get_post(credentials, post_urn)

    async def delete_post(
        self,
        binding: AccountBinding,
        credentials: Credentials,
        post_urn: str,
        *,
        now: datetime | None = None,
    ) -> DeleteOutcome:
        """Delete a post. Raw transport — the caller supplies an authorized target.

        `PublicationService.delete_publication` is the safe entry point: it
        resolves the URN from this binding's own receipt ledger first, so a URN
        cannot be passed in from outside and deleted.
        """
        self._require_capability(binding, "delete")
        await self._reserve_urn(binding, post_urn, endpoint="delete", now=now)
        return await self.adapter_for(binding).delete_post(credentials, post_urn)

    async def share_statistics(
        self,
        binding: AccountBinding,
        credentials: Credentials,
        organization_urn: str,
        *,
        start: datetime,
        end: datetime,
        granularity: Granularity = "DAY",
        now: datetime | None = None,
    ) -> ShareStatistics:
        """Organic share statistics for an organization this binding may read.

        Needs `rw_organization_admin` plus an ADMINISTRATOR role — neither of
        which a successful publish demonstrates, hence its own capability. The
        organization must also be in the binding's allowlist: a CMA token that
        happens to reach another Page is not authorization to report on it.
        """
        self._require_capability(binding, "share_statistics")
        if organization_urn not in binding.allowed_organization_urns:
            raise AuthorForbidden(
                f"binding {binding.binding_id} is not authorized to read statistics for "
                f"{organization_urn}"
            )
        if binding.adapter != "rest_posts":
            raise CapabilityUnavailable(
                "share statistics are a versioned-REST (CMA) surface; the Share product has none",
                capability="share_statistics",
            )
        await self._reserve_urn(binding, binding.author_urn, endpoint="read", now=now)
        return await fetch_share_statistics(
            self._http,
            credentials.access_token,
            organization_urn,
            start=start,
            end=end,
            granularity=granularity,
            api_base=self._api_base,
            linkedin_version=self._linkedin_version,
            now=now,
        )

    def _require_capability(self, binding: AccountBinding, capability: Capability) -> None:
        state = binding.capability_state(capability)
        if state != "enabled":
            raise CapabilityUnavailable(
                f"capability {capability!r} is {state} on binding {binding.binding_id}",
                capability=capability,
            )

    async def _reserve_urn(
        self,
        binding: AccountBinding,
        member_urn: str,
        *,
        endpoint: Endpoint,
        now: datetime | None,
    ) -> None:
        if self._limiter is None:
            return
        await self._limiter.reserve(
            app_id=binding.app_id, member_urn=member_urn, endpoint=endpoint, now=now
        )

    async def _reserve(
        self,
        binding: AccountBinding,
        draft: PostDraft,
        *,
        endpoint: Endpoint,
        now: datetime | None,
    ) -> None:
        if self._limiter is None:
            return
        await self._limiter.reserve(
            app_id=binding.app_id,
            member_urn=draft.author_urn,
            endpoint=endpoint,
            now=now,
        )


def _rebase_deferral(exc: QuotaDeferred, moment: datetime) -> QuotaDeferred:
    """Express a provider deferral in the caller's clock frame.

    `Retry-After` is a *delay*, and the transport resolves it against the wall
    clock because that is the only clock it has. Every window check upstream —
    due, expiry, not-before — uses the tick's own `now`. Leaving the two frames
    mixed lets a deferral land outside the approval window it was supposed to
    stay inside, and makes the behaviour untestable without sleeping.
    """
    if exc.not_before is None:
        return exc
    delay = exc.not_before - datetime.now(timezone.utc)
    if delay.total_seconds() < 0:
        delay = timedelta(0)
    return QuotaDeferred(
        exc.message,
        not_before=moment + delay,
        http_status=exc.http_status,
        provider_code=exc.provider_code,
        request_id=exc.request_id,
    )


def _asset_ref(media: ImageDraft | ArticleDraft | DocumentDraft) -> MediaRef | None:
    """The bytes this media draft needs resolved, if any.

    An article with no thumbnail needs none — the link preview is LinkedIn's to
    render, not ours to upload.
    """
    if isinstance(media, (ImageDraft, DocumentDraft)):
        return media.asset
    return media.thumbnail
