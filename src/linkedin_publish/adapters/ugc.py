"""`share_ugc` — the self-service Share on LinkedIn product.

Documented at learn.microsoft.com/linkedin/consumer/integrations/self-serve/
share-on-linkedin. Text, article and image only: **document posts do not exist
on this surface**, and asking for one returns `capability_unavailable` rather
than degrading to a text post.

Its media URNs are `urn:li:digitalmediaAsset:*` and are not interchangeable with
the REST surface's `urn:li:image:*` / `urn:li:document:*`.
"""

from __future__ import annotations

from urllib.parse import quote

import httpx

from .._http import (
    map_read_failure,
    map_write_failure,
    request_id,
    response_object,
    restli_id,
    write_transport_failure,
)
from .._json import as_object
from ..auth import Credentials
from ..errors import (
    CapabilityUnavailable,
    ProviderRejected,
    PublishOutcomeUnknown,
    TransientReadFailure,
)
from ..media import AssetReader, ResolvedAsset, check_upload_destination, upload_bytes
from ..models import Adapter, ArticleDraft, DeleteOutcome, ImageDraft, PostDraft, PostSnapshot
from ..version import API_BASE, UGC_ASSET_REGISTER_PATH, UGC_POSTS_PATH, ugc_headers
from .base import PublishOutcome, UploadedMedia, permalink_for

__all__ = ["ShareUgcAdapter"]

_FEEDSHARE_IMAGE = "urn:li:digitalmediaRecipe:feedshare-image"
_UPLOAD_MECHANISM = "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"


class ShareUgcAdapter:
    """Transport for `/v2/ugcPosts`."""

    name: Adapter = "share_ugc"

    def __init__(self, http: httpx.AsyncClient, *, api_base: str = API_BASE) -> None:
        self._http = http
        self._api_base = api_base

    async def upload_image(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
    ) -> UploadedMedia:
        """Register an upload slot, PUT the bytes, return the asset URN."""
        body = {
            "registerUploadRequest": {
                "owner": author_urn,
                "recipes": [_FEEDSHARE_IMAGE],
                "serviceRelationships": [
                    {"relationshipType": "OWNER", "identifier": "urn:li:userGeneratedContent"}
                ],
            }
        }
        try:
            response = await self._http.post(
                f"{self._api_base}{UGC_ASSET_REGISTER_PATH}",
                json=body,
                headers=ugc_headers(credentials.access_token),
            )
        except httpx.HTTPError as exc:
            raise write_transport_failure(exc, what="ugc registerUpload") from exc

        if response.status_code >= 400:
            raise map_write_failure(response, what="ugc registerUpload")

        value = _value(response)
        asset_urn = value.get("asset")
        mechanism = as_object(value.get("uploadMechanism"))
        detail = as_object(mechanism.get(_UPLOAD_MECHANISM))
        upload_url = detail.get("uploadUrl")

        if not isinstance(asset_urn, str) or not isinstance(upload_url, str):
            raise ProviderRejected(
                "ugc registerUpload returned no usable asset or upload URL",
                http_status=response.status_code,
                request_id=request_id(response),
            )

        check_upload_destination(upload_url)
        # The UGC upload endpoint is documented as requiring the member token.
        await upload_bytes(
            self._http,
            upload_url,
            reader,
            subject_id,
            asset,
            access_token=credentials.access_token,
        )
        return UploadedMedia(urn=asset_urn, adapter="share_ugc")

    async def upload_document(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
    ) -> UploadedMedia:
        raise CapabilityUnavailable(
            "document posts are not available on the Share (UGC) product; "
            "a document requires a commissioned rest_posts binding",
            capability="document",
        )

    async def create_post(
        self,
        credentials: Credentials,
        draft: PostDraft,
        media_urn: str | None,
    ) -> PublishOutcome:
        share_content: dict[str, object] = {
            "shareCommentary": {"text": draft.commentary},
            "shareMediaCategory": "NONE",
        }

        media = draft.media
        if isinstance(media, ImageDraft):
            if media_urn is None:
                raise ProviderRejected("an image post requires an uploaded asset URN")
            entry: dict[str, object] = {"status": "READY", "media": media_urn}
            if media.alt_text:
                entry["description"] = {"text": media.alt_text}
            share_content["shareMediaCategory"] = "IMAGE"
            share_content["media"] = [entry]
        elif isinstance(media, ArticleDraft):
            entry = {"status": "READY", "originalUrl": str(media.url)}
            if media.title:
                entry["title"] = {"text": media.title}
            if media.description:
                entry["description"] = {"text": media.description}
            share_content["shareMediaCategory"] = "ARTICLE"
            share_content["media"] = [entry]
        elif media is not None:
            raise CapabilityUnavailable(
                f"{media.kind} posts are not available on the Share (UGC) product",
                capability=media.kind,
            )

        body = {
            "author": draft.author_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {"com.linkedin.ugc.ShareContent": share_content},
            "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": draft.visibility},
        }

        try:
            response = await self._http.post(
                f"{self._api_base}{UGC_POSTS_PATH}",
                json=body,
                headers=ugc_headers(credentials.access_token),
            )
        except httpx.HTTPError as exc:
            raise write_transport_failure(exc, what="ugc create") from exc

        if response.status_code >= 400:
            raise map_write_failure(response, what="ugc create")

        post_urn = restli_id(response) or _body_id(response)
        if post_urn is None:
            # Accepted, but we cannot name what was created. Never a retry.
            raise PublishOutcomeUnknown(
                "ugc create returned success without a post URN; the post may exist",
                http_status=response.status_code,
                request_id=request_id(response),
            )
        return PublishOutcome(post_urn=post_urn, permalink=permalink_for(post_urn), adapter="share_ugc")


    async def get_post(self, credentials: Credentials, post_urn: str) -> PostSnapshot:
        """Read one post back from the provider.

        A read is safe to retry, so its failures map through `map_read_failure`
        rather than the write taxonomy. A 403 here means the credential lacks the
        read permission — reported as such, never as "the post does not exist".
        """
        try:
            response = await self._http.get(
                self._post_url(post_urn), headers=self._headers(credentials.access_token)
            )
        except httpx.HTTPError as exc:
            raise TransientReadFailure(f"{self.name} get: {type(exc).__name__}") from exc

        if response.status_code >= 400:
            raise map_read_failure(response, what=f"{self.name} get")
        return self._snapshot(post_urn, response_object(response))

    async def delete_post(self, credentials: Credentials, post_urn: str) -> DeleteOutcome:
        """Delete one post and report what the provider actually said.

        A 404 is NOT reported as a successful delete. It means this credential
        has no such post — which may be because it is already gone, or because it
        was never visible to this app. Collapsing those into 204 would turn "you
        never had access" into "it is gone", and an operator would stop looking.
        """
        try:
            response = await self._http.delete(
                self._post_url(post_urn), headers=self._headers(credentials.access_token)
            )
        except httpx.HTTPError as exc:
            # A delete is a write: an unproven outcome is unknown, never a retry.
            raise PublishOutcomeUnknown(
                f"{self.name} delete: transport failure ({type(exc).__name__}); outcome unknown"
            ) from exc

        if response.status_code in (200, 204):
            return DeleteOutcome(
                post_urn=post_urn, deleted=True, existed=True, http_status=response.status_code
            )
        if response.status_code == 404:
            return DeleteOutcome(
                post_urn=post_urn,
                deleted=False,
                existed=False,
                http_status=404,
                reason="provider has no such post for this credential — already deleted, or never visible to this app",
            )
        raise map_write_failure(response, what=f"{self.name} delete")


    def _post_url(self, post_urn: str) -> str:
        # UGC addresses a post by its URL-encoded URN in the path.
        return f"{self._api_base}{UGC_POSTS_PATH}/{quote(post_urn, safe='')}"

    def _headers(self, access_token: str) -> dict[str, str]:
        return ugc_headers(access_token)

    @staticmethod
    def _snapshot(post_urn: str, payload: dict[str, object]) -> PostSnapshot:
        content = as_object(
            as_object(payload.get("specificContent")).get("com.linkedin.ugc.ShareContent")
        )
        commentary = as_object(content.get("shareCommentary")).get("text")
        visibility = as_object(payload.get("visibility")).get(
            "com.linkedin.ugc.MemberNetworkVisibility"
        )
        author = payload.get("author")
        state = payload.get("lifecycleState")
        return PostSnapshot(
            post_urn=post_urn,
            author_urn=author if isinstance(author, str) else None,
            commentary=commentary if isinstance(commentary, str) else None,
            visibility=visibility if isinstance(visibility, str) else None,
            raw_lifecycle_state=state if isinstance(state, str) else None,
            permalink=permalink_for(post_urn),
        )


def _value(response: httpx.Response) -> dict[str, object]:
    return as_object(response_object(response).get("value"))


def _body_id(response: httpx.Response) -> str | None:
    value = response_object(response).get("id")
    return value if isinstance(value, str) and value.startswith("urn:li:") else None
