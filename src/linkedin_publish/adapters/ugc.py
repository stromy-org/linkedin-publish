"""`share_ugc` — the self-service Share on LinkedIn product.

Documented at learn.microsoft.com/linkedin/consumer/integrations/self-serve/
share-on-linkedin. Text, article and image only: **document posts do not exist
on this surface**, and asking for one returns `capability_unavailable` rather
than degrading to a text post.

Its media URNs are `urn:li:digitalmediaAsset:*` and are not interchangeable with
the REST surface's `urn:li:image:*` / `urn:li:document:*`.
"""

from __future__ import annotations

import httpx

from .._http import map_write_failure, request_id, response_object, restli_id, write_transport_failure
from .._json import as_object
from ..auth import Credentials
from ..errors import CapabilityUnavailable, ProviderRejected, PublishOutcomeUnknown
from ..media import AssetReader, ResolvedAsset, check_upload_destination, upload_bytes
from ..models import Adapter, ArticleDraft, ImageDraft, PostDraft
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


def _value(response: httpx.Response) -> dict[str, object]:
    return as_object(response_object(response).get("value"))


def _body_id(response: httpx.Response) -> str | None:
    value = response_object(response).get("id")
    return value if isinstance(value, str) and value.startswith("urn:li:") else None
