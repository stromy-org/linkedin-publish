"""`rest_posts` — the versioned Posts API.

Documented at learn.microsoft.com/linkedin/marketing/community-management/
shares/posts-api. Preferred for an approved CMA binding; a Share-only binding
may select it **only after commissioning proves it**, never by discovering that
UGC returned a 403.

Its media URNs are `urn:li:image:*` and `urn:li:document:*`. A GET on a REST
image is not available to a write-only `w_member_social` token, so this adapter
never builds a polling loop around upload status — an upload either returns its
URN or the capability stays unavailable.
"""

from __future__ import annotations

import httpx

from .._http import map_write_failure, request_id, response_object, restli_id, write_transport_failure
from .._json import as_object
from ..auth import Credentials
from ..errors import ProviderRejected, PublishOutcomeUnknown
from ..media import AssetReader, ResolvedAsset, check_upload_destination, upload_bytes
from ..models import Adapter, ArticleDraft, DocumentDraft, ImageDraft, PostDraft
from ..version import (
    API_BASE,
    LINKEDIN_VERSION,
    REST_DOCUMENTS_INITIALIZE_PATH,
    REST_IMAGES_INITIALIZE_PATH,
    REST_POSTS_PATH,
    rest_headers,
)
from .base import PublishOutcome, UploadedMedia, permalink_for

__all__ = ["RestPostsAdapter"]


class RestPostsAdapter:
    """Transport for `/rest/posts`, `/rest/images` and `/rest/documents`."""

    name: Adapter = "rest_posts"

    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        api_base: str = API_BASE,
        linkedin_version: str = LINKEDIN_VERSION,
    ) -> None:
        self._http = http
        self._api_base = api_base
        self._version = linkedin_version

    async def upload_image(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
    ) -> UploadedMedia:
        return await self._initialize_and_upload(
            credentials,
            author_urn,
            reader,
            subject_id,
            asset,
            path=REST_IMAGES_INITIALIZE_PATH,
            urn_key="image",
            what="rest image upload",
        )

    async def upload_document(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
    ) -> UploadedMedia:
        return await self._initialize_and_upload(
            credentials,
            author_urn,
            reader,
            subject_id,
            asset,
            path=REST_DOCUMENTS_INITIALIZE_PATH,
            urn_key="document",
            what="rest document upload",
        )

    async def _initialize_and_upload(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
        *,
        path: str,
        urn_key: str,
        what: str,
    ) -> UploadedMedia:
        try:
            response = await self._http.post(
                f"{self._api_base}{path}",
                json={"initializeUploadRequest": {"owner": author_urn}},
                headers=rest_headers(credentials.access_token, version=self._version),
            )
        except httpx.HTTPError as exc:
            raise write_transport_failure(exc, what=what) from exc

        if response.status_code >= 400:
            raise map_write_failure(response, what=what)

        value = _value(response)
        upload_url = value.get("uploadUrl")
        media_urn = value.get(urn_key)
        if not isinstance(upload_url, str) or not isinstance(media_urn, str):
            raise ProviderRejected(
                f"{what}: initializeUpload returned no usable URL or URN",
                http_status=response.status_code,
                request_id=request_id(response),
            )

        check_upload_destination(upload_url)
        # The REST upload URL is pre-signed; forwarding the member token to it
        # would leak the credential to an origin that does not need it.
        await upload_bytes(self._http, upload_url, reader, subject_id, asset, access_token=None)
        return UploadedMedia(urn=media_urn, adapter="rest_posts")

    async def create_post(
        self,
        credentials: Credentials,
        draft: PostDraft,
        media_urn: str | None,
    ) -> PublishOutcome:
        body: dict[str, object] = {
            "author": draft.author_urn,
            "commentary": draft.commentary,
            "visibility": draft.visibility,
            "distribution": {
                "feedDistribution": "MAIN_FEED",
                "targetEntities": [],
                "thirdPartyDistributionChannels": [],
            },
            "lifecycleState": "PUBLISHED",
            "isReshareDisabledByAuthor": False,
        }

        media = draft.media
        if isinstance(media, ImageDraft):
            if media_urn is None:
                raise ProviderRejected("an image post requires an uploaded image URN")
            entry: dict[str, object] = {"id": media_urn}
            if media.alt_text:
                entry["altText"] = media.alt_text
            body["content"] = {"media": entry}
        elif isinstance(media, DocumentDraft):
            if media_urn is None:
                raise ProviderRejected("a document post requires an uploaded document URN")
            body["content"] = {"media": {"id": media_urn, "title": media.title}}
        elif isinstance(media, ArticleDraft):
            article: dict[str, object] = {"source": str(media.url)}
            if media.title:
                article["title"] = media.title
            if media.description:
                article["description"] = media.description
            if media_urn is not None:
                article["thumbnail"] = media_urn
            body["content"] = {"article": article}

        try:
            response = await self._http.post(
                f"{self._api_base}{REST_POSTS_PATH}",
                json=body,
                headers=rest_headers(credentials.access_token, version=self._version),
            )
        except httpx.HTTPError as exc:
            raise write_transport_failure(exc, what="rest create") from exc

        if response.status_code >= 400:
            raise map_write_failure(response, what="rest create")

        post_urn = restli_id(response)
        if post_urn is None:
            raise PublishOutcomeUnknown(
                "rest create returned success without x-restli-id; the post may exist",
                http_status=response.status_code,
                request_id=request_id(response),
            )
        return PublishOutcome(post_urn=post_urn, permalink=permalink_for(post_urn), adapter="rest_posts")


def _value(response: httpx.Response) -> dict[str, object]:
    return as_object(response_object(response).get("value"))
