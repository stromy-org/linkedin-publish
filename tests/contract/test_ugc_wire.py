"""Wire contract for `share_ugc` (Share on LinkedIn, `/v2/ugcPosts`)."""

from __future__ import annotations

import json

import httpx
import pytest

from linkedin_publish import ArticleDraft, DocumentDraft, ImageDraft, MediaRef, PostDraft
from linkedin_publish.errors import (
    AuthorForbidden,
    CapabilityUnavailable,
    ProviderRejected,
    PublishOutcomeUnknown,
)
from tests.conftest import NOW, PERSON, PNG_SHA, FakeAssetReader, binding, make_client

pytestmark = pytest.mark.contract

SIGNED_UPLOAD = "https://upload.linkedin.test/media/put?sig=abc"
POST_URN = "urn:li:share:7100000000000000000"


def created(request: httpx.Request) -> httpx.Response:
    return httpx.Response(201, json={}, headers={"x-restli-id": POST_URN})


async def test_a_text_post_serializes_to_the_documented_ugc_shape(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created)
    draft = PostDraft(author_urn=PERSON, commentary="Intelligence, orchestrated.")

    receipt = await client.publish(binding(), credentials, draft, publication_id="p1", now=NOW)

    assert log.paths() == ["/v2/ugcPosts"]
    body = json.loads(log.last().body)
    assert body["author"] == PERSON
    assert body["lifecycleState"] == "PUBLISHED"
    content = body["specificContent"]["com.linkedin.ugc.ShareContent"]
    assert content["shareCommentary"]["text"] == "Intelligence, orchestrated."
    assert content["shareMediaCategory"] == "NONE"
    assert body["visibility"]["com.linkedin.ugc.MemberNetworkVisibility"] == "PUBLIC"
    assert receipt.post_urn == POST_URN
    assert receipt.permalink == f"https://www.linkedin.com/feed/update/{POST_URN}/"
    assert receipt.adapter == "share_ugc"
    await client.aclose()


async def test_ugc_sends_restli_but_not_a_linkedin_version_header(credentials) -> None:  # noqa: ANN001
    """UGC is not part of the versioned API; sending a version conflates the two."""
    client, log = make_client(created)
    await client.publish(
        binding(), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
    )
    headers = log.last().headers
    assert headers["x-restli-protocol-version"] == "2.0.0"
    assert "linkedin-version" not in headers
    assert headers["authorization"] == "Bearer test-token-not-a-real-credential"
    await client.aclose()


async def test_an_image_post_registers_uploads_and_references_a_digitalmediaasset(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    asset_urn = "urn:li:digitalmediaAsset:D5600"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v2/assets":
            return httpx.Response(
                200,
                json={
                    "value": {
                        "asset": asset_urn,
                        "uploadMechanism": {
                            "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest": {
                                "uploadUrl": SIGNED_UPLOAD
                            }
                        },
                    }
                },
            )
        if request.url.host == "upload.linkedin.test":
            return httpx.Response(201)
        return created(request)

    client, log = make_client(handler)
    draft = PostDraft(
        author_urn=PERSON,
        commentary="Chart of the week",
        media=ImageDraft(asset=MediaRef(sha256=PNG_SHA), alt_text="A bar chart"),
    )
    await client.publish(
        binding(capabilities=("text", "image")),
        credentials,
        draft,
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
    )

    assert log.paths() == ["/v2/assets", "/media/put", "/v2/ugcPosts"]
    content = json.loads(log.last().body)["specificContent"]["com.linkedin.ugc.ShareContent"]
    assert content["shareMediaCategory"] == "IMAGE"
    assert content["media"][0]["media"] == asset_urn
    assert content["media"][0]["description"]["text"] == "A bar chart"
    await client.aclose()


async def test_an_article_post_carries_the_original_url(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created)
    draft = PostDraft(
        author_urn=PERSON,
        commentary="Worth a read",
        media=ArticleDraft(url="https://stromy.com.au/insights/x", title="Insight"),  # type: ignore[arg-type]
    )
    await client.publish(
        binding(capabilities=("text", "article")), credentials, draft, publication_id="p1"
    )
    content = json.loads(log.last().body)["specificContent"]["com.linkedin.ugc.ShareContent"]
    assert content["shareMediaCategory"] == "ARTICLE"
    assert content["media"][0]["originalUrl"] == "https://stromy.com.au/insights/x"
    await client.aclose()


async def test_a_document_on_ugc_is_refused_before_any_request(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    """There is no document surface on Share. It never degrades to text."""
    client, log = make_client(created)
    draft = PostDraft(
        author_urn=PERSON,
        commentary="Our Q3 deck",
        media=DocumentDraft(asset=MediaRef(sha256=PNG_SHA), title="Q3"),
    )
    with pytest.raises(CapabilityUnavailable) as caught:
        await client.publish(
            binding(capabilities=("text", "document")),
            credentials,
            draft,
            publication_id="p1",
            reader=reader,
            subject_id="subject-1",
        )
    assert caught.value.capability == "document"
    assert log.count == 0
    await client.aclose()


async def test_a_403_never_triggers_a_second_adapter(credentials) -> None:  # noqa: ANN001
    """No runtime fallback. One binding, one product, one attempt."""
    client, log = make_client(lambda _r: httpx.Response(403, json={"serviceErrorCode": "ACCESS_DENIED"}))
    with pytest.raises(AuthorForbidden):
        await client.publish(
            binding(), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
        )
    assert log.paths() == ["/v2/ugcPosts"]
    assert "/rest/posts" not in log.paths()
    await client.aclose()


async def test_a_201_without_an_id_is_unknown_not_a_retry(credentials) -> None:  # noqa: ANN001
    """Accepted, but we cannot name what was created. That is `unknown`."""
    client, _log = make_client(lambda _r: httpx.Response(201, json={}))
    with pytest.raises(PublishOutcomeUnknown) as caught:
        await client.publish(
            binding(), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
        )
    assert caught.value.retryable is False
    await client.aclose()


async def test_a_urn_in_the_body_is_accepted_when_the_header_is_absent(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(201, json={"id": POST_URN}))
    receipt = await client.publish(
        binding(), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
    )
    assert receipt.post_urn == POST_URN
    await client.aclose()


async def test_a_register_upload_without_a_url_is_a_rejection(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(200, json={"value": {}}))
    draft = PostDraft(
        author_urn=PERSON, commentary="hi", media=ImageDraft(asset=MediaRef(sha256=PNG_SHA))
    )
    with pytest.raises(ProviderRejected, match="no usable asset or upload URL"):
        await client.publish(
            binding(capabilities=("text", "image")),
            credentials,
            draft,
            publication_id="p1",
            reader=reader,
            subject_id="subject-1",
        )
    await client.aclose()


async def test_a_write_timeout_is_unknown(credentials) -> None:  # noqa: ANN001
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.WriteTimeout("gone", request=request)

    client, _log = make_client(handler)
    with pytest.raises(PublishOutcomeUnknown):
        await client.publish(
            binding(), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
        )
    await client.aclose()
