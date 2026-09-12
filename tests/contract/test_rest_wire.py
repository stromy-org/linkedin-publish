"""Wire contract for `rest_posts` (the versioned Posts API)."""

from __future__ import annotations

import json

import httpx
import pytest

from linkedin_publish import ArticleDraft, DocumentDraft, ImageDraft, MediaRef, PostDraft
from linkedin_publish.errors import PublishOutcomeUnknown
from linkedin_publish.version import LINKEDIN_VERSION
from tests.conftest import NOW, PDF_SHA, PERSON, PNG_SHA, FakeAssetReader, binding, make_client

pytestmark = pytest.mark.contract

SIGNED_UPLOAD = "https://upload.linkedin.test/rest/put?sig=abc"
POST_URN = "urn:li:share:7200000000000000000"

REST_BINDING = dict(adapter="rest_posts")


def created(request: httpx.Request) -> httpx.Response:
    return httpx.Response(201, json={}, headers={"x-restli-id": POST_URN})


async def test_a_text_post_serializes_to_the_documented_rest_shape(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created)
    draft = PostDraft(author_urn=PERSON, commentary="Intelligence, orchestrated.")

    receipt = await client.publish(
        binding(**REST_BINDING), credentials, draft, publication_id="p1", now=NOW
    )

    assert log.paths() == ["/rest/posts"]
    body = json.loads(log.last().body)
    assert body["author"] == PERSON
    assert body["commentary"] == "Intelligence, orchestrated."
    assert body["visibility"] == "PUBLIC"
    assert body["lifecycleState"] == "PUBLISHED"
    assert body["isReshareDisabledByAuthor"] is False
    assert body["distribution"]["feedDistribution"] == "MAIN_FEED"
    assert receipt.adapter == "rest_posts"
    await client.aclose()


async def test_rest_sends_both_the_version_and_restli_headers(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created)
    await client.publish(
        binding(**REST_BINDING), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
    )
    headers = log.last().headers
    assert headers["linkedin-version"] == LINKEDIN_VERSION
    assert headers["x-restli-protocol-version"] == "2.0.0"
    await client.aclose()


async def test_the_linkedin_version_is_configured_centrally(credentials) -> None:  # noqa: ANN001
    """One place to change it at release or commissioning — never per call site."""
    client, log = make_client(created, linkedin_version="202612")
    await client.publish(
        binding(**REST_BINDING), credentials, PostDraft(author_urn=PERSON, commentary="hi"), publication_id="p1"
    )
    assert log.last().headers["linkedin-version"] == "202612"
    await client.aclose()


async def test_an_image_post_uses_the_rest_image_urn_space(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    image_urn = "urn:li:image:C4E10AQ"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rest/images":
            return httpx.Response(200, json={"value": {"uploadUrl": SIGNED_UPLOAD, "image": image_urn}})
        if request.url.host == "upload.linkedin.test":
            return httpx.Response(201)
        return created(request)

    client, log = make_client(handler)
    draft = PostDraft(
        author_urn=PERSON,
        commentary="Chart",
        media=ImageDraft(asset=MediaRef(sha256=PNG_SHA), alt_text="A bar chart"),
    )
    await client.publish(
        binding(capabilities=("text", "image"), **REST_BINDING),
        credentials,
        draft,
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
    )

    assert log.paths() == ["/rest/images", "/rest/put", "/rest/posts"]
    content = json.loads(log.last().body)["content"]
    assert content["media"]["id"] == image_urn
    assert content["media"]["altText"] == "A bar chart"
    await client.aclose()


async def test_a_document_post_uses_the_rest_document_urn_space(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    """This is where an approved PDF "carousel" actually lands."""
    document_urn = "urn:li:document:C4E20AQ"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rest/documents":
            return httpx.Response(
                200, json={"value": {"uploadUrl": SIGNED_UPLOAD, "document": document_urn}}
            )
        if request.url.host == "upload.linkedin.test":
            return httpx.Response(201)
        return created(request)

    client, log = make_client(handler)
    draft = PostDraft(
        author_urn=PERSON,
        commentary="Our Q3 deck",
        media=DocumentDraft(asset=MediaRef(sha256=PDF_SHA), title="Q3 review"),
    )
    await client.publish(
        binding(capabilities=("text", "document"), **REST_BINDING),
        credentials,
        draft,
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
    )
    assert log.paths() == ["/rest/documents", "/rest/put", "/rest/posts"]
    content = json.loads(log.last().body)["content"]
    assert content["media"]["id"] == document_urn
    assert content["media"]["title"] == "Q3 review"
    await client.aclose()


async def test_the_signed_rest_upload_never_receives_the_member_token(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    upload_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rest/images":
            return httpx.Response(
                200, json={"value": {"uploadUrl": SIGNED_UPLOAD, "image": "urn:li:image:X"}}
            )
        if request.url.host == "upload.linkedin.test":
            upload_headers.update(dict(request.headers))
            return httpx.Response(201)
        return created(request)

    client, _log = make_client(handler)
    draft = PostDraft(author_urn=PERSON, commentary="hi", media=ImageDraft(asset=MediaRef(sha256=PNG_SHA)))
    await client.publish(
        binding(capabilities=("text", "image"), **REST_BINDING),
        credentials,
        draft,
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
    )
    assert "authorization" not in upload_headers
    await client.aclose()


async def test_an_article_thumbnail_is_an_uploaded_image_urn(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rest/images":
            return httpx.Response(
                200, json={"value": {"uploadUrl": SIGNED_UPLOAD, "image": "urn:li:image:T"}}
            )
        if request.url.host == "upload.linkedin.test":
            return httpx.Response(201)
        return created(request)

    client, _log = make_client(handler)
    draft = PostDraft(
        author_urn=PERSON,
        commentary="Read this",
        media=ArticleDraft(
            url="https://stromy.com.au/x",  # type: ignore[arg-type]
            title="X",
            thumbnail=MediaRef(sha256=PNG_SHA),
        ),
    )
    await client.publish(
        binding(capabilities=("text", "article"), **REST_BINDING),
        credentials,
        draft,
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
    )
    article = json.loads(_log.last().body)["content"]["article"]
    assert article["source"] == "https://stromy.com.au/x"
    assert article["thumbnail"] == "urn:li:image:T"
    await client.aclose()


async def test_a_201_without_x_restli_id_is_unknown(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(201, json={"id": POST_URN}))
    with pytest.raises(PublishOutcomeUnknown, match="without x-restli-id"):
        await client.publish(
            binding(**REST_BINDING),
            credentials,
            PostDraft(author_urn=PERSON, commentary="hi"),
            publication_id="p1",
        )
    await client.aclose()


async def test_no_polling_loop_is_built_around_upload_status(
    credentials, reader: FakeAssetReader
) -> None:  # noqa: ANN001
    """A REST image GET is unavailable to a write-only token, so we never ask."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            raise AssertionError(f"unexpected read: {request.url}")
        if request.url.path == "/rest/images":
            return httpx.Response(
                200, json={"value": {"uploadUrl": SIGNED_UPLOAD, "image": "urn:li:image:X"}}
            )
        if request.url.host == "upload.linkedin.test":
            return httpx.Response(201)
        return created(request)

    client, log = make_client(handler)
    draft = PostDraft(author_urn=PERSON, commentary="hi", media=ImageDraft(asset=MediaRef(sha256=PNG_SHA)))
    await client.publish(
        binding(capabilities=("text", "image"), **REST_BINDING),
        credentials,
        draft,
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
    )
    assert all(call.method != "GET" for call in log.calls)
    await client.aclose()


async def test_the_adapter_comes_from_the_binding_not_the_draft(credentials) -> None:  # noqa: ANN001
    """Identical drafts, different bindings, different products. No inference."""
    draft = PostDraft(author_urn=PERSON, commentary="same words")

    ugc_client, ugc_log = make_client(created)
    await ugc_client.publish(binding(), credentials, draft, publication_id="p1")
    assert ugc_log.paths() == ["/v2/ugcPosts"]
    await ugc_client.aclose()

    rest_client, rest_log = make_client(created)
    await rest_client.publish(binding(**REST_BINDING), credentials, draft, publication_id="p2")
    assert rest_log.paths() == ["/rest/posts"]
    await rest_client.aclose()
