"""CMA organization capabilities ship disabled behind one named switch (ORG-PLAN-285 C7).

Organization publishing (any post authored as `urn:li:organization:*`, documents
included) and organization share statistics need LinkedIn's Community
Management API product, which no app here has yet. The plan lets those adapters
ship *disabled, with fixture tests*. These tests pin both halves:

* **disabled by default** — every entry point (client, service tick, each
  adapter, `fetch_share_statistics`) refuses with `CmaOrganizationDisabled`,
  naming `cma_organization_enabled`, before any request and before any binding
  state is read;
* **the wire shape when enabled** — recorded against hand-built fixtures of the
  documented versioned REST surface (Posts API, Documents API, Organization Share
  Statistics), through the injectable transport. No network.

Each refusal test is differential: the same call with the switch on reaches the
wire, so the refusal is the switch's and not something else's.
"""

from __future__ import annotations

import json
from datetime import timedelta

import httpx
import pytest

from linkedin_publish import (
    CMA_ORGANIZATION_SWITCH,
    ArticleDraft,
    CapabilityUnavailable,
    CmaOrganizationDisabled,
    DocumentDraft,
    MediaRef,
    PostDraft,
    PublicationKey,
    PublicationRecord,
    fetch_share_statistics,
)
from linkedin_publish.adapters import RestPostsAdapter, ShareUgcAdapter
from linkedin_publish.errors import AuthorForbidden
from linkedin_publish.version import LINKEDIN_VERSION
from tests.conftest import NOW, ORG, PDF_SHA, PERSON, FakeAssetReader, binding, make_client
from tests.contract.test_service import DIGEST, seeded

pytestmark = pytest.mark.contract

POST_URN = "urn:li:share:7300000000000000000"
DOCUMENT_URN = "urn:li:document:D5510AQFx87994pYx0Q"
SIGNED_UPLOAD = "https://upload.linkedin.test/dms-uploads/D5510AQ?sig=abc"
API = "https://api.linkedin.test"

#: A commissioned CMA binding: the person may also author as ORG, and every
#: organization capability carries evidence. Only the build switch is missing.
CMA = {
    "adapter": "rest_posts",
    "capabilities": ("text", "article", "document", "share_statistics"),
    "allowed_orgs": (ORG,),
}


def org_text() -> PostDraft:
    return PostDraft(author_urn=ORG, commentary="Notes from the week.", visibility="PUBLIC")


def org_document() -> PostDraft:
    return PostDraft(
        author_urn=ORG,
        commentary="The quarterly review, as a document.",
        media=DocumentDraft(asset=MediaRef(sha256=PDF_SHA), title="Quarterly review"),
    )


def created(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(201, json={}, headers={"x-restli-id": POST_URN})


def documents_api(request: httpx.Request) -> httpx.Response:
    """Fixture of the documented Documents API: initializeUpload, PUT, then the post."""
    if request.url.path == "/rest/documents":
        return httpx.Response(
            200,
            json={"value": {"uploadUrlExpiresAt": 1650567510704, "uploadUrl": SIGNED_UPLOAD, "document": DOCUMENT_URN}},
        )
    if request.url.host == "upload.linkedin.test":
        return httpx.Response(201)
    return created(request)


def statistics_api(_request: httpx.Request) -> httpx.Response:
    """Fixture of a documented time-bound share-statistics response."""
    return httpx.Response(
        200,
        json={
            "paging": {"count": 10, "start": 0},
            "elements": [
                {
                    "timeRange": {"start": 1757548800000, "end": 1757635200000},
                    "totalShareStatistics": {
                        "clickCount": 4,
                        "engagement": 0.05,
                        "likeCount": 2,
                        "commentCount": 1,
                        "shareCount": 0,
                        "impressionCount": 140,
                        "uniqueImpressionsCount": 90,
                    },
                    "organizationalEntity": ORG,
                }
            ],
        },
    )


# ------------------------------------------------------------- the contract


def test_the_switch_has_one_name_and_its_refusal_is_a_capability_refusal() -> None:
    assert CMA_ORGANIZATION_SWITCH == "cma_organization_enabled"
    assert issubclass(CmaOrganizationDisabled, CapabilityUnavailable)
    exc = CmaOrganizationDisabled("x", capability="text", switch=CMA_ORGANIZATION_SWITCH)
    detail = exc.as_dict()
    assert detail["code"] == "capability_unavailable"
    assert detail["switch"] == "cma_organization_enabled"
    assert detail["capability"] == "text"


def test_a_client_is_built_with_the_switch_off() -> None:
    client, _ = make_client(created)
    assert client.cma_organization_enabled is False


# --------------------------------------------------- (a) organization publishing


async def test_organization_publishing_is_refused_by_default_with_the_switch_named(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created)
    with pytest.raises(CmaOrganizationDisabled) as caught:
        await client.publish(binding(**CMA), credentials, org_text(), publication_id="p1", now=NOW)  # type: ignore[arg-type]
    assert "cma_organization_enabled" in caught.value.message
    assert caught.value.switch == "cma_organization_enabled"
    assert caught.value.capability == "text"
    assert log.count == 0
    await client.aclose()

    enabled, wire = make_client(created, cma_organization_enabled=True)
    receipt = await enabled.publish(binding(**CMA), credentials, org_text(), publication_id="p1", now=NOW)  # type: ignore[arg-type]
    assert receipt.post_urn == POST_URN
    assert wire.paths() == ["/rest/posts"]
    await enabled.aclose()


async def test_an_enabled_organization_text_post_matches_the_posts_api_shape(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created, cma_organization_enabled=True)
    await client.publish(binding(**CMA), credentials, org_text(), publication_id="p1", now=NOW)  # type: ignore[arg-type]
    call = log.last()
    body = json.loads(call.body)
    assert body["author"] == ORG
    assert body["visibility"] == "PUBLIC"
    assert body["lifecycleState"] == "PUBLISHED"
    assert body["distribution"] == {
        "feedDistribution": "MAIN_FEED",
        "targetEntities": [],
        "thirdPartyDistributionChannels": [],
    }
    assert body["isReshareDisabledByAuthor"] is False
    assert call.headers["linkedin-version"] == LINKEDIN_VERSION
    assert call.headers["x-restli-protocol-version"] == "2.0.0"
    await client.aclose()


async def test_an_enabled_organization_article_carries_the_documented_article_fields(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created, cma_organization_enabled=True)
    draft = PostDraft(
        author_urn=ORG,
        commentary="Worth a read.",
        media=ArticleDraft(url="https://example.test/a", title="A title", description="A line"),  # type: ignore[arg-type]
    )
    await client.publish(binding(**CMA), credentials, draft, publication_id="p1", now=NOW)  # type: ignore[arg-type]
    article = json.loads(log.last().body)["content"]["article"]
    assert article == {"source": "https://example.test/a", "title": "A title", "description": "A line"}
    await client.aclose()


async def test_the_switch_is_checked_before_the_binding_allowlist(credentials) -> None:  # noqa: ANN001
    """Off, an organization draft is a build refusal; on, the binding decides."""
    not_allowed = binding(adapter="rest_posts", capabilities=("text",))
    client, log = make_client(created)
    with pytest.raises(CmaOrganizationDisabled):
        client.check(not_allowed, org_text())
    enabled, _ = make_client(created, cma_organization_enabled=True)
    with pytest.raises(AuthorForbidden):
        enabled.check(not_allowed, org_text())
    assert log.count == 0
    await client.aclose()
    await enabled.aclose()


async def test_the_switch_does_not_bypass_commissioning_evidence(credentials) -> None:  # noqa: ANN001
    """Necessary, never sufficient: an uncommissioned capability still refuses."""
    client, log = make_client(created, cma_organization_enabled=True)
    with pytest.raises(CapabilityUnavailable) as caught:
        await client.publish(
            binding(adapter="rest_posts", capabilities=(), allowed_orgs=(ORG,)),
            credentials,
            org_text(),
            publication_id="p1",
            now=NOW,
        )
    assert not isinstance(caught.value, CmaOrganizationDisabled)
    assert log.count == 0
    await client.aclose()


async def test_an_organization_post_on_the_share_product_is_refused_too(credentials) -> None:  # noqa: ANN001
    ugc = {"adapter": "share_ugc", "capabilities": ("text",), "allowed_orgs": (ORG,)}
    client, log = make_client(created)
    with pytest.raises(CmaOrganizationDisabled):
        await client.publish(binding(**ugc), credentials, org_text(), publication_id="p1", now=NOW)  # type: ignore[arg-type]
    assert log.count == 0
    await client.aclose()

    enabled, wire = make_client(created, cma_organization_enabled=True)
    await enabled.publish(binding(**ugc), credentials, org_text(), publication_id="p1", now=NOW)  # type: ignore[arg-type]
    assert wire.paths() == ["/v2/ugcPosts"]
    assert json.loads(wire.last().body)["author"] == ORG
    await enabled.aclose()


async def test_a_person_post_never_consults_the_switch(credentials) -> None:  # noqa: ANN001
    client, log = make_client(created)
    assert client.cma_organization_enabled is False
    person = PostDraft(author_urn=PERSON, commentary="Mine.")
    await client.publish(binding(**CMA), credentials, person, publication_id="p1", now=NOW)  # type: ignore[arg-type]
    assert json.loads(log.last().body)["author"] == PERSON
    await client.aclose()


async def test_a_dry_run_prepare_refuses_an_organization_draft() -> None:
    client, log = make_client(created)
    with pytest.raises(CmaOrganizationDisabled):
        await client.prepare(binding(**CMA), org_text())  # type: ignore[arg-type]
    assert log.count == 0
    await client.aclose()


async def test_the_tick_counts_an_organization_draft_as_skipped_and_names_the_switch() -> None:
    org_record = PublicationRecord(
        publication_id="pub-org",
        key=PublicationKey(
            subject_kind="entra_oid",
            subject_id="subject-1",
            campaign_id="camp-1",
            post_id="post-org",
            account_id="acct-william",
        ),
        binding_id="bind-1",
        payload_digest=DIGEST,
        draft=org_text(),
        scheduled_at=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(hours=23),
    )
    _store, client, log, service, bound = await seeded(
        records=[org_record], capabilities=("text",), allowed_orgs=(ORG,)
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert (result.skipped_capability, result.published, log.count) == (1, 0, 0)
    assert result.failures[0]["switch"] == "cma_organization_enabled"
    await client.aclose()


# ------------------------------------------------------------ (b) documents


async def test_an_organization_document_is_refused_by_default_before_any_upload(
    credentials, reader: FakeAssetReader  # noqa: ANN001
) -> None:
    client, log = make_client(documents_api)
    with pytest.raises(CmaOrganizationDisabled) as caught:
        await client.publish(
            binding(**CMA),  # type: ignore[arg-type]
            credentials,
            org_document(),
            publication_id="p1",
            reader=reader,
            subject_id="subject-1",
            now=NOW,
        )
    assert caught.value.capability == "document"
    assert log.count == 0, "no initializeUpload, no PUT, no create"
    await client.aclose()


async def test_an_enabled_organization_document_follows_the_documents_api(
    credentials, reader: FakeAssetReader  # noqa: ANN001
) -> None:
    client, log = make_client(documents_api, cma_organization_enabled=True)
    receipt = await client.publish(
        binding(**CMA),  # type: ignore[arg-type]
        credentials,
        org_document(),
        publication_id="p1",
        reader=reader,
        subject_id="subject-1",
        now=NOW,
    )
    assert receipt.post_urn == POST_URN
    assert log.paths() == ["/rest/documents", "/dms-uploads/D5510AQ", "/rest/posts"]

    initialize, upload, create = log.calls
    assert httpx.URL(initialize.url).params["action"] == "initializeUpload"
    assert json.loads(initialize.body) == {"initializeUploadRequest": {"owner": ORG}}
    assert initialize.headers["linkedin-version"] == LINKEDIN_VERSION
    assert upload.method == "PUT"
    assert "authorization" not in upload.headers, "the pre-signed URL never receives the token"
    body = json.loads(create.body)
    assert body["author"] == ORG
    assert body["content"] == {"media": {"id": DOCUMENT_URN, "title": "Quarterly review"}}
    await client.aclose()


async def test_the_rest_adapter_alone_refuses_an_organization_document_upload(
    credentials, reader: FakeAssetReader  # noqa: ANN001
) -> None:
    """The adapter is exported; it refuses on its own, not only through the client."""
    log: list[str] = []

    def record(request: httpx.Request) -> httpx.Response:
        log.append(request.url.path)
        return documents_api(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as http:
        adapter = RestPostsAdapter(http, api_base=API)
        from linkedin_publish.media import ResolvedAsset

        asset = ResolvedAsset(sha256=PDF_SHA, size=73, media_type="application/pdf")
        with pytest.raises(CmaOrganizationDisabled):
            await adapter.upload_document(credentials, ORG, reader, "subject-1", asset)
        with pytest.raises(CmaOrganizationDisabled):
            await adapter.create_post(credentials, org_document(), DOCUMENT_URN)
        assert log == []

        enabled = RestPostsAdapter(http, api_base=API, cma_organization_enabled=True)
        uploaded = await enabled.upload_document(credentials, ORG, reader, "subject-1", asset)
        assert uploaded.urn == DOCUMENT_URN
        assert log == ["/rest/documents", "/dms-uploads/D5510AQ"]


async def test_the_ugc_adapter_alone_refuses_an_organization_author(credentials) -> None:  # noqa: ANN001
    log: list[str] = []

    def record(request: httpx.Request) -> httpx.Response:
        log.append(request.url.path)
        return created(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as http:
        with pytest.raises(CmaOrganizationDisabled):
            await ShareUgcAdapter(http, api_base=API).create_post(credentials, org_text(), None)
        assert log == []
        await ShareUgcAdapter(http, api_base=API, cma_organization_enabled=True).create_post(
            credentials, org_text(), None
        )
        assert log == ["/v2/ugcPosts"]


# ------------------------------------------------------- (c) share statistics


async def test_share_statistics_are_refused_by_default_even_when_commissioned(credentials) -> None:  # noqa: ANN001
    client, log = make_client(statistics_api)
    with pytest.raises(CmaOrganizationDisabled) as caught:
        await client.share_statistics(
            binding(**CMA),  # type: ignore[arg-type]
            credentials,
            ORG,
            start=NOW - timedelta(days=7),
            end=NOW,
            now=NOW,
        )
    assert caught.value.capability == "share_statistics"
    assert log.count == 0
    await client.aclose()


async def test_enabled_share_statistics_send_the_documented_query(credentials) -> None:  # noqa: ANN001
    client, log = make_client(statistics_api, cma_organization_enabled=True)
    stats = await client.share_statistics(
        binding(**CMA),  # type: ignore[arg-type]
        credentials,
        ORG,
        start=NOW - timedelta(days=7),
        end=NOW,
        granularity="DAY",
        now=NOW,
    )
    call = log.last()
    url = httpx.URL(call.url)
    assert url.path == "/rest/organizationalEntityShareStatistics"
    assert url.params["q"] == "organizationalEntity"
    assert url.params["organizationalEntity"] == ORG
    start_ms = int((NOW - timedelta(days=7)).timestamp() * 1000)
    end_ms = int(NOW.timestamp() * 1000)
    assert url.params["timeIntervals"] == (
        f"(timeRange:(start:{start_ms},end:{end_ms}),timeGranularityType:DAY)"
    )
    assert call.headers["linkedin-version"] == LINKEDIN_VERSION
    point = stats.points[0]
    assert (point.impressions, point.unique_impressions, point.clicks, point.likes) == (140, 90, 4, 2)
    await client.aclose()


async def test_the_public_statistics_function_refuses_on_its_own() -> None:
    calls: list[str] = []

    def record(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return statistics_api(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as http:
        with pytest.raises(CmaOrganizationDisabled):
            await fetch_share_statistics(
                http, "test-token", ORG, start=NOW - timedelta(days=7), end=NOW, api_base=API, now=NOW
            )
        assert calls == []
        stats = await fetch_share_statistics(
            http,
            "test-token",
            ORG,
            start=NOW - timedelta(days=7),
            end=NOW,
            api_base=API,
            now=NOW,
            cma_organization_enabled=True,
        )
        assert calls == ["/rest/organizationalEntityShareStatistics"]
        assert stats.points
