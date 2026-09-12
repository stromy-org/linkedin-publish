"""Reads, deletes and analytics — the capability-gated surfaces.

None of these are enabled by publishing. Each has its own commissioning
evidence, and each refuses before touching the network when it does not have it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import httpx
import pytest

from linkedin_publish import (
    InMemoryPublicationStore,
    PostDraft,
    PublicationKey,
    PublicationRecord,
    PublicationService,
    StaticCredentialProvider,
)
from linkedin_publish.analytics import MAX_QUERY_DAYS, build_query
from linkedin_publish.errors import (
    AuthorForbidden,
    CapabilityUnavailable,
    ProviderRejected,
    PublishOutcomeUnknown,
    TransientReadFailure,
    ValidationFailure,
)
from tests.conftest import NOW, ORG, PERSON, binding, make_client

pytestmark = pytest.mark.contract

POST_URN = "urn:li:share:7100000000000000000"


# -------------------------------------------------------------------- reads ---


async def test_a_read_is_refused_before_the_network_without_the_capability(credentials) -> None:  # noqa: ANN001
    """The pilot's w_member_social is write-only, so this is the usual answer."""
    client, log = make_client(lambda _r: httpx.Response(200, json={}))
    with pytest.raises(CapabilityUnavailable) as caught:
        await client.get_post(binding(), credentials, POST_URN)
    assert caught.value.capability == "read_post"
    assert log.count == 0
    await client.aclose()


async def test_a_granted_read_returns_the_provider_snapshot(credentials) -> None:  # noqa: ANN001
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "author": PERSON,
                "lifecycleState": "PUBLISHED",
                "specificContent": {
                    "com.linkedin.ugc.ShareContent": {"shareCommentary": {"text": "hello"}}
                },
                "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
            },
        )

    client, log = make_client(handler)
    snapshot = await client.get_post(
        binding(capabilities=("text", "read_post")), credentials, POST_URN
    )
    assert snapshot.commentary == "hello"
    assert snapshot.author_urn == PERSON
    assert snapshot.visibility == "PUBLIC"
    assert unquote(log.paths()[0]).endswith(POST_URN)
    await client.aclose()


async def test_a_forbidden_read_is_a_permission_answer_not_an_empty_result(credentials) -> None:  # noqa: ANN001
    """403 means "you may not read", which is not "there is nothing there"."""
    client, _log = make_client(lambda _r: httpx.Response(403, json={"serviceErrorCode": "ACCESS_DENIED"}))
    with pytest.raises(AuthorForbidden):
        await client.get_post(binding(capabilities=("text", "read_post")), credentials, POST_URN)
    await client.aclose()


async def test_a_read_5xx_is_a_bounded_retry_not_an_unknown(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(503, json={}))
    with pytest.raises(TransientReadFailure) as caught:
        await client.get_post(binding(capabilities=("text", "read_post")), credentials, POST_URN)
    assert caught.value.retryable is True
    await client.aclose()


# ------------------------------------------------------------------ deletes ---


async def test_a_delete_is_refused_without_the_capability(credentials) -> None:  # noqa: ANN001
    client, log = make_client(lambda _r: httpx.Response(204))
    with pytest.raises(CapabilityUnavailable):
        await client.delete_post(binding(), credentials, POST_URN)
    assert log.count == 0
    await client.aclose()


async def test_a_successful_delete_reports_the_provider_status(credentials) -> None:  # noqa: ANN001
    client, log = make_client(lambda _r: httpx.Response(204))
    outcome = await client.delete_post(
        binding(capabilities=("text", "delete")), credentials, POST_URN
    )
    assert (outcome.deleted, outcome.existed, outcome.http_status) == (True, True, 204)
    assert log.last().method == "DELETE"
    await client.aclose()


async def test_a_404_is_not_reported_as_a_successful_delete(credentials) -> None:  # noqa: ANN001
    """"You never had access" and "it is gone" are different, and stay different."""
    client, _log = make_client(lambda _r: httpx.Response(404, json={}))
    outcome = await client.delete_post(
        binding(capabilities=("text", "delete")), credentials, POST_URN
    )
    assert outcome.deleted is False
    assert outcome.existed is False
    assert outcome.http_status == 404
    assert outcome.reason is not None
    await client.aclose()


async def test_a_delete_transport_failure_is_unknown(credentials) -> None:  # noqa: ANN001
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lost", request=request)

    client, _log = make_client(handler)
    with pytest.raises(PublishOutcomeUnknown):
        await client.delete_post(binding(capabilities=("text", "delete")), credentials, POST_URN)
    await client.aclose()


async def test_a_delete_target_comes_from_the_ledger_not_from_the_caller(credentials) -> None:  # noqa: ANN001
    """A URN we never recorded is not a publication of ours, so it cannot be deleted."""
    store = InMemoryPublicationStore()
    record = PublicationRecord(
        publication_id="pub-1",
        key=PublicationKey(
            subject_kind="entra_oid",
            subject_id="subject-1",
            campaign_id="camp-1",
            post_id="post-1",
            account_id="acct-william",
        ),
        binding_id="bind-1",
        payload_digest="d" * 64,
        draft=PostDraft(author_urn=PERSON, commentary="hi"),
        scheduled_at=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(hours=23),
    )
    await store.upsert_publication(record)

    client, log = make_client(lambda _r: httpx.Response(204))
    service = PublicationService(store, client, StaticCredentialProvider(credentials))
    bound = binding(capabilities=("text", "delete"))

    # Not published yet: nothing to delete, and no request is made.
    with pytest.raises(ValidationFailure, match="only a published record"):
        await service.delete_publication("pub-1", binding=bound, actor="william")
    assert log.count == 0

    await store.compare_and_set(
        "pub-1",
        expected_state="pending",
        expected_attempt_token=None,
        updates={"state": "claimed", "attempt_token": "t"},
    )
    await store.compare_and_set(
        "pub-1", expected_state="claimed", expected_attempt_token="t", updates={"state": "sending"}
    )
    await store.compare_and_set(
        "pub-1",
        expected_state="sending",
        expected_attempt_token="t",
        updates={"state": "published", "post_urn": POST_URN, "attempt_token": None},
    )

    outcome = await service.delete_publication("pub-1", binding=bound, actor="william")
    assert outcome.deleted is True
    assert unquote(log.last().url).endswith(POST_URN)

    events = [event.kind for event in await store.events("pub-1")]
    assert "deleted" in events
    await client.aclose()


async def test_another_bindings_publication_cannot_be_deleted(credentials) -> None:  # noqa: ANN001
    store = InMemoryPublicationStore()
    await store.upsert_publication(
        PublicationRecord(
            publication_id="pub-1",
            key=PublicationKey(
                subject_kind="entra_oid",
                subject_id="subject-1",
                campaign_id="camp-1",
                post_id="post-1",
                account_id="acct-william",
            ),
            binding_id="bind-1",
            payload_digest="d" * 64,
            draft=PostDraft(author_urn=PERSON, commentary="hi"),
            scheduled_at=NOW,
            expires_at=NOW + timedelta(hours=1),
        )
    )
    client, log = make_client(lambda _r: httpx.Response(204))
    service = PublicationService(store, client, StaticCredentialProvider(credentials))
    other = binding(binding_id="bind-2", capabilities=("text", "delete"))

    with pytest.raises(AuthorForbidden):
        await service.delete_publication("pub-1", binding=other, actor="william")
    assert log.count == 0
    await client.aclose()


# ---------------------------------------------------------------- analytics ---


def test_the_query_encodes_a_millisecond_interval() -> None:
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    end = datetime(2026, 9, 8, tzinfo=timezone.utc)
    query = build_query(ORG, start=start, end=end, granularity="DAY", now=end)
    assert query["q"] == "organizationalEntity"
    assert query["organizationalEntity"] == ORG
    assert f"start:{int(start.timestamp() * 1000)}" in query["timeIntervals"]
    assert f"end:{int(end.timestamp() * 1000)}" in query["timeIntervals"]
    assert "timeGranularityType:DAY" in query["timeIntervals"]


def test_the_query_refuses_an_unbounded_history_scan() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=MAX_QUERY_DAYS + 1)
    with pytest.raises(ValidationFailure, match="caps one request"):
        build_query(ORG, start=start, end=end, now=end)


def test_the_query_refuses_a_window_outside_the_rolling_year() -> None:
    end = datetime(2026, 9, 8, tzinfo=timezone.utc)
    start = end - timedelta(days=400)
    with pytest.raises(ValidationFailure, match="rolling"):
        build_query(ORG, start=start, end=start + timedelta(days=1), now=end)


def test_the_query_refuses_a_naive_window_and_an_inverted_one() -> None:
    aware = datetime(2026, 9, 1, tzinfo=timezone.utc)
    with pytest.raises(ValidationFailure, match="timezone-aware"):
        build_query(ORG, start=datetime(2026, 9, 1), end=aware + timedelta(days=1), now=aware)
    with pytest.raises(ValidationFailure, match="after start"):
        build_query(ORG, start=aware, end=aware, now=aware)


def test_the_query_refuses_a_non_organization_urn() -> None:
    with pytest.raises(Exception, match="organization"):
        build_query(PERSON, start=NOW - timedelta(days=1), end=NOW, now=NOW)


async def test_analytics_needs_its_own_capability(credentials) -> None:  # noqa: ANN001
    """A successful publish says nothing about ADMINISTRATOR role."""
    client, log = make_client(lambda _r: httpx.Response(200, json={"elements": []}))
    with pytest.raises(CapabilityUnavailable) as caught:
        await client.share_statistics(
            binding(adapter="rest_posts", allowed_orgs=(ORG,)),
            credentials,
            ORG,
            start=NOW - timedelta(days=7),
            end=NOW,
            now=NOW,
        )
    assert caught.value.capability == "share_statistics"
    assert log.count == 0
    await client.aclose()


async def test_analytics_refuses_an_organization_outside_the_binding(credentials) -> None:  # noqa: ANN001
    """A token that happens to reach another Page is not authorization to report on it."""
    client, log = make_client(lambda _r: httpx.Response(200, json={"elements": []}))
    with pytest.raises(AuthorForbidden):
        await client.share_statistics(
            binding(adapter="rest_posts", capabilities=("text", "share_statistics")),
            credentials,
            "urn:li:organization:9999",
            start=NOW - timedelta(days=7),
            end=NOW,
            now=NOW,
        )
    assert log.count == 0
    await client.aclose()


async def test_analytics_is_unavailable_on_the_share_product(credentials) -> None:  # noqa: ANN001
    client, log = make_client(lambda _r: httpx.Response(200, json={"elements": []}))
    with pytest.raises(CapabilityUnavailable, match="versioned-REST"):
        await client.share_statistics(
            binding(capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
            credentials,
            ORG,
            start=NOW - timedelta(days=7),
            end=NOW,
            now=NOW,
        )
    assert log.count == 0
    await client.aclose()


async def test_a_measured_zero_and_a_negative_count_both_survive(credentials) -> None:  # noqa: ANN001
    """Clamping a retraction to zero silently inflates the report."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "elements": [
                    {
                        "timeRange": {"start": 1757548800000, "end": 1757635200000},
                        "totalShareStatistics": {
                            "impressionCount": 0,
                            "uniqueImpressionsCount": 0,
                            "clickCount": -3,
                            "likeCount": 5,
                            "engagement": 0.0,
                        },
                    }
                ]
            },
        )

    client, _log = make_client(handler)
    stats = await client.share_statistics(
        binding(adapter="rest_posts", capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
        credentials,
        ORG,
        start=NOW - timedelta(days=7),
        end=NOW,
        now=NOW,
    )
    point = stats.points[0]
    assert point.impressions == 0
    assert point.clicks == -3
    assert point.engagement == 0.0
    assert point.reason is None
    await client.aclose()


async def test_an_absent_metric_is_none_with_a_reason_not_zero(credentials) -> None:  # noqa: ANN001
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "elements": [
                    {
                        "timeRange": {"start": 1757548800000, "end": 1757635200000},
                        "totalShareStatistics": {},
                    }
                ]
            },
        )

    client, _log = make_client(handler)
    stats = await client.share_statistics(
        binding(adapter="rest_posts", capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
        credentials,
        ORG,
        start=NOW - timedelta(days=7),
        end=NOW,
        now=NOW,
    )
    point = stats.points[0]
    assert point.impressions is None
    assert point.clicks is None
    assert point.reason is not None
    await client.aclose()


async def test_an_empty_window_carries_its_provenance_and_a_reason(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(200, json={"elements": []}))
    stats = await client.share_statistics(
        binding(adapter="rest_posts", capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
        credentials,
        ORG,
        start=NOW - timedelta(days=7),
        end=NOW,
        now=NOW,
    )
    assert stats.points == ()
    assert stats.reason is not None
    assert stats.source == "organizationalEntityShareStatistics"
    assert stats.organization_urn == ORG
    assert stats.observed_at == NOW
    await client.aclose()


async def test_a_missing_role_surfaces_as_a_permission_failure(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(403, json={"serviceErrorCode": "ACCESS_DENIED"}))
    with pytest.raises(AuthorForbidden):
        await client.share_statistics(
            binding(adapter="rest_posts", capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
            credentials,
            ORG,
            start=NOW - timedelta(days=7),
            end=NOW,
            now=NOW,
        )
    await client.aclose()


async def test_a_malformed_analytics_body_is_a_reason_not_a_crash(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(lambda _r: httpx.Response(200, json={"unexpected": True}))
    stats = await client.share_statistics(
        binding(adapter="rest_posts", capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
        credentials,
        ORG,
        start=NOW - timedelta(days=7),
        end=NOW,
        now=NOW,
    )
    assert stats.points == ()
    assert stats.reason == "provider returned no elements array"
    await client.aclose()


async def test_a_rejected_analytics_read_never_leaks_the_body(credentials) -> None:  # noqa: ANN001
    client, _log = make_client(
        lambda _r: httpx.Response(400, json={"message": "internal org id 7788 not permitted"})
    )
    with pytest.raises(ProviderRejected) as caught:
        await client.share_statistics(
            binding(adapter="rest_posts", capabilities=("text", "share_statistics"), allowed_orgs=(ORG,)),
            credentials,
            ORG,
            start=NOW - timedelta(days=7),
            end=NOW,
            now=NOW,
        )
    assert "7788" not in str(caught.value)
    await client.aclose()
