"""The durable publication service: approval, state machine, and no replay.

These are the assertions the whole design exists for. Several of them are
*counting* tests — how many requests actually left — because "it refused" and
"it refused after posting" look identical from a return value.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import httpx
import pytest

from linkedin_publish import (
    ApprovalRecord,
    BindingLoader,
    CommissioningGrant,
    InMemoryBudgetStore,
    InMemoryPublicationStore,
    PostDraft,
    PublicationKey,
    PublicationRecord,
    PublicationService,
    QuotaLimiter,
    StaticCredentialProvider,
    StoreConflict,
)
from linkedin_publish.errors import (
    AuthorForbidden,
    CapabilityUnavailable,
    PublishOutcomeUnknown,
    QuotaDeferred,
    ValidationFailure,
)
from linkedin_publish.limits import QuotaProfile
from tests.conftest import NOW, PERSON, Recorder, binding, make_client

pytestmark = pytest.mark.contract

POST_URN = "urn:li:share:7100000000000000000"
DIGEST = "d" * 64


def created(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(201, json={}, headers={"x-restli-id": POST_URN})


def record(
    *,
    publication_id: str = "pub-1",
    digest: str = DIGEST,
    post_id: str = "post-1",
    state: str = "pending",
    scheduled_at: datetime | None = None,
    expires_at: datetime | None = None,
    commentary: str = "Intelligence, orchestrated.",
) -> PublicationRecord:
    return PublicationRecord(
        publication_id=publication_id,
        key=PublicationKey(
            subject_kind="entra_oid",
            subject_id="subject-1",
            campaign_id="camp-1",
            post_id=post_id,
            account_id="acct-william",
        ),
        binding_id="bind-1",
        payload_digest=digest,
        draft=PostDraft(author_urn=PERSON, commentary=commentary),
        state=state,  # type: ignore[arg-type]
        scheduled_at=scheduled_at or (NOW - timedelta(minutes=5)),
        expires_at=expires_at or (NOW + timedelta(hours=23)),
    )


def approval(publication_id: str = "pub-1", *, digest: str = DIGEST, revoked: bool = False) -> ApprovalRecord:
    return ApprovalRecord(
        approval_id="appr-1",
        publication_id=publication_id,
        payload_digest=digest,
        binding_id="bind-1",
        approved_by="william@stromy",
        approved_at=NOW - timedelta(hours=1),
        revoked_at=(NOW - timedelta(minutes=1)) if revoked else None,
    )


async def seeded(
    handler=created,  # noqa: ANN001
    *,
    approved: bool = True,
    records: list[PublicationRecord] | None = None,
    **binding_kwargs: object,
):  # noqa: ANN201
    store = InMemoryPublicationStore()
    for row in records or [record()]:
        await store.upsert_publication(row)
        if approved:
            await store.put_approval(approval(row.publication_id, digest=row.payload_digest))
    client, log = make_client(handler)
    service = PublicationService(
        store,
        client,
        StaticCredentialProvider(
            __import__("linkedin_publish").Credentials(
                access_token="t", client_id="app-personal", client_secret="s", credential_version="v1"
            )
        ),
    )
    return store, client, log, service, binding(**binding_kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------- dry run ---


async def test_a_dry_run_makes_zero_requests_and_mutates_nothing() -> None:
    store, client, log, service, bound = await seeded()
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=True)

    assert log.count == 0
    assert result.due == 1
    assert result.published == 0
    assert result.dry_run is True
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "pending"
    assert stored.attempt_token is None
    assert stored.attempts == 0
    await client.aclose()


async def test_no_due_records_is_no_work_not_a_failure() -> None:
    _store, client, log, service, bound = await seeded(
        records=[record(scheduled_at=NOW + timedelta(days=1), expires_at=NOW + timedelta(days=2))]
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert result.due == 0
    assert result.outcome == "no-work"
    assert log.count == 0
    await client.aclose()


# --------------------------------------------------------------- approval ---


async def test_an_approved_due_record_publishes_exactly_once() -> None:
    store, client, log, service, bound = await seeded()
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)

    assert result.published == 1
    assert log.count == 1
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "published"
    assert stored.post_urn == POST_URN
    await client.aclose()


async def test_a_missing_approval_produces_zero_requests() -> None:
    store, client, log, service, bound = await seeded(approved=False)
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)

    assert log.count == 0
    assert result.published == 0
    stored = await store.get_publication("pub-1")
    assert stored is not None
    # Returned to pending — it was never sent, so it is still eligible if approved.
    assert stored.state == "pending"
    await client.aclose()


async def test_a_revoked_approval_produces_zero_requests() -> None:
    store, client, log, service, bound = await seeded()
    await store.put_approval(approval(revoked=True))

    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert log.count == 0
    assert result.published == 0
    await client.aclose()


async def test_a_mutated_payload_has_no_approval_and_makes_zero_requests() -> None:
    """The approval is over a digest. Change the payload, the approval is gone."""
    store, client, log, service, bound = await seeded()
    stored = await store.get_publication("pub-1")
    assert stored is not None

    with pytest.raises(ValidationFailure, match="does not match the expected digest"):
        await service.publish_one("pub-1", bound, expected_digest="0" * 64, now=NOW)
    assert log.count == 0
    await client.aclose()


async def test_an_approval_for_another_binding_is_refused() -> None:
    store, client, log, service, bound = await seeded()
    await store.put_approval(
        approval().model_copy(update={"binding_id": "bind-other"})
    )
    with pytest.raises(AuthorForbidden, match="different binding"):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    await client.aclose()


# ------------------------------------------------------------ kill switch ---


async def test_publishing_disabled_blocks_the_send_with_zero_requests() -> None:
    _store, client, log, service, bound = await seeded(publish_enabled=False)
    with pytest.raises(CapabilityUnavailable, match="publishing is disabled"):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    await client.aclose()


async def test_a_commissioning_grant_permits_exactly_one_send_while_disabled() -> None:
    """The C1 canary: one approved publication out, publishing still off."""
    store, client, log, service, bound = await seeded(publish_enabled=False)
    await store.put_grant(
        CommissioningGrant(
            grant_id="grant-1",
            publication_id="pub-1",
            payload_digest=DIGEST,
            binding_id="bind-1",
            capability="text",
            issued_by="william@stromy",
            issued_at=NOW - timedelta(minutes=1),
            expires_at=NOW + timedelta(minutes=29),
        )
    )

    receipt = await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert receipt.post_urn == POST_URN
    assert log.count == 1

    grant = await store.get_grant("pub-1")
    assert grant is not None
    assert grant.consumed_at is not None
    assert bound.publish_enabled is False
    await client.aclose()


async def test_a_consumed_grant_cannot_license_a_second_send() -> None:
    store, client, log, service, bound = await seeded(
        publish_enabled=False,
        records=[record(), record(publication_id="pub-2", post_id="post-2")],
    )
    for publication_id in ("pub-1", "pub-2"):
        await store.put_grant(
            CommissioningGrant(
                grant_id=f"grant-{publication_id}",
                publication_id=publication_id,
                payload_digest=DIGEST,
                binding_id="bind-1",
                capability="text",
                issued_by="william@stromy",
                issued_at=NOW - timedelta(minutes=1),
                expires_at=NOW + timedelta(minutes=29),
            )
        )
    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1

    # pub-1's grant is spent; it cannot be replayed, and pub-2's is its own.
    with pytest.raises(ValidationFailure, match="already published"):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1
    await client.aclose()


async def test_an_expired_grant_does_not_license_a_send() -> None:
    store, client, log, service, bound = await seeded(publish_enabled=False)
    await store.put_grant(
        CommissioningGrant(
            grant_id="grant-1",
            publication_id="pub-1",
            payload_digest=DIGEST,
            binding_id="bind-1",
            capability="text",
            issued_by="william@stromy",
            issued_at=NOW - timedelta(hours=2),
            expires_at=NOW - timedelta(minutes=1),
        )
    )
    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    await client.aclose()


async def test_a_grant_for_another_digest_does_not_license_a_send() -> None:
    store, client, log, service, bound = await seeded(publish_enabled=False)
    await store.put_grant(
        CommissioningGrant(
            grant_id="grant-1",
            publication_id="pub-1",
            payload_digest="9" * 64,
            binding_id="bind-1",
            capability="text",
            issued_by="william@stromy",
            issued_at=NOW,
            expires_at=NOW + timedelta(minutes=29),
        )
    )
    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    await client.aclose()


# ----------------------------------------------------------- the hard case ---


async def test_accepted_then_lost_is_unknown_with_exactly_one_remote_acceptance() -> None:
    """LinkedIn takes the post; the connection dies before we see the receipt.

    The record must end `unknown` and must never be sent again — not by a retry,
    not by the next tick, not by a new process.
    """
    accepted = Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        # The remote side has now created the post.
        accepted.calls.append(request)  # type: ignore[arg-type]
        raise httpx.ReadTimeout("connection lost after acceptance", request=request)

    store, client, _log, service, bound = await seeded(handler)

    with pytest.raises(PublishOutcomeUnknown):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)

    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "unknown"
    assert stored.failure_code == "publish_outcome_unknown"

    # Two further ticks, a fresh service object standing in for a new process.
    for _ in range(2):
        fresh = PublicationService(
            store,
            client,
            StaticCredentialProvider(
                __import__("linkedin_publish").Credentials(
                    access_token="t", client_id="app-personal", client_secret="s", credential_version="v1"
                )
            ),
        )
        result = await fresh.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
        assert result.published == 0

    assert len(accepted.calls) == 1, "the post was sent more than once"
    await client.aclose()


async def test_a_published_record_is_never_replayed() -> None:
    store, client, log, service, bound = await seeded()
    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1

    with pytest.raises(ValidationFailure, match="already published"):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert result.due == 0
    assert log.count == 1
    await client.aclose()


async def test_reimporting_the_same_key_with_a_new_payload_is_a_conflict() -> None:
    """Same key, different digest: a conflict, never a second post."""
    store, client, _log, _service, _bound = await seeded()
    with pytest.raises(StoreConflict, match="different payload digest"):
        await store.upsert_publication(record(publication_id="pub-2", digest="9" * 64))
    await client.aclose()


async def test_reimporting_an_identical_payload_is_idempotent() -> None:
    store, client, _log, _service, _bound = await seeded()
    again = await store.upsert_publication(record(publication_id="pub-2"))
    assert again.publication_id == "pub-1"
    await client.aclose()


# --------------------------------------------------------------- lifecycle ---


async def test_an_expired_window_expires_rather_than_publishing_late() -> None:
    store, client, log, service, bound = await seeded(
        records=[record(scheduled_at=NOW - timedelta(days=2), expires_at=NOW - timedelta(days=1))]
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert result.expired == 1
    assert log.count == 0
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "expired"
    await client.aclose()


async def test_a_late_but_unexpired_record_still_publishes() -> None:
    store, client, log, service, bound = await seeded(
        records=[record(scheduled_at=NOW - timedelta(hours=20), expires_at=NOW + timedelta(hours=4))]
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert result.published == 1
    assert result.oldest_overdue_seconds == 20 * 3600
    await client.aclose()


async def test_a_rate_limit_defers_without_losing_the_record() -> None:
    store, client, log, service, bound = await seeded(
        lambda _r: httpx.Response(429, json={}, headers={"retry-after": "600"})
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)

    assert result.deferred == 1
    assert log.count == 1
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "pending"
    assert stored.not_before is not None
    assert stored.not_before > NOW
    await client.aclose()


async def test_a_deferred_record_is_not_due_until_its_not_before() -> None:
    store, client, _log, service, bound = await seeded(
        lambda _r: httpx.Response(429, json={}, headers={"retry-after": "600"})
    )
    await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    still_early = await store.find_due(
        binding_id="bind-1", campaign_id="camp-1", now=NOW + timedelta(minutes=5), limit=10
    )
    assert still_early == []
    later = await store.find_due(
        binding_id="bind-1", campaign_id="camp-1", now=NOW + timedelta(minutes=11), limit=10
    )
    assert len(later) == 1
    await client.aclose()


async def test_a_definite_rejection_fails_permanently() -> None:
    store, client, log, service, bound = await seeded(
        lambda _r: httpx.Response(422, json={"serviceErrorCode": 100})
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert result.failed == 1
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "failed"
    assert log.count == 1
    await client.aclose()


# ------------------------------------------------------------- reconcile ----


async def test_an_unknown_record_is_reconciled_to_published_by_an_operator() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lost", request=request)

    store, client, _log, service, bound = await seeded(handler)
    with pytest.raises(PublishOutcomeUnknown):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)

    reconciled = await service.reconcile("pub-1", actor="william@stromy", post_urn=POST_URN, now=NOW)
    assert reconciled.state == "published"
    assert reconciled.post_urn == POST_URN
    await client.aclose()


async def test_an_unknown_record_can_be_dispositioned_as_failed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lost", request=request)

    store, client, _log, service, bound = await seeded(handler)
    with pytest.raises(PublishOutcomeUnknown):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)

    reconciled = await service.reconcile(
        "pub-1", actor="william@stromy", failure_reason="not visible on the profile", now=NOW
    )
    assert reconciled.state == "failed"
    await client.aclose()


async def test_reconcile_requires_exactly_one_disposition() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lost", request=request)

    _store, client, _log, service, bound = await seeded(handler)
    with pytest.raises(PublishOutcomeUnknown):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    with pytest.raises(ValidationFailure, match="exactly one"):
        await service.reconcile("pub-1", actor="w", post_urn=POST_URN, failure_reason="both")
    await client.aclose()


async def test_only_unknown_records_are_reconciled() -> None:
    _store, client, _log, service, bound = await seeded()
    with pytest.raises(ValidationFailure, match="only unknown records"):
        await service.reconcile("pub-1", actor="w", post_urn=POST_URN)
    await client.aclose()


# ---------------------------------------------------------- cancellation ----


async def test_a_pending_record_cancels_cleanly() -> None:
    store, client, _log, service, bound = await seeded()
    assert await service.cancel("pub-1", actor="william@stromy") == "cancelled"
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "cancelled"
    await client.aclose()


async def test_cancelling_a_published_record_is_too_late_not_a_promise() -> None:
    _store, client, _log, service, bound = await seeded()
    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert await service.cancel("pub-1", actor="william@stromy") == "too_late"
    await client.aclose()


async def test_cancelling_an_unknown_record_reports_outcome_pending() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lost", request=request)

    _store, client, _log, service, bound = await seeded(handler)
    with pytest.raises(PublishOutcomeUnknown):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert await service.cancel("pub-1", actor="w") == "outcome_pending"
    await client.aclose()


# --------------------------------------------------------- lease recovery ---


async def test_a_dead_claim_returns_to_pending() -> None:
    """`claimed` means no send began, so it is safe to re-offer."""
    store, client, _log, service, bound = await seeded()
    await store.compare_and_set(
        "pub-1",
        expected_state="pending",
        expected_attempt_token=None,
        updates={"state": "claimed", "attempt_token": "dead", "lease_deadline": NOW - timedelta(minutes=1)},
    )
    await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=True)
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "pending"
    await client.aclose()


async def test_a_dead_send_becomes_unknown_never_pending() -> None:
    """This asymmetry is the design. `pending` would be a licence to repost."""
    store, client, log, service, bound = await seeded()
    await store.compare_and_set(
        "pub-1",
        expected_state="pending",
        expected_attempt_token=None,
        updates={"state": "claimed", "attempt_token": "t", "lease_deadline": NOW + timedelta(minutes=5)},
    )
    await store.compare_and_set(
        "pub-1",
        expected_state="claimed",
        expected_attempt_token="t",
        updates={"state": "sending", "lease_deadline": NOW - timedelta(minutes=1)},
    )
    await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    stored = await store.get_publication("pub-1")
    assert stored is not None
    assert stored.state == "unknown"
    assert log.count == 0
    await client.aclose()


async def test_an_illegal_transition_is_refused_by_the_store() -> None:
    store, client, _log, _service, _bound = await seeded()
    with pytest.raises(StoreConflict, match="illegal transition"):
        await store.compare_and_set(
            "pub-1", expected_state="pending", expected_attempt_token=None, updates={"state": "published"}
        )
    await client.aclose()


async def test_a_second_worker_loses_the_claim_race() -> None:
    store, client, _log, _service, _bound = await seeded()
    await store.compare_and_set(
        "pub-1",
        expected_state="pending",
        expected_attempt_token=None,
        updates={"state": "claimed", "attempt_token": "worker-a"},
    )
    with pytest.raises(StoreConflict, match="expected pending"):
        await store.compare_and_set(
            "pub-1",
            expected_state="pending",
            expected_attempt_token=None,
            updates={"state": "claimed", "attempt_token": "worker-b"},
        )
    await client.aclose()


# ------------------------------------------------------------- capability ---


async def test_an_uncommissioned_capability_skips_the_batch_with_zero_requests() -> None:
    store, client, log, service, bound = await seeded(
        records=[
            PublicationRecord(
                publication_id="pub-img",
                key=PublicationKey(
                    subject_kind="entra_oid",
                    subject_id="subject-1",
                    campaign_id="camp-1",
                    post_id="post-img",
                    account_id="acct-william",
                ),
                binding_id="bind-1",
                payload_digest=DIGEST,
                draft=PostDraft.model_validate(
                    {
                        "author_urn": PERSON,
                        "commentary": "chart",
                        "media": {"kind": "image", "asset": {"sha256": "a" * 64}},
                    }
                ),
                scheduled_at=NOW - timedelta(minutes=5),
                expires_at=NOW + timedelta(hours=23),
            )
        ],
        capabilities=("text",),
    )
    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert result.skipped_capability == 1
    assert result.published == 0
    assert log.count == 0
    await client.aclose()


# ------------------------------------------------------------ authorization ---


def test_a_binding_is_not_served_to_another_subject() -> None:
    """Cross-subject access fails even where the token would have write access."""
    loader = BindingLoader([binding()])
    assert loader.load("bind-1", subject_kind="entra_oid", subject_id="subject-1").binding_id == "bind-1"
    with pytest.raises(AuthorForbidden):
        loader.load("bind-1", subject_kind="entra_oid", subject_id="someone-else")
    with pytest.raises(AuthorForbidden):
        loader.load("bind-1", subject_kind="client_slug", subject_id="subject-1")


def test_an_unknown_binding_is_indistinguishable_from_a_forbidden_one() -> None:
    """Binding existence is not something an unauthorized caller learns."""
    loader = BindingLoader([binding()])
    with pytest.raises(AuthorForbidden) as missing:
        loader.load("bind-nope", subject_kind="entra_oid", subject_id="subject-1")
    with pytest.raises(AuthorForbidden) as forbidden:
        loader.load("bind-1", subject_kind="entra_oid", subject_id="other")
    assert str(missing.value).replace("bind-nope", "X") == str(forbidden.value).replace("bind-1", "X")


# ----------------------------------------------------------------- quotas ---


async def test_an_exhausted_quota_defers_before_the_network() -> None:
    store = InMemoryPublicationStore()
    await store.upsert_publication(record())
    await store.put_approval(approval())

    budget = InMemoryBudgetStore()
    limiter = QuotaLimiter(budget, QuotaProfile(name="t", app_daily=0, member_daily=10, per_endpoint={}))
    client, log = make_client(created, limiter=limiter)
    service = PublicationService(
        store,
        client,
        StaticCredentialProvider(
            __import__("linkedin_publish").Credentials(
                access_token="t", client_id="app-personal", client_secret="s", credential_version="v1"
            )
        ),
    )
    with pytest.raises(QuotaDeferred):
        await service.publish_one("pub-1", binding(), expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    await client.aclose()
