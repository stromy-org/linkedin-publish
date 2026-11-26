"""Meta adapter wire fixtures: every Graph operation the adapter exercises, offline.

Hand-built fixtures against `tests.social_fakes.FakeGraph` (an `httpx.MockTransport`).
No network, no credential. The counted assertions are the point: one create per
approval, zero writes on a read-only path, and no publish after a lost response.
"""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest

from social_publish import DispatchService, InMemorySocialStore, OutcomeUnknown, Refused
from tests.social_fakes import (
    IG,
    IMAGE,
    NOW,
    PAGE,
    SIGNED,
    TOKEN,
    FakeGraph,
    adapter,
    approval_for,
    dumps,
    leaked,
    meta_binding,
    meta_payload,
    record_for,
)

pytestmark = [pytest.mark.contract]

FB_TEXT = (("schedule_post", "text"), ("schedule_post", "link"), ("schedule_post", "feed_image"))
IG_IMAGE = (("publish_post", "feed_image"),)
REPLY = (("reply", "comment_reply"),)


async def setup(graph: FakeGraph, binding, payload, **kwargs):  # noqa: ANN001, ANN201
    store = InMemorySocialStore()
    record = record_for(binding, payload, **kwargs)
    await store.import_intent(record, approval_for(record), actor="test", at=NOW)
    service = DispatchService(store, {"meta_graph": adapter(graph)})
    return store, service, record


# ---------------------------------------------------------------- Facebook


async def test_a_facebook_text_schedule_is_created_once_and_read_back() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())

    tick = await service.tick(binding, now=NOW, dry_run=False)

    assert tick.scheduled == 1 and tick.sent == 1
    stored = await store.get(record.publication_id)
    assert stored is not None and stored.state == "scheduled"
    assert stored.remote_id in graph.scheduled
    assert [c.path for c in graph.writes()] == [f"{PAGE}/feed"]
    create = graph.writes()[0]
    assert create.body["published"] == "false"
    assert create.body["scheduled_publish_time"] == str(int((NOW + timedelta(hours=2)).timestamp()))
    assert create.authorization == f"Bearer {TOKEN}"
    assert all(TOKEN not in c.path and "access_token" not in c.body for c in graph.calls)

    # A second tick only reads back; the schedule is never fresh work.
    again = await service.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)
    assert again.sent == 0 and len(graph.writes()) == 1


async def test_a_scheduled_post_reads_back_as_published_after_its_slot() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())
    await service.tick(binding, now=NOW, dry_run=False)
    stored = await store.get(record.publication_id)
    assert stored is not None and stored.remote_id is not None
    graph.go_live(stored.remote_id)

    tick = await service.tick(binding, now=NOW + timedelta(hours=3), dry_run=False)

    after = await store.get(record.publication_id)
    assert tick.published == 1 and after is not None and after.state == "published"
    assert len(graph.writes()) == 1


async def test_a_readback_that_differs_is_withdrawn_and_read_back_absent() -> None:
    graph = FakeGraph(readback_message_override="something else")
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())

    tick = await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert tick.cancelled == 1 and stored is not None and stored.state == "cancelled"
    assert stored.failure_code == "readback_mismatch"
    assert [c.method for c in graph.writes()] == ["POST", "DELETE"]
    assert graph.scheduled == {}


async def test_cancel_deletes_once_and_verifies_absence() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())
    await service.tick(binding, now=NOW, dry_run=False)

    assert await service.cancel(binding, record.publication_id, now=NOW) == "cancelled"
    stored = await store.get(record.publication_id)
    assert stored is not None and stored.state == "cancelled"
    assert [c.method for c in graph.writes()] == ["POST", "DELETE"]


async def test_a_refused_cancel_stays_scheduled_and_visible() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())
    await service.tick(binding, now=NOW, dry_run=False)
    graph.faults["delete"] = [FakeGraph.error(200, status=403)]

    assert await service.cancel(binding, record.publication_id, now=NOW) == "cancel_failed"
    stored = await store.get(record.publication_id)
    assert stored is not None and stored.state == "scheduled" and stored.failure_code == "cancel_failed"


async def test_a_photo_schedule_sends_a_signed_url_in_the_body_only() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload(fmt="feed_image", media=(IMAGE,)))

    await service.tick(binding, now=NOW, dry_run=False)

    (create,) = graph.writes()
    assert create.path == f"{PAGE}/photos" and create.body["url"] == SIGNED
    events = dumps([e.model_dump() for e in await store.events(record.publication_id)])
    stored = await store.get(record.publication_id)
    assert leaked(graph, events, dumps(stored.model_dump() if stored else {})) == []


async def test_a_lost_facebook_create_is_unknown_then_reconciled_from_one_match() -> None:
    graph = FakeGraph()
    graph.faults["feed"] = ["lost"]
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())

    first = await service.tick(binding, now=NOW, dry_run=False)
    assert first.unknown == 1
    stored = await store.get(record.publication_id)
    assert stored is not None and stored.state == "unknown"

    second = await service.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)

    after = await store.get(record.publication_id)
    assert second.reconciled == 1 and after is not None and after.state == "scheduled"
    assert len(graph.writes()) == 1  # never re-sent


async def test_an_unknown_with_two_matches_stays_unknown() -> None:
    graph = FakeGraph()
    graph.faults["feed"] = ["lost"]
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())
    await service.tick(binding, now=NOW, dry_run=False)
    # Someone scheduled the same text by hand: two candidates, no certainty.
    twin = dict(next(iter(graph.scheduled.values())))
    twin["id"] = f"{PAGE}_twin"
    graph.scheduled[twin["id"]] = twin  # type: ignore[index]

    tick = await service.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)

    stored = await store.get(record.publication_id)
    assert tick.unknown == 1 and stored is not None and stored.state == "unknown"
    assert len(graph.writes()) == 1


async def test_a_rate_limited_create_defers_inside_the_window() -> None:
    graph = FakeGraph()
    graph.faults["feed"] = [FakeGraph.error(80001, status=400)]
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload())

    tick = await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert tick.deferred == 1 and stored is not None and stored.state == "pending"
    assert stored.not_before is not None and stored.not_before <= stored.expires_at
    assert tick.failures[0]["provider_code"] == "80001"
    assert "Belle victoire" not in dumps(tick.failures)  # the echoed body never survives


async def test_a_slot_under_ten_minutes_ahead_is_refused_before_transport() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store, service, record = await setup(graph, binding, meta_payload(slot=NOW + timedelta(minutes=5)))

    tick = await service.tick(binding, now=NOW, dry_run=False)

    assert tick.failures[0]["code"] == "schedule_out_of_bounds"
    assert graph.calls == []


def test_the_adapter_is_pinned_to_the_c3_graph_version() -> None:
    from social_publish.adapters.meta import GRAPH_VERSION, MetaGraphAdapter

    assert GRAPH_VERSION == "v27.0"
    with pytest.raises(ValueError, match="pinned"):
        MetaGraphAdapter(httpx.AsyncClient(), None, version="v24.0")  # type: ignore[arg-type]


# --------------------------------------------------------------- Instagram


async def test_the_container_lifecycle_publishes_once_when_finished() -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    graph.faults["media_create"] = []

    # First status read: still processing past the bounded poll -> claim released, container kept.
    service = DispatchService(store, {"meta_graph": adapter(graph, poll_attempts=2)})
    first = await service.tick(binding, now=NOW, dry_run=False)
    stored = await store.get(record.publication_id)
    assert first.deferred == 1 and stored is not None and stored.state == "pending"
    container = stored.container_id
    assert container is not None and graph.containers[container]["status_code"] == "IN_PROGRESS"

    graph.status_script[container] = ["FINISHED"]
    second = await service.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)

    after = await store.get(record.publication_id)
    assert second.published == 1 and after is not None and after.state == "published"
    assert after.container_id == container  # the same container, never a second one
    paths = [c.path for c in graph.writes()]
    assert paths == [f"{IG}/media", f"{IG}/media_publish"]
    assert graph.writes()[1].body == {"creation_id": container}
    assert graph.writes()[0].body["image_url"] == SIGNED


async def test_the_container_id_is_persisted_before_publish_is_dispatched() -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    seen: dict[str, object] = {}
    original = graph.handler

    def spy(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/media_publish"):
            row = store._records[record.publication_id]  # noqa: SLF001 - observing durability at send time
            seen["state"], seen["container"] = row.state, row.container_id
        return original(request)

    graph.status_script = {}
    graph.handler = spy  # type: ignore[method-assign]
    service = DispatchService(store, {"meta_graph": adapter(graph)})
    # Make the container ready on its first poll.
    first_create = graph._create_container  # noqa: SLF001

    def finished(body: dict[str, str]) -> dict[str, object]:
        out = first_create(body)
        graph.containers[str(out["id"])]["status_code"] = "FINISHED"
        return out

    graph._create_container = finished  # type: ignore[method-assign]  # noqa: SLF001
    await service.tick(binding, now=NOW, dry_run=False)

    assert seen["state"] == "sending" and seen["container"] is not None


@pytest.mark.parametrize(
    ("status", "state", "code"),
    [("ERROR", "failed", "media_processing_failed"), ("PUBLISHED", "unknown", "container_already_published")],
)
async def test_a_container_that_errors_or_shows_published_is_never_published(
    status: str, state: str, code: str
) -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    graph.status_script = {"900000000000001": [status]}

    await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert stored is not None and stored.state == state and stored.failure_code == code
    assert [c.path for c in graph.writes()] == [f"{IG}/media"]


async def test_an_expired_container_is_discarded_and_a_new_one_created_next_tick() -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    graph.status_script = {"900000000000001": ["EXPIRED"]}

    await service.tick(binding, now=NOW, dry_run=False)
    stored = await store.get(record.publication_id)
    assert stored is not None and stored.state == "pending" and stored.container_id is None

    graph.status_script = {"900000000000002": ["FINISHED"]}
    await service.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)
    after = await store.get(record.publication_id)
    assert after is not None and after.state == "published" and after.container_id == "900000000000002"


async def test_a_lost_publish_response_requeries_the_same_container_and_never_replays() -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    graph.status_script = {"900000000000001": ["FINISHED"]}
    graph.faults["media_publish"] = ["lost_before"]  # Meta did NOT publish, and we did not hear

    with_lost = await service.tick(binding, now=NOW, dry_run=False)
    stored = await store.get(record.publication_id)
    assert with_lost.unknown == 1 and stored is not None and stored.state == "unknown"
    assert stored.container_id == "900000000000001"

    # A restart: a fresh adapter and service over the same durable store.
    restarted = DispatchService(store, {"meta_graph": adapter(graph)})
    for minutes in (1, 2, 3):
        tick = await restarted.tick(binding, now=NOW + timedelta(minutes=minutes), dry_run=False)
        assert tick.unknown == 1  # FINISHED but unpublished is unresolved: it stays unknown
    after = await store.get(record.publication_id)
    assert after is not None and after.state == "unknown"
    publishes = [c for c in graph.writes() if c.path.endswith("media_publish")]
    assert len(publishes) == 1  # the one attempt; never replayed
    reads = [c.path for c in graph.calls if c.method == "GET" and c.path == "900000000000001"]
    assert len(reads) >= 4  # the same container was re-queried each time


async def test_a_lost_publish_that_did_publish_settles_from_the_same_container() -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    graph.status_script = {"900000000000001": ["FINISHED"]}
    graph.faults["media_publish"] = ["lost"]  # Meta published; the answer was lost

    await service.tick(binding, now=NOW, dry_run=False)
    restarted = DispatchService(store, {"meta_graph": adapter(graph)})
    tick = await restarted.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)

    after = await store.get(record.publication_id)
    assert tick.reconciled == 1 and after is not None and after.state == "published"
    assert after.remote_id in graph.media
    assert len([c for c in graph.writes() if c.path.endswith("media_publish")]) == 1


async def test_an_exhausted_publishing_quota_defers_without_publishing() -> None:
    graph = FakeGraph(quota=(50, 50))
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph, binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
    )
    graph.status_script = {"900000000000001": ["FINISHED"]}

    tick = await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert tick.deferred == 1 and stored is not None and stored.state == "pending"
    assert stored.failure_code == "quota_exhausted" and stored.container_id == "900000000000001"
    assert not any(c.path.endswith("media_publish") for c in graph.calls)


async def test_an_approval_that_expires_at_the_slot_sends_nothing() -> None:
    graph = FakeGraph()
    binding = meta_binding("instagram_professional", enabled=IG_IMAGE)
    store, service, record = await setup(
        graph,
        binding,
        meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,)),
        expires_at=NOW - timedelta(seconds=1),
        scheduled_at=NOW - timedelta(hours=1),
    )

    tick = await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert tick.expired == 1 and stored is not None and stored.state == "expired"
    assert graph.calls == []


# ------------------------------------------------------------------- replies


@pytest.mark.parametrize("platform", ["facebook_page", "instagram_professional"])
async def test_a_reply_checks_its_parent_then_sends_once(platform: str) -> None:
    graph = FakeGraph()
    version = graph.add_comment("777_1", "Quand commence la saison ?", platform=platform)
    binding = meta_binding(platform, enabled=REPLY)
    payload = meta_payload(
        platform,
        action="reply",
        fmt="comment_reply",
        caption="Samedi !",
        parent={"post_id": "777", "comment_id": "777_1", "comment_version": version},
    )
    store, service, record = await setup(graph, binding, payload)

    tick = await service.tick(binding, now=NOW, dry_run=False)

    edge = "comments" if platform == "facebook_page" else "replies"
    assert tick.published == 1
    assert [c.path for c in graph.writes()] == [f"777_1/{edge}"]
    assert graph.calls[0].method == "GET" and graph.calls[0].path == "777_1"


async def test_a_reply_to_an_edited_parent_is_held_with_zero_writes() -> None:
    graph = FakeGraph()
    version = graph.add_comment("777_1", "Quand commence la saison ?")
    graph.add_comment("777_1", "Quand commence la saison ? Et les billets ?")  # edited since approval
    binding = meta_binding(enabled=REPLY)
    payload = meta_payload(
        action="reply",
        fmt="comment_reply",
        caption="Samedi !",
        parent={"post_id": "777", "comment_id": "777_1", "comment_version": version},
    )
    store, service, record = await setup(graph, binding, payload)

    tick = await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert graph.writes() == [] and stored is not None and stored.state == "pending"
    assert tick.failures[0]["code"] == "parent_changed"


async def test_a_reply_to_a_deleted_parent_is_closed_with_zero_writes() -> None:
    graph = FakeGraph()
    binding = meta_binding(enabled=REPLY)
    payload = meta_payload(
        action="reply",
        fmt="comment_reply",
        caption="Samedi !",
        parent={"post_id": "777", "comment_id": "777_gone", "comment_version": "a" * 64},
    )
    store, service, record = await setup(graph, binding, payload)

    await service.tick(binding, now=NOW, dry_run=False)

    stored = await store.get(record.publication_id)
    assert graph.writes() == [] and stored is not None and stored.state == "failed"
    assert stored.failure_code == "parent_missing"


async def test_a_lost_reply_is_reconciled_read_only() -> None:
    graph = FakeGraph()
    version = graph.add_comment("777_1", "Quand ?")
    graph.faults["reply"] = ["lost"]
    binding = meta_binding(enabled=REPLY)
    payload = meta_payload(
        action="reply",
        fmt="comment_reply",
        caption="Samedi !",
        parent={"post_id": "777", "comment_id": "777_1", "comment_version": version},
    )
    store, service, record = await setup(graph, binding, payload)

    await service.tick(binding, now=NOW, dry_run=False)
    tick = await service.tick(binding, now=NOW + timedelta(minutes=1), dry_run=False)

    stored = await store.get(record.publication_id)
    assert tick.reconciled == 1 and stored is not None and stored.state == "published"
    assert len(graph.writes()) == 1


async def test_a_token_of_another_version_is_refused_before_transport() -> None:
    from social_publish.adapters.meta import MetaGraphAdapter
    from tests.social_fakes import Signer, Tokens, _no_sleep

    graph = FakeGraph()
    binding = meta_binding(enabled=FB_TEXT)
    store = InMemorySocialStore()
    record = record_for(binding, meta_payload())
    await store.import_intent(record, approval_for(record), actor="test", at=NOW)
    stale = MetaGraphAdapter(graph.client(), Tokens("v0"), media=Signer(), sleep=_no_sleep)
    service = DispatchService(store, {"meta_graph": stale})

    with pytest.raises(Refused, match="credential_version_mismatch"):
        await service.dispatch_one(binding, record.publication_id, now=NOW)
    assert graph.calls == []


def test_outcome_unknown_is_not_a_refusal() -> None:
    assert not issubclass(OutcomeUnknown, Refused)
