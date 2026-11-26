"""Meta commissioning on the neutral core: one grant, one send, enablement from its receipt.

Mirrors LinkedIn's canary route (ORG-PLAN-285 C1 steps 7–8) for Meta bindings
(ORG-PLAN-652 C5a, F32). Offline: a recorded Graph (`tests.social_fakes`) and
the in-memory stores, which keep the Postgres stores' rules. The database half
(the commissioner role, the runtime refused by a column grant) is in
`tests/integration/test_meta_commissioning.py`.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from social_publish import (
    GRANT_TTL,
    DispatchService,
    InMemorySocialBindingStore,
    InMemorySocialStore,
    OperationCapability,
    Refused,
    SocialBinding,
    SocialGrant,
    SocialRecord,
    capability_key,
    commission_problems,
    enable_problems,
    new_grant,
)
from tests.social_fakes import (
    IMAGE,
    NOW,
    FakeGraph,
    adapter,
    approval_for,
    meta_binding,
    meta_payload,
    record_for,
)

pytestmark = [pytest.mark.contract]

ACTOR = "fixture-operator"


def fresh(platform: str = "facebook_page", **updates: object) -> SocialBinding:
    """A registered, DISABLED binding with every capability unknown and its token measured."""
    base = meta_binding(platform, publish_enabled=False).model_copy(
        update={"token_expires_at": NOW + timedelta(days=50), "token_observed_at": NOW - timedelta(hours=1)}
    )
    return base.model_copy(update=updates)


async def stored(binding: SocialBinding, record: SocialRecord) -> InMemorySocialStore:
    store = InMemorySocialStore([binding])
    await store.import_intent(record, approval_for(record), actor=ACTOR, at=NOW)
    return store


def fb_text(binding: SocialBinding, publication_id: str = "canary-1") -> SocialRecord:
    return record_for(binding, meta_payload(caption=f"Fixture canary {publication_id}."), publication_id=publication_id)


async def granted(store: InMemorySocialStore, record: SocialRecord, **overrides: object) -> SocialGrant:
    grant = new_grant(record, actor=ACTOR, now=NOW).model_copy(update=overrides)
    await store.put_grant(grant)
    return grant


def commissioning(store: InMemorySocialStore, graph: FakeGraph) -> DispatchService:
    return DispatchService(store, {"meta_graph": adapter(graph)}, actor=ACTOR, commissioning_actor=ACTOR)


def scheduler(store: InMemorySocialStore, graph: FakeGraph) -> DispatchService:
    return DispatchService(store, {"meta_graph": adapter(graph)})


# -------------------------------------------------------------------- grant


async def test_a_grant_sends_one_unknown_canary_on_a_disabled_binding_and_is_spent_at_sending() -> None:
    binding = fresh()
    record = fb_text(binding)
    store = await stored(binding, record)
    grant = await granted(store, record)
    graph = FakeGraph()

    outcome = await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)

    assert outcome == "scheduled"
    assert [c.path for c in graph.writes()] == [f"{graph.page}/feed"]
    assert await store.get_grant(record.publication_id) is None  # consumed, not open
    kinds = [e.kind for e in await store.events(record.publication_id)]
    assert kinds.index("grant_consumed") < kinds.index("sending") < kinds.index("scheduled")
    assert grant.capability == capability_key("schedule_post", "text") == "schedule_post:text"
    # One send only: the record is no longer fresh work, and the grant is gone.
    with pytest.raises(Refused, match="publish_disabled"):  # the spent grant licenses nothing more
        await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)
    assert len(graph.writes()) == 1


async def test_the_scheduler_cannot_use_a_grant() -> None:
    """Negative control: the same grant, the same record — the scheduler sends nothing."""
    for publish_enabled in (False, True):
        binding = fresh(publish_enabled=publish_enabled)
        record = fb_text(binding)
        store = await stored(binding, record)
        await granted(store, record)
        graph = FakeGraph()

        tick = await scheduler(store, graph).tick(binding, now=NOW, dry_run=False)
        if publish_enabled:
            assert tick.skipped_capability == 1  # `unknown` is not permission without a usable grant
        with pytest.raises(Refused, match="publish_disabled|capability_unavailable"):
            await scheduler(store, graph).dispatch_one(binding, record.publication_id, now=NOW)

        assert graph.writes() == []
        assert await store.get_grant(record.publication_id) is not None  # never spent
        left = await store.get(record.publication_id)
        assert left is not None and left.state == "pending"


async def test_a_grant_for_another_format_licenses_nothing() -> None:
    """Negative control: a `schedule_post:feed_image` grant on a `text` canary sends nothing."""
    binding = fresh()
    record = fb_text(binding)
    store = await stored(binding, record)
    await granted(store, record, capability=capability_key("schedule_post", "feed_image"))
    graph = FakeGraph()

    with pytest.raises(Refused, match="publish_disabled"):
        await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)

    assert graph.writes() == []
    problems = commission_problems(
        binding,
        record,
        approval_for(record),
        expected_digest=record.payload_digest,
        operation="schedule_post",
        format="feed_image",
        open_grant=None,
        now=NOW,
    )
    assert any("would license nothing" in p for p in problems)


@pytest.mark.parametrize(
    "overrides",
    [
        {"issued_by": "someone-else"},
        {"expires_at": NOW - timedelta(seconds=1)},
        {"payload_digest": "0" * 64},
        {"binding_id": "bind-other"},
    ],
    ids=["other-actor", "expired", "other-digest", "other-binding"],
)
async def test_a_grant_that_does_not_match_exactly_licenses_nothing(overrides: dict[str, object]) -> None:
    binding = fresh()
    record = fb_text(binding)
    store = await stored(binding, record)
    await granted(store, record, **overrides)
    graph = FakeGraph()

    with pytest.raises(Refused, match="publish_disabled"):
        await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)
    assert graph.writes() == []


async def test_a_grant_never_licenses_an_unavailable_capability_or_a_held_binding() -> None:
    unavailable = OperationCapability(operation="schedule_post", format="text", state="unavailable", reason="probe")
    for binding in (fresh(capabilities=(unavailable,)), fresh(reenable_after_measure=True)):
        record = fb_text(binding)
        store = await stored(binding, record)
        await granted(store, record)
        graph = FakeGraph()
        with pytest.raises(Refused):
            await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)
        assert graph.writes() == []
        assert await store.get_grant(record.publication_id) is not None


async def test_a_grant_on_an_enabled_binding_still_needs_an_unknown_pair() -> None:
    """On an enabled binding a grant licenses only the unknown pair; the enabled one needs none."""
    binding = fresh(publish_enabled=True)
    record = fb_text(binding)
    store = await stored(binding, record)
    await granted(store, record)
    graph = FakeGraph()

    assert await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW) == "scheduled"
    assert await store.get_grant(record.publication_id) is None


async def test_a_grant_that_cannot_be_consumed_sends_nothing() -> None:
    """The database is the judge: a sender whose consume fails stays `claimed`, returns to `pending`."""

    class Refusing(InMemorySocialStore):
        async def consume_grant(self, grant_id: str, *, at: object) -> bool:  # type: ignore[override]
            raise PermissionError("permission denied for table commissioning_grants")

    binding = fresh()
    record = fb_text(binding)
    store = Refusing([binding])
    await store.import_intent(record, approval_for(record), actor=ACTOR, at=NOW)
    await granted(store, record)
    graph = FakeGraph()

    with pytest.raises(Refused, match="grant_unusable"):
        await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)

    assert graph.writes() == []
    left = await store.get(record.publication_id)
    assert left is not None and left.state == "pending" and left.attempt_token is None


async def test_an_instagram_feed_image_canary_stages_then_publishes_once() -> None:
    binding = fresh("instagram_professional")
    record = record_for(
        binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,)), publication_id="ig-canary"
    )
    store = await stored(binding, record)
    await granted(store, record)
    graph = FakeGraph()
    graph.status_script = {"900000000000001": ["FINISHED"]}

    assert await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW) == "published"
    assert [c.path for c in graph.writes()] == [f"{graph.ig}/media", f"{graph.ig}/media_publish"]


async def test_a_waiting_container_keeps_the_grant_unspent() -> None:
    binding = fresh("instagram_professional")
    record = record_for(
        binding, meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,)), publication_id="ig-wait"
    )
    store = await stored(binding, record)
    await granted(store, record)
    graph = FakeGraph()  # the container never leaves IN_PROGRESS

    assert await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW) == "waiting"
    assert await store.get_grant(record.publication_id) is not None
    assert [c.path for c in graph.writes()] == [f"{graph.ig}/media"]  # a container, never a publish


def test_commissioning_refusals_are_named() -> None:
    binding = fresh()
    record = fb_text(binding)
    enabled = fresh(
        capabilities=(
            OperationCapability(
                operation="schedule_post", format="text", state="enabled", evidence_publication_id="earlier"
            ),
        )
    )
    open_grant = new_grant(record, actor=ACTOR, now=NOW)

    def check(**kw: object) -> list[str]:
        args: dict[str, object] = {
            "binding": binding,
            "record": record,
            "approval": approval_for(record),
            "expected_digest": record.payload_digest,
            "operation": "schedule_post",
            "format": "text",
            "open_grant": None,
            "now": NOW,
        }
        args.update(kw)
        return commission_problems(**args)  # type: ignore[arg-type]

    assert check() == []
    assert any("already enabled" in p for p in check(binding=enabled))
    assert any("not the" in p and "you reviewed" in p for p in check(expected_digest="f" * 64))
    assert any("no active approval" in p for p in check(approval=None))
    assert any("already holds an open grant" in p for p in check(open_grant=open_grant))
    assert any("window closed" in p for p in check(now=record.expires_at))
    assert any("held" in p for p in check(binding=fresh(reenable_after_measure=True)))
    assert check(record=None) == ["the publication is not in the ledger"]
    with pytest.raises(ValueError, match="at most"):
        new_grant(record, actor=ACTOR, now=NOW, ttl=GRANT_TTL + timedelta(seconds=1))


async def test_a_second_open_grant_is_refused() -> None:
    binding = fresh()
    record = fb_text(binding)
    store = await stored(binding, record)
    await granted(store, record)
    from social_publish.errors import StoreConflict

    with pytest.raises(StoreConflict, match="open grant"):
        await granted(store, record)


# ------------------------------------------------------------------- enable


async def _sent_canary(binding: SocialBinding, record: SocialRecord) -> tuple[InMemorySocialStore, FakeGraph]:
    store = await stored(binding, record)
    await granted(store, record)
    graph = FakeGraph()
    graph.status_script = {"900000000000001": ["FINISHED"]}
    await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)
    return store, graph


@pytest.mark.parametrize(
    ("platform", "operation", "fmt", "receipt_state"),
    [
        ("facebook_page", "schedule_post", "text", "scheduled"),
        ("instagram_professional", "publish_post", "feed_image", "published"),
        ("facebook_page", "reply", "comment_reply", "published"),
    ],
)
async def test_enable_from_the_canary_receipt_then_the_scheduler_sends(
    platform: str, operation: str, fmt: str, receipt_state: str
) -> None:
    binding = fresh(platform)
    graph_for_parent = FakeGraph()
    if operation == "reply":
        version = graph_for_parent.add_comment("c1", "Great news!")
        payload = meta_payload(
            platform,
            action="reply",
            fmt="comment_reply",
            parent={"post_id": "p1", "comment_id": "c1", "comment_version": version},
        )
    elif fmt == "feed_image":
        payload = meta_payload(platform, fmt=fmt, media=(IMAGE,))
    else:
        payload = meta_payload(platform)
    record = record_for(binding, payload, publication_id="canary")
    store = await stored(binding, record)
    await granted(store, record)
    graph = graph_for_parent
    graph.status_script = {"900000000000001": ["FINISHED"]}
    await commissioning(store, graph).dispatch_one(binding, record.publication_id, now=NOW)
    receipt = await store.get("canary")
    assert receipt is not None and receipt.state == receipt_state

    bindings = InMemorySocialBindingStore([binding])
    problems = enable_problems(binding, receipt, await store.events("canary"), operation=operation, format=fmt, now=NOW)
    assert problems == []
    enabled = await bindings.enable_capability(
        binding.binding_id,
        operation=operation,
        format=fmt,
        evidence_publication_id="canary",
        observed_at=NOW,
        expected_version=binding.credential_version,
    )
    assert enabled.publish_enabled and enabled.capability_state(operation, fmt) == "enabled"
    # Exactly that pair: every other declared pair stays unknown.
    assert [c for c in enabled.capabilities if c.state == "enabled"] == [
        c for c in enabled.capabilities if (c.operation, c.format) == (operation, fmt)
    ]
    if operation == "schedule_post":
        assert enabled.capability_state("schedule_post", "feed_image") == "unknown"
        # The scheduler now sends the next text post without any grant.
        nxt = fb_text(enabled, "second")
        await store.import_intent(nxt, approval_for(nxt), actor=ACTOR, at=NOW)
        await scheduler(store, graph).tick(enabled, now=NOW, dry_run=False)
        second = await store.get("second")
        assert second is not None and second.state == "scheduled"


async def test_enable_without_a_receipt_is_refused() -> None:
    """Negative control: no receipt of this ledger's own send, no enablement."""
    binding = fresh()
    record = fb_text(binding)
    store = await stored(binding, record)  # pending: never sent

    def problems(evidence: SocialRecord | None, events: list[object], **kw: object) -> list[str]:
        which = kw.pop("binding", binding)
        args: dict[str, object] = {"operation": "schedule_post", "format": "text", "now": NOW}
        args.update(kw)
        return enable_problems(which, evidence, events, **args)  # type: ignore[arg-type]

    assert any("receipt with a Meta object id" in p for p in problems(record, []))
    assert problems(None, [])[0] == "the evidence publication is not in the ledger"
    # An imported `scheduled` row (C3 sent it) carries a remote id but no send of ours.
    imported = record.model_copy(
        update={"state": "scheduled", "remote_id": "100000000000001_9", "adapter": "meta_graph"}
    )
    imported_events = await store.events(record.publication_id)
    assert any("imported row is never commissioning evidence" in p for p in problems(imported, imported_events))
    # An unknown send is reconciled first.
    unknown = record.model_copy(update={"state": "unknown"})
    assert any("reconciled first" in p for p in problems(unknown, imported_events))

    # A real receipt, but for another format, or without token evidence.
    sent_store, _ = await _sent_canary(binding, record.model_copy(update={"publication_id": "c2"}))
    sent = await sent_store.get("c2")
    events = await sent_store.events("c2")
    assert sent is not None
    assert problems(sent, events) == []
    assert any("enables that pair only" in p for p in problems(sent, events, format="link"))
    unmeasured = binding.model_copy(update={"token_expires_at": None, "token_observed_at": None})
    assert any("no token evidence" in p for p in problems(sent, events, binding=unmeasured))
    expired = binding.model_copy(update={"token_expires_at": NOW - timedelta(minutes=1)})
    assert any("expired" in p for p in problems(sent, events, binding=expired))
