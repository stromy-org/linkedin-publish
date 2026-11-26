"""The shared adapter contract: one set of invariants, every adapter must pass it.

Parametrized over the LinkedIn adapter (the unchanged `LinkedInClient` behind
`social_publish.adapters.linkedin`) and the Meta adapter for both of its
platforms. Each case counts provider requests, because the invariants are all
of the form "this never reaches the provider":

1. Capability is per (operation, format): a format the binding has not enabled
   is refused before transport, and the record keeps its format (no downgrade).
2. An uncertain response leaves `unknown`; no other adapter is ever tried for
   that record, and later ticks never send it again.
3. Reconciliation is read-only: zero provider writes.
4. The adapter comes from the trusted binding; an unknown adapter name is
   refused before transport.
5. A disabled binding (the rollback switch) and a dry run send nothing.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta

import httpx
import pytest

from linkedin_publish import PostDraft, StaticCredentialProvider
from social_publish import (
    DispatchService,
    InMemorySocialStore,
    MetaPayload,
    SocialBinding,
    SocialKey,
    SocialRecord,
)
from social_publish.adapters.linkedin import LinkedInAdapter, neutral_binding
from tests.conftest import PERSON, Recorder, make_client
from tests.conftest import binding as linkedin_binding
from tests.social_fakes import IMAGE, NOW, FakeGraph, adapter, approval_for, meta_binding, meta_payload, record_for

pytestmark = [pytest.mark.contract]

POST_URN = "urn:li:share:7100000000000000000"


@dataclass
class Harness:
    """One adapter under test: its binding, a record factory, and its request log."""

    name: str
    adapter: object
    binding: SocialBinding
    supported: SocialRecord  # a record of an enabled, declared (operation, format)
    unsupported: SocialRecord  # same account, a format the binding has not enabled
    writes: object  # () -> int
    calls: object  # () -> int
    lose_next_write: object  # () -> None

    def write_count(self) -> int:
        return self.writes()  # type: ignore[operator]

    def call_count(self) -> int:
        return self.calls()  # type: ignore[operator]


def _linkedin(credentials) -> Harness:  # noqa: ANN001
    state = {"lose": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v2/ugcPosts":
            if state["lose"]:
                raise httpx.ReadTimeout("lost")
            return httpx.Response(201, json={}, headers={"x-restli-id": POST_URN})
        return httpx.Response(404, json={})

    client, log = make_client(handler)
    native = linkedin_binding(capabilities=("text",))
    neutral = neutral_binding(native)
    li = LinkedInAdapter("share_ugc", client, StaticCredentialProvider(credentials), {native.binding_id: native})

    def rec(draft: PostDraft, publication_id: str) -> SocialRecord:
        payload = draft.model_dump(mode="json")
        fmt = draft.required_capability
        return SocialRecord(
            publication_id=publication_id,
            key=SocialKey(
                subject_kind="entra_oid",
                subject_id="subject-1",
                campaign_id="c",
                post_id=publication_id,
                account_id=native.account_id,
            ),
            binding_id=native.binding_id,
            platform="linkedin",
            payload=payload,
            payload_digest=hashlib.sha256(draft.model_dump_json().encode()).hexdigest(),
            operation="publish",
            format=fmt,
            scheduled_at=NOW - timedelta(minutes=1),
            expires_at=NOW + timedelta(hours=3),
            intent_id=f"li/{publication_id}",
        )

    text = rec(PostDraft(author_urn=PERSON, commentary="Intelligence, orchestrated."), "li-text")
    image = rec(
        PostDraft.model_validate(
            {
                "author_urn": PERSON,
                "commentary": "With a picture.",
                "media": {"kind": "image", "asset": {"sha256": "a" * 64}},
            }
        ),
        "li-image",
    )

    def lose() -> None:
        state["lose"] = True

    log_ref: Recorder = log
    return Harness(
        "linkedin",
        li,
        neutral,
        text,
        image,
        writes=lambda: sum(1 for c in log_ref.calls if c.method == "POST"),
        calls=lambda: log_ref.count,
        lose_next_write=lose,
    )


def _meta(platform: str) -> Harness:
    graph = FakeGraph()
    if platform == "facebook_page":
        enabled = (("schedule_post", "text"),)
        supported = meta_payload()
        unsupported = meta_payload(fmt="feed_image", media=(IMAGE,))  # declared, but not enabled here
        lose_op = "feed"
    else:
        enabled = (("publish_post", "feed_image"),)
        supported = meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,))
        graph.status_script = {"900000000000001": ["FINISHED"]}
        unsupported = MetaPayload.model_validate(
            {
                **meta_payload("instagram_professional", fmt="feed_image", media=(IMAGE,)).model_dump(),
                "format": "carousel",
                "media": (IMAGE, "sha256:" + "d" * 64),
            }
        )
        lose_op = "media_publish"
    binding = meta_binding(platform, enabled=enabled)
    return Harness(
        platform,
        adapter(graph),
        binding,
        record_for(binding, supported, publication_id=f"{platform}-ok"),
        record_for(binding, unsupported, publication_id=f"{platform}-other"),
        writes=lambda: len(graph.writes()),
        calls=lambda: len(graph.calls),
        lose_next_write=lambda: graph.faults.setdefault(lose_op, []).append("lost_before"),
    )


@pytest.fixture(params=["linkedin", "facebook_page", "instagram_professional"])
def harness(request, credentials) -> Harness:  # noqa: ANN001
    if request.param == "linkedin":
        return _linkedin(credentials)
    return _meta(request.param)


async def _store(*records: SocialRecord) -> InMemorySocialStore:
    store = InMemorySocialStore()
    for record in records:
        await store.import_intent(record, approval_for(record), actor="test", at=NOW)
    return store


class Decoy:
    """A second adapter that must never be called for another binding's record."""

    def __init__(self, name: str, platforms: frozenset[str]) -> None:
        self.name = name
        self.platforms = platforms
        self.touched = 0

    def declared(self) -> frozenset[tuple[str, str]]:
        self.touched += 1
        return frozenset()

    def __getattr__(self, item: str):  # noqa: ANN204
        self.touched += 1
        raise AssertionError(f"decoy adapter used: {item}")


async def test_an_unenabled_format_is_refused_before_transport_and_never_downgraded(harness: Harness) -> None:
    store = await _store(harness.unsupported)
    service = DispatchService(store, {harness.binding.adapter: harness.adapter})  # type: ignore[dict-item]

    tick = await service.tick(harness.binding, now=NOW, dry_run=False)

    assert tick.skipped_capability == 1 and tick.sent == 0
    assert harness.call_count() == 0
    stored = await store.get(harness.unsupported.publication_id)
    assert stored is not None and stored.state == "pending"
    assert (stored.format, stored.payload) == (harness.unsupported.format, harness.unsupported.payload)


async def test_an_enabled_format_goes_out_exactly_once(harness: Harness) -> None:
    store = await _store(harness.supported)
    service = DispatchService(store, {harness.binding.adapter: harness.adapter})  # type: ignore[dict-item]

    await service.tick(harness.binding, now=NOW, dry_run=False)
    await service.tick(harness.binding, now=NOW + timedelta(minutes=1), dry_run=False)

    assert harness.write_count() == (2 if harness.name == "instagram_professional" else 1)  # IG: container + publish
    stored = await store.get(harness.supported.publication_id)
    assert stored is not None and stored.state in ("published", "scheduled")


async def test_an_uncertain_response_is_unknown_and_no_other_adapter_is_tried(harness: Harness) -> None:
    harness.lose_next_write()  # type: ignore[operator]
    store = await _store(harness.supported)
    decoys = {
        name: Decoy(name, frozenset({harness.binding.platform})) for name in ("share_ugc", "rest_posts", "meta_graph")
    }
    decoys[harness.binding.adapter] = harness.adapter  # type: ignore[assignment]
    service = DispatchService(store, decoys)  # type: ignore[arg-type]

    first = await service.tick(harness.binding, now=NOW, dry_run=False)
    writes_after_send = harness.write_count()
    later = [await service.tick(harness.binding, now=NOW + timedelta(minutes=m), dry_run=False) for m in (1, 2)]

    assert first.unknown == 1
    stored = await store.get(harness.supported.publication_id)
    assert stored is not None and stored.state == "unknown"
    assert all(t.sent == 0 for t in later)
    assert harness.write_count() == writes_after_send  # reconciliation wrote nothing
    assert all(d.touched == 0 for d in decoys.values() if isinstance(d, Decoy))


async def test_the_adapter_comes_from_the_binding_and_a_missing_one_refuses(harness: Harness) -> None:
    from social_publish import Refused

    store = await _store(harness.supported)
    service = DispatchService(store, {"not-this-one": harness.adapter})  # type: ignore[dict-item]

    with pytest.raises(Refused, match="adapter_unavailable"):
        await service.tick(harness.binding, now=NOW, dry_run=False)
    assert harness.call_count() == 0


async def test_a_disabled_binding_and_a_dry_run_send_nothing(harness: Harness) -> None:
    store = await _store(harness.supported)
    service = DispatchService(store, {harness.binding.adapter: harness.adapter})  # type: ignore[dict-item]
    disabled = harness.binding.model_copy(update={"publish_enabled": False})

    dry = await service.tick(harness.binding, now=NOW, dry_run=True)
    off = await service.tick(disabled, now=NOW, dry_run=False)

    assert dry.due == 1 and dry.sent == 0 and off.disabled and off.sent == 0
    assert harness.write_count() == 0
    stored = await store.get(harness.supported.publication_id)
    assert stored is not None and stored.state == "pending"


@pytest.mark.parametrize("platform", ["facebook_page", "instagram_professional"])
async def test_a_binding_claim_on_an_undeclared_format_is_still_refused(platform: str) -> None:
    """Enabling a format the adapter does not implement licenses nothing."""
    graph = FakeGraph()
    op = "schedule_post" if platform == "facebook_page" else "publish_post"
    binding = meta_binding(platform, enabled=((op, "carousel"),))
    base = meta_payload(platform, fmt="feed_image", media=(IMAGE,)).model_dump()
    carousel = MetaPayload.model_validate({**base, "format": "carousel", "media": (IMAGE, "sha256:" + "d" * 64)})
    store = await _store(record_for(binding, carousel))
    service = DispatchService(store, {"meta_graph": adapter(graph)})

    tick = await service.tick(binding, now=NOW, dry_run=False)

    assert tick.skipped_capability == 1 and graph.calls == []
