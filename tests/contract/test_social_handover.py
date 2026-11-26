"""The C3 hand-over: `publish.py export-source` → `import_c3_handover` (F37).

The document is `social-ops-handover/1` as comms-mcp's social-ops `publish.py
export-source` prints it (PR https://github.com/stromy-org/comms-mcp/pull/86,
head 3b1c52bd). The vectors are re-copied from that head; the third,
`ig_feed_image_api`, is an Instagram image on the `graph_api_publish` route with
C3's default late window (Decision D24: expiry = slot + 1 h).
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from social_publish import (
    C3_HANDOVER_VERSION,
    ApprovalRefused,
    DispatchService,
    InMemorySocialStore,
    canonical_digest,
    canonical_json,
    import_c3_handover,
    verify_record,
)
from social_publish.approval import _STATE_MAP  # pyright: ignore[reportPrivateUsage]
from tests.contract.test_social_import import GOLDEN, INSTANCE, record, sign, trust
from tests.social_fakes import FakeGraph, adapter, meta_binding

pytestmark = [pytest.mark.contract]

API = GOLDEN["vectors"]["ig_feed_image_api"]
CAPTION = API["inputs"]["post"]["caption"]
IMPORT_AT = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)
IG_BINDING = meta_binding("instagram_professional", enabled=(("publish_post", "feed_image"),))


def export(rec: dict[str, Any] | None = None, **overrides: Any) -> dict[str, Any]:
    """The document `publish.py export-source` prints for a never-dispatched, quiesced operation."""
    rec = rec or record(API)
    env = rec["envelope"]
    doc: dict[str, Any] = {
        "export_version": C3_HANDOVER_VERSION,
        "exported_at": "2026-12-03T09:59:00Z",
        "binding_id": env["binding_id"],
        "operation_key": "/".join((env["instance_id"], env["target"]["account_id"], env["post_id"], env["operation"])),
        "post_id": env["post_id"],
        "route": env["route"],
        "approval_sha256": rec["attestation"]["envelope_sha256"],
        "record": rec,
        "later_records": [],
        "caption": CAPTION,
        "source": {"quiesced": True, "state": None, "remote_id": None, "container_id": None},
        "transfer_with": f"publish.py ... transfer --post {env['post_id']} --intent-id <id> --expect-state none",
    }
    doc.update(overrides)
    return doc


async def handover(store: InMemorySocialStore, doc: object, binding=IG_BINDING, now=IMPORT_AT):  # noqa: ANN001, ANN201
    return await import_c3_handover(
        store, binding, doc, trust=trust(), instance=INSTANCE, actor="fixture-operator", now=now
    )


def test_the_api_vector_reproduces_byte_for_byte() -> None:
    assert canonical_json(API["envelope"]).decode() == API["canonical_envelope"]
    assert canonical_digest(API["envelope"]) == API["envelope_sha256"]
    assert canonical_json(API["attestation"]).decode() == API["canonical_attestation"]
    assert API["envelope"]["route"] == "graph_api_publish"
    verify_record(record(API), trust(), INSTANCE, IMPORT_AT)


async def test_the_api_vector_hands_over_as_pending_and_publishes_once_at_its_slot() -> None:
    store = InMemorySocialStore()

    outcome = await handover(store, export())

    row = outcome.record
    assert outcome.created and row.state == "pending"
    assert (row.operation, row.format) == ("publish_post", "feed_image")
    assert row.intent_id == API["operation_key"]
    slot = datetime(2026, 12, 12, 9, 0, tzinfo=UTC)
    assert (row.scheduled_at, row.expires_at) == (slot, slot + timedelta(hours=1))  # D24: C3's late window
    assert (await handover(store, export())).created is False  # the same hand-over twice: one owner

    graph = FakeGraph()
    graph.status_script = {"900000000000001": ["FINISHED"]}
    service = DispatchService(store, {"meta_graph": adapter(graph)})
    await service.tick(IG_BINDING, now=slot - timedelta(minutes=1), dry_run=False)  # not yet due
    assert graph.writes() == []
    await service.tick(IG_BINDING, now=slot + timedelta(minutes=5), dry_run=False)
    await service.tick(IG_BINDING, now=slot + timedelta(minutes=10), dry_run=False)
    published = await store.get(row.publication_id)
    assert published is not None and published.state == "published"
    assert [c.path for c in graph.writes()] == [f"{graph.ig}/media", f"{graph.ig}/media_publish"]


async def test_a_hand_over_whose_digest_does_not_match_its_record_is_refused() -> None:
    """Negative control: the document's approval digest must be the attested envelope digest."""
    store = InMemorySocialStore()
    with pytest.raises(ApprovalRefused, match="handover_mismatch"):
        await handover(store, export(approval_sha256="0" * 64))
    assert await store.all_for_binding(IG_BINDING.binding_id) == []  # nothing written


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"export_version": "social-ops-handover/2"}, "handover_invalid"),
        ({"operation_key": "fixture-brand/ig-main/c0ffee123456/reply"}, "handover_mismatch"),
        ({"binding_id": "fixture-brand/fb-main"}, "binding_mismatch"),
        ({"route": "human_handoff"}, "handover_mismatch"),
        ({"caption": None}, "handover_invalid"),
        ({"later_records": ["not-a-record"]}, "handover_invalid"),
        ({"caption": CAPTION + " "}, "approval_mismatch"),
        (
            {"source": {"quiesced": False, "state": None, "remote_id": None, "container_id": None}},
            "source_not_quiesced",
        ),
        (
            {"source": {"quiesced": True, "state": "claimed", "remote_id": None, "container_id": None}},
            "source_not_quiesced",
        ),
    ],
    ids=[
        "version",
        "operation-key",
        "c3-binding",
        "route",
        "no-caption",
        "later",
        "caption",
        "not-quiesced",
        "claimed",
    ],
)
async def test_a_hand_over_that_disagrees_with_its_record_is_refused(overrides: dict[str, Any], code: str) -> None:
    store = InMemorySocialStore()
    with pytest.raises(ApprovalRefused, match=code):
        await handover(store, export(**overrides))
    assert await store.all_for_binding(IG_BINDING.binding_id) == []


async def test_a_hand_over_for_another_ledger_account_is_refused() -> None:
    other = IG_BINDING.model_copy(update={"account_id": "ig-other"})
    with pytest.raises(ApprovalRefused, match="binding_mismatch"):
        await handover(InMemorySocialStore(), export(), binding=other)


async def test_a_transferred_operation_is_never_an_import_source() -> None:
    assert "transferred" not in _STATE_MAP
    source = {"quiesced": True, "state": "transferred", "remote_id": None, "container_id": None}
    with pytest.raises(ApprovalRefused, match="source_state_unknown"):
        await handover(InMemorySocialStore(), export(source=source))


async def test_later_records_pass_through_and_a_revocation_refuses() -> None:
    revoke = sign(copy.deepcopy(API["envelope"]), kind="revoke", sequence=2)
    with pytest.raises(ApprovalRefused, match="approval_revoked"):
        await handover(InMemorySocialStore(), export(later_records=[revoke]))


async def test_an_unknown_source_state_passes_through_as_unknown() -> None:
    source = {"quiesced": True, "state": "unknown", "remote_id": None, "container_id": None}
    outcome = await handover(InMemorySocialStore(), export(source=source))
    assert outcome.record.state == "unknown"  # never fresh work


async def test_an_api_approval_with_no_window_past_its_slot_is_still_refused() -> None:
    """D24 keeps the ledger strict: an expiry at (or before) the slot has no dispatch window."""
    envelope = copy.deepcopy(API["envelope"])
    envelope["expires_utc"] = envelope["slot_utc"]
    rec = sign(envelope)
    with pytest.raises(ApprovalRefused, match="approval_expired"):
        await handover(InMemorySocialStore(), export(rec))
