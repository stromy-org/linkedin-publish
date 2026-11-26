"""Trusted import of a C3 approval: origin, revocation, digest, then one owner.

Driven by C3's exported golden vectors (`tests/fixtures/social/c3-golden-vectors.json`,
copied from comms-mcp `social-ops/assets/fixtures/approval/golden-vectors.json`
at a8d0cfb). Records are signed in-test with the vectors' public test seed — a
key that never signs a real approval.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from social_publish import (
    ApprovalRefused,
    DispatchService,
    ImportSource,
    InMemorySocialStore,
    canonical_digest,
    canonical_json,
    import_c3_approval,
    load_trust,
    verify_record,
)
from tests.social_fakes import FakeGraph, adapter, meta_binding

pytestmark = [pytest.mark.contract]

GOLDEN = json.loads((Path(__file__).parents[1] / "fixtures" / "social" / "c3-golden-vectors.json").read_text())
FB = GOLDEN["vectors"]["fb_feed_image"]
IG = GOLDEN["vectors"]["ig_carousel_dst_overlap"]
SEED = bytes.fromhex(GOLDEN["test_key"]["seed_hex"])
INSTANCE = GOLDEN["instance"]
IMPORT_AT = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)
QUIESCED = ImportSource(quiesced=True)
FB_BINDING = meta_binding("facebook_page", enabled=(("schedule_post", "feed_image"),))
IG_BINDING = meta_binding("instagram_professional", enabled=(("publish_post", "feed_image"),))


def trust(**overrides: object) -> dict[str, Any]:
    key = {k: GOLDEN["test_key"][k] for k in ("key_id", "public_key_b64", "actor", "role")}
    key.update(overrides)  # type: ignore[arg-type]
    return load_trust({"schema_version": "social-ops-trust/1", "keys": [key]})


def record(vector: dict[str, Any] = FB) -> dict[str, Any]:
    return {
        "record_version": GOLDEN["record_version"],
        "envelope": copy.deepcopy(vector["envelope"]),
        "attestation": copy.deepcopy(vector["attestation"]),
        "signature_b64": vector["signature_b64"],
    }


def sign(envelope: dict[str, Any], **attestation: object) -> dict[str, Any]:
    """A record over `envelope`, signed with the public test seed."""
    att = {
        "actor": GOLDEN["test_key"]["actor"],
        "approved_at": "2026-12-03T09:00:00Z",
        "envelope_sha256": canonical_digest(envelope),
        "evidence": {"board_ref": "fixture-board"},
        "key_id": GOLDEN["test_key"]["key_id"],
        "kind": "approve",
        "operation_key": "/".join(
            (envelope["instance_id"], envelope["target"]["account_id"], envelope["post_id"], envelope["operation"])
        ),
        "presence": "test",
        "sequence": 1,
        "supersedes": None,
    }
    att.update(attestation)
    signature = Ed25519PrivateKey.from_private_bytes(SEED).sign(canonical_json(att))
    return {
        "record_version": GOLDEN["record_version"],
        "envelope": envelope,
        "attestation": att,
        "signature_b64": base64.b64encode(signature).decode(),
    }


CAPTION = FB["inputs"]["post"]["caption"]


async def do_import(store, rec=None, *, source=QUIESCED, binding=FB_BINDING, caption=CAPTION, **kw):  # noqa: ANN001, ANN201
    return await import_c3_approval(
        store,
        binding,
        record=rec or record(),
        caption=caption,
        trust=kw.pop("trust_set", None) or trust(),
        instance=INSTANCE,
        source=source,
        actor="fixture-operator",
        now=kw.pop("now", IMPORT_AT),
        **kw,
    )


# -------------------------------------------------------------- golden vectors


@pytest.mark.parametrize("vector", [FB, IG], ids=["fb", "ig"])
def test_the_golden_envelopes_and_attestations_reproduce_byte_for_byte(vector: dict[str, Any]) -> None:
    assert canonical_json(vector["envelope"]).decode() == vector["canonical_envelope"]
    assert canonical_digest(vector["envelope"]) == vector["envelope_sha256"]
    assert canonical_json(vector["attestation"]).decode() == vector["canonical_attestation"]
    verify_record(record(vector), trust(), INSTANCE, IMPORT_AT)


@pytest.mark.parametrize("mutation", GOLDEN["mutations"], ids=[m["name"] for m in GOLDEN["mutations"]])
def test_every_golden_mutation_changes_the_digest_and_is_rejected(mutation: dict[str, Any]) -> None:
    rec = record(GOLDEN["vectors"][mutation["vector"]])
    node = rec["envelope"]
    for step in mutation["path"][:-1]:
        node = node[step]
    node[mutation["path"][-1]] = mutation["value"]
    assert canonical_digest(rec["envelope"]) == mutation["envelope_sha256"]
    with pytest.raises(ApprovalRefused, match="approval_untrusted"):
        verify_record(rec, trust(), INSTANCE, IMPORT_AT)


# ------------------------------------------------------------- origin checks


def test_an_untrusted_key_a_forged_author_and_a_revoked_key_are_refused() -> None:
    other = Ed25519PrivateKey.generate()
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    from social_publish.approval import key_id_for

    public = other.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    stranger = load_trust(
        {
            "schema_version": "social-ops-trust/1",
            "keys": [
                {
                    "key_id": key_id_for(public),
                    "public_key_b64": base64.b64encode(public).decode(),
                    "actor": "Someone Else",
                    "role": "operator",
                }
            ],
        }
    )
    with pytest.raises(ApprovalRefused, match="approval_untrusted"):
        verify_record(record(), stranger, INSTANCE, IMPORT_AT)

    forged = sign(copy.deepcopy(FB["envelope"]), actor="Not The Operator")
    with pytest.raises(ApprovalRefused, match="approval_forged_author"):
        verify_record(forged, trust(), INSTANCE, IMPORT_AT)

    with pytest.raises(ApprovalRefused, match="grant_revoked"):
        verify_record(record(), trust(revoked_at="2026-12-03T09:30:00Z"), INSTANCE, IMPORT_AT)


async def test_a_later_revocation_record_refuses_the_import() -> None:
    revoke = sign(copy.deepcopy(FB["envelope"]), kind="revoke", sequence=2)
    with pytest.raises(ApprovalRefused, match="approval_revoked"):
        await do_import(InMemorySocialStore(), later_records=[revoke])


async def test_caption_bytes_that_do_not_reproduce_the_digest_are_refused() -> None:
    with pytest.raises(ApprovalRefused, match="approval_mismatch"):
        await do_import(InMemorySocialStore(), caption=CAPTION + " ")


async def test_an_approval_for_another_account_is_refused() -> None:
    other = meta_binding("facebook_page").model_copy(update={"native_account_id": "100000000000009"})
    with pytest.raises(ApprovalRefused, match="binding_mismatch"):
        await do_import(InMemorySocialStore(), binding=other)


async def test_a_source_that_is_not_quiesced_or_holds_a_claim_is_refused() -> None:
    with pytest.raises(ApprovalRefused, match="source_not_quiesced"):
        await do_import(InMemorySocialStore(), source=ImportSource(quiesced=False))
    with pytest.raises(ApprovalRefused, match="source_not_quiesced"):
        await do_import(InMemorySocialStore(), source=ImportSource(quiesced=True, state="claimed"))


# ------------------------------------------------------------ one owner


async def test_importing_the_same_approval_twice_gives_a_single_owner() -> None:
    store = InMemorySocialStore()

    first = await do_import(store)
    second = await do_import(store)

    assert first.created and not second.created
    assert second.record.publication_id == first.record.publication_id
    rows = await store.all_for_binding(FB_BINDING.binding_id)
    assert len(rows) == 1
    assert rows[0].intent_id == FB["operation_key"] and rows[0].state == "pending"
    assert rows[0].payload["envelope_sha256"] == FB["envelope_sha256"]
    approval = await store.get_approval(rows[0].publication_id)
    assert approval is not None and approval.payload_digest == rows[0].payload_digest


async def test_concurrent_imports_of_one_approval_still_give_one_owner() -> None:
    store = InMemorySocialStore()

    outcomes = await asyncio.gather(*[do_import(store) for _ in range(6)])

    assert sum(o.created for o in outcomes) == 1
    assert len({o.record.publication_id for o in outcomes}) == 1


@pytest.mark.parametrize(
    ("c3_state", "ledger_state"),
    [
        ("scheduled", "scheduled"),
        ("sending", "unknown"),
        ("unknown", "unknown"),
        ("created", "unknown"),
        ("cancel_failed", "unknown"),
        ("published", "published"),
    ],
)
async def test_an_already_dispatched_operation_never_becomes_fresh_work(c3_state: str, ledger_state: str) -> None:
    graph = FakeGraph()
    store = InMemorySocialStore()
    remote = f"{FB_BINDING.native_account_id}_555"
    source = ImportSource(quiesced=True, state=c3_state, remote_id=remote)
    if c3_state == "scheduled":
        graph.scheduled[remote] = {"id": remote, "message": CAPTION, "scheduled_publish_time": 0, "is_published": False}

    first = await do_import(store, source=source)
    again = await do_import(store, source=source)  # the double import of a dispatched operation
    service = DispatchService(store, {"meta_graph": adapter(graph)})
    for minutes in (0, 5, 60):
        await service.tick(FB_BINDING, now=IMPORT_AT + timedelta(minutes=minutes), dry_run=False)

    assert first.record.state == ledger_state and not again.created
    assert len(await store.all_for_binding(FB_BINDING.binding_id)) == 1
    assert graph.writes() == []  # nothing was sent, ever


async def test_an_instagram_handoff_imports_as_a_handoff_and_is_never_published() -> None:
    graph = FakeGraph()
    store = InMemorySocialStore()
    ig_caption = IG["inputs"]["post"]["caption"]

    pending = ImportSource(quiesced=True)
    with pytest.raises(ApprovalRefused, match="route_handoff_only"):
        await do_import(store, record(IG), binding=IG_BINDING, caption=ig_caption, source=pending)

    handed = ImportSource(quiesced=True, state="handoff_recorded", remote_id="planner-ref-1")
    outcome = await do_import(store, record(IG), binding=IG_BINDING, caption=ig_caption, source=handed)
    service = DispatchService(store, {"meta_graph": adapter(graph)})
    await service.tick(IG_BINDING, now=datetime(2026, 10, 25, 0, 0, tzinfo=UTC), dry_run=False)

    assert outcome.record.state == "handoff"
    assert graph.calls == []


async def test_a_newer_approval_replaces_a_pending_row_but_not_a_dispatched_one() -> None:
    store = InMemorySocialStore()
    first = await do_import(store)
    envelope = copy.deepcopy(FB["envelope"])
    envelope["packet_revision"] = "b" * 64
    newer = sign(envelope, sequence=2, supersedes=FB["envelope_sha256"])

    replaced = await do_import(store, newer)

    assert replaced.created and replaced.superseded == first.record.publication_id
    old = await store.get(first.record.publication_id)
    assert old is not None and old.state == "cancelled"
    live = await store.live_for_intent(FB["operation_key"])
    assert live is not None and live.publication_id == replaced.record.publication_id

    # Once the live row has reached Meta, a third approval cannot take it over.
    await store.compare_and_set(
        live.publication_id,
        expected_state="pending",
        expected_attempt_token=None,
        updates={"state": "claimed", "attempt_token": "t"},
    )
    await store.compare_and_set(
        live.publication_id, expected_state="claimed", expected_attempt_token="t", updates={"state": "sending"}
    )
    envelope3 = copy.deepcopy(FB["envelope"])
    envelope3["packet_revision"] = "c" * 64
    with pytest.raises(ApprovalRefused, match="old_action_unresolved"):
        await do_import(store, sign(envelope3, sequence=3))


async def test_an_imported_facebook_approval_dispatches_once_inside_its_window() -> None:
    graph = FakeGraph()
    store = InMemorySocialStore()
    imported = await do_import(store)
    service = DispatchService(store, {"meta_graph": adapter(graph)})

    await service.tick(FB_BINDING, now=IMPORT_AT, dry_run=False)
    await service.tick(FB_BINDING, now=IMPORT_AT + timedelta(minutes=5), dry_run=False)

    stored = await store.get(imported.record.publication_id)
    assert stored is not None and stored.state == "scheduled"
    assert [c.path for c in graph.writes()] == [f"{FB_BINDING.native_account_id}/photos"]
    assert stored.expires_at == datetime(2026, 12, 8, 10, 50, tzinfo=UTC)  # slot minus the 10-minute lead
