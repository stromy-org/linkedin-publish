"""Rights evidence kept on the ledger row, and the re-check before a media URL is signed (F42).

`import_c3_approval` keeps the envelope's `rights_evidence` under
`origin["rights_evidence"]`. The golden vectors pin that no existing digest moves:
the values below were captured from the importer at 7fcd4e5 (v0.9.0), before the
evidence was kept, and are asserted byte for byte.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from social_publish import (
    ApprovalRefused,
    InMemorySocialStore,
    canonical_digest,
    require_rights,
    rights_evidence_of,
    rights_problems,
)
from tests.contract.test_social_import import (
    FB,
    FB_BINDING,
    GOLDEN,
    IG_BINDING,
    IMPORT_AT,
    do_import,
    record,
    sign,
)

pytestmark = [pytest.mark.contract]

IG_API = GOLDEN["vectors"]["ig_feed_image_api"]
HANDLE = FB["envelope"]["media"][0]

#: Captured from v0.9.0 (7fcd4e5) before rights were kept: publication id, payload digest, approval id.
PINNED = {
    "fb": ("pub_6c9af2bfc88f7088b227a30c", "6c9af2bfc88f7088b227a30c91114383428b3b4c270aab956fd31864d3794eb1",
           "c3_df54212d029e4fef5543cdd43992d3c3"),
    "ig": ("pub_77d0daa69c6a0fb70e287c35", "77d0daa69c6a0fb70e287c35f6b309d2bde4c8cebc0e2f00271fe76d432296dc",
           "c3_30ca2f30dbcdc059efc61c7f3c9945c6"),
}  # fmt: skip
#: The origin v0.9.0 wrote; the new origin is exactly this plus `rights_evidence`.
OLD_FB_ORIGIN = {
    "actor": "Fixture Operator",
    "envelope_sha256": "df54212d029e4fef5543cdd43992d3c3b503712253668f5be812d7e68394bc8c",
    "key_id": "ed25519:56475aa75463474c",
    "kind": "c3-approval",
    "route": "graph_native_schedule",
    "sequence": 1,
    "source_state": None,
}


async def imported(vector: dict[str, Any], binding: Any) -> Any:
    store = InMemorySocialStore()
    outcome = await do_import(store, record(vector), binding=binding, caption=vector["inputs"]["post"]["caption"])
    return store, outcome.record


async def test_keeping_rights_changes_no_digest_and_adds_exactly_one_origin_key() -> None:
    for name, vector, binding in (("fb", FB, FB_BINDING), ("ig", IG_API, IG_BINDING)):
        store, rec = await imported(vector, binding)
        assert (rec.publication_id, rec.payload_digest) == PINNED[name][:2]
        approval = await store.get_approval(rec.publication_id)
        assert approval is not None and approval.approval_id == PINNED[name][2]
        assert canonical_digest(vector["envelope"]) == vector["envelope_sha256"]  # the signed bytes are untouched
        assert rec.origin is not None and rec.origin["envelope_sha256"] == vector["envelope_sha256"]
        assert "rights_evidence" not in rec.payload  # never part of what is digested or sent
    _, fb = await imported(FB, FB_BINDING)
    assert fb.origin == {**OLD_FB_ORIGIN, "rights_evidence": FB["envelope"]["rights_evidence"]}


async def test_an_envelope_without_rights_imports_with_the_old_origin_unchanged() -> None:
    envelope = copy.deepcopy(FB["envelope"])
    del envelope["rights_evidence"]
    store = InMemorySocialStore()
    outcome = await do_import(store, sign(envelope), caption=FB["inputs"]["post"]["caption"])
    assert outcome.record.origin == {**OLD_FB_ORIGIN, "envelope_sha256": canonical_digest(envelope)}
    assert rights_evidence_of(outcome.record) is None
    assert rights_problems(outcome.record, now=IMPORT_AT)  # media with no retained evidence is refused


async def test_a_malformed_rights_list_refuses_the_import() -> None:
    for bad in ("nope", [{"platforms": ["facebook_page"]}], [{"handle": HANDLE, "platforms": "facebook_page"}]):
        envelope = copy.deepcopy(FB["envelope"])
        envelope["rights_evidence"] = bad
        with pytest.raises(ApprovalRefused, match="rights_evidence_invalid"):
            await do_import(InMemorySocialStore(), sign(envelope), caption=FB["inputs"]["post"]["caption"])


async def test_rights_are_accepted_while_valid_and_covered() -> None:
    _, rec = await imported(FB, FB_BINDING)
    assert rights_evidence_of(rec) == FB["envelope"]["rights_evidence"]
    assert rights_problems(rec, now=IMPORT_AT) == []
    assert rights_problems(rec, now=IMPORT_AT, platform="instagram_professional") == []
    require_rights(rec, now=IMPORT_AT)


async def _with(expires: str | None, platforms: list[str]) -> Any:
    envelope = copy.deepcopy(FB["envelope"])
    envelope["rights_evidence"][0].update({"expires": expires, "platforms": platforms})
    store = InMemorySocialStore()
    outcome = await do_import(store, sign(envelope), caption=FB["inputs"]["post"]["caption"])
    return outcome.record


async def test_expired_rights_are_refused_and_the_boundary_is_exclusive() -> None:
    soon = IMPORT_AT + timedelta(days=2)
    rec = await _with(soon.isoformat().replace("+00:00", "Z"), ["facebook_page"])
    assert rights_problems(rec, now=soon - timedelta(seconds=1)) == []
    for moment in (soon, soon + timedelta(days=1)):
        assert any("expired at" in p for p in rights_problems(rec, now=moment))
        with pytest.raises(ApprovalRefused, match="rights_expired"):
            require_rights(rec, now=moment)


async def test_a_platform_the_rights_do_not_cover_is_refused() -> None:
    rec = await _with(None, ["instagram_professional"])
    with pytest.raises(ApprovalRefused, match="rights_not_covered") as raised:
        require_rights(rec, now=IMPORT_AT)
    assert "do not cover facebook_page" in str(raised.value)
    require_rights(rec, now=IMPORT_AT, platform="instagram_professional")


async def test_a_handle_without_evidence_and_an_unreadable_expiry_are_refused() -> None:
    rec = await _with(None, ["facebook_page"])
    other = "sha256:" + "d" * 64
    assert any(other in p and "no rights evidence" in p for p in rights_problems(rec, now=IMPORT_AT, handles=[other]))
    broken = rec.model_copy(
        update={
            "origin": {
                **(rec.origin or {}),
                "rights_evidence": [{**FB["envelope"]["rights_evidence"][0], "expires": "soon"}],
            }
        }
    )
    assert any("not ISO 8601" in p for p in rights_problems(broken, now=IMPORT_AT))
    with pytest.raises(ValueError, match="timezone-aware"):
        rights_problems(rec, now=datetime(2026, 12, 3))  # noqa: DTZ001


async def test_a_record_without_media_needs_no_rights() -> None:
    rec = (await imported(FB, FB_BINDING))[1]
    text_only = rec.model_copy(update={"payload": {**rec.payload, "media": []}, "origin": None})
    assert rights_problems(text_only, now=datetime.now(UTC)) == []
