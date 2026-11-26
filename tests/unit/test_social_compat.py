"""The LinkedIn surface is byte-identical after `social_publish` took ownership.

The digests below were computed with the v0.7.0 tag's own code (`git archive
v0.7.0`) over `tests/fixtures/publish-manifest.valid.json`. If any of them moves,
every stored LinkedIn approval in a live ledger stops matching its payload.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import linkedin_publish
import social_publish
from linkedin_publish.manifest import PublishManifest

pytestmark = [pytest.mark.unit]

FIXTURE = Path(__file__).parents[1] / "fixtures" / "publish-manifest.valid.json"

V070_MANIFEST_DIGEST = "b70630235cd00b697b4890a4d4627131e6f6590e0b700c3d94463f51302c8342"
V070_ENTRY_DIGESTS = {
    "2f6161370339": "faa4de640c3e2cd0baf4273b2f5b43948f5e83150c1d3422e387d17629a631a1",
    "8c41de0a1b77": "4e35049302891fad4aaee4266ada3fe5f30e3d67f43bf892495de899c67fbd83",
}

V070_DRAFT_SHA256 = [
    "c666871da2cda68b571405df30bb1c7c07f5668a4a02de9dd03378c257d180b1",
    "aba407bfc7bb983a5b712ac88347396604f03b5411d40e5b527115c12bc3e48a",
]

#: The table `linkedin_publish.store` declared literally through v0.7.0.
V070_TRANSITIONS = {
    "pending": {"claimed", "expired", "cancelled"},
    "claimed": {"pending", "sending", "cancelled"},
    "sending": {"published", "pending", "failed", "unknown"},
    "unknown": {"published", "failed"},
    "published": set(),
    "failed": set(),
    "expired": set(),
    "cancelled": set(),
}


def test_linkedin_manifest_and_entry_digests_are_the_v070_bytes() -> None:
    manifest = PublishManifest.from_json(FIXTURE.read_bytes())
    assert manifest.digest() == V070_MANIFEST_DIGEST
    assert {entry.post_id: entry.digest() for entry in manifest.publications} == V070_ENTRY_DIGESTS


def test_the_stored_draft_serialization_is_the_v070_bytes() -> None:
    """The jsonb `draft` column receives exactly these bytes (`upsert_publication`)."""
    manifest = PublishManifest.from_json(FIXTURE.read_bytes())
    drafts = [entry.as_draft().model_dump_json().encode() for entry in manifest.publications]
    assert [hashlib.sha256(d).hexdigest() for d in drafts] == V070_DRAFT_SHA256


def test_both_namespaces_share_one_canonical_encoding() -> None:
    from linkedin_publish import manifest

    assert manifest.canonical_json is social_publish.canonical_json
    assert manifest.canonical_digest is social_publish.canonical_digest
    assert linkedin_publish.canonical_digest is social_publish.canonical_digest
    # Non-ASCII is preserved, keys sorted, no whitespace: the ledger encoding.
    assert social_publish.canonical_json({"b": "é", "a": 1}) == '{"a":1,"b":"é"}'.encode()


def test_the_linkedin_transition_table_is_the_v070_table() -> None:
    from linkedin_publish.store import allowed_transitions

    assert {state: set(allowed_transitions(state)) for state in V070_TRANSITIONS} == V070_TRANSITIONS  # type: ignore[arg-type]


def test_store_conflict_and_the_sanitizer_are_one_object_in_both_namespaces() -> None:
    from linkedin_publish.errors import sanitize_detail as li_sanitize
    from social_publish.errors import sanitize_detail

    assert linkedin_publish.StoreConflict is social_publish.StoreConflict
    assert li_sanitize is sanitize_detail


def test_every_public_linkedin_import_still_resolves() -> None:
    for name in linkedin_publish.__all__:
        assert hasattr(linkedin_publish, name), name
    from social_publish import linkedin as compat

    for name in linkedin_publish.__all__:
        if name != "__version__":
            assert getattr(compat, name) is getattr(linkedin_publish, name), name


def test_a_meta_binding_never_carries_a_linkedin_urn() -> None:
    from pydantic import ValidationError

    from tests.social_fakes import meta_binding

    good = meta_binding()
    assert good.author_urn is None and good.native_account_id is not None
    with pytest.raises(ValidationError, match="never invented"):
        type(good).model_validate({**good.model_dump(), "author_urn": "urn:li:person:x"})
    with pytest.raises(ValidationError, match="numeric native_account_id"):
        type(good).model_validate({**good.model_dump(), "native_account_id": "urn:li:organization:1"})


def test_both_namespaces_report_the_released_version() -> None:
    import tomllib

    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())
    assert linkedin_publish.__version__ == social_publish.__version__ == pyproject["project"]["version"]
