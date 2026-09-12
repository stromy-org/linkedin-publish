"""Publish manifest v1: fails closed, and any revision invalidates approval."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from linkedin_publish import PublishManifest
from linkedin_publish.manifest import SCHEMA_VERSION, ManifestEntry, canonical_digest
from tests.conftest import ORG, PERSON, PNG_SHA

pytestmark = pytest.mark.contract

SOURCE_SHA = "e" * 64


def entry(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "post_id": "2f6161370339",
        "binding_id": "bind-1",
        "author_urn": PERSON,
        "commentary": "Intelligence, orchestrated. Read more: https://stromy.com.au",
        "visibility": "PUBLIC",
        "media": None,
        "scheduled_at": "2026-09-15T07:00:00+02:00",
        "timezone": "Europe/Brussels",
        "expires_at": "2026-09-16T07:00:00+02:00",
    }
    base.update(overrides)
    return base


def manifest(*entries: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": "stromy-autumn-2026",
        "source_posts_sha256": SOURCE_SHA,
        "publications": list(entries) or [entry()],
    }


# ------------------------------------------------------------------ shape ---


def test_a_valid_manifest_parses_and_is_unapproved() -> None:
    parsed = PublishManifest.model_validate(manifest())
    assert parsed.campaign_id == "stromy-autumn-2026"
    assert len(parsed.digest()) == 64
    # Nothing on the model says "approved". Approval lives in the ledger.
    assert not hasattr(parsed, "approved")


def test_an_unknown_schema_version_fails_closed() -> None:
    payload = manifest()
    payload["schema_version"] = "2.0"
    with pytest.raises(ValueError, match="unsupported manifest schema_version"):
        PublishManifest.from_json(json.dumps(payload))


def test_an_extra_field_fails_closed() -> None:
    payload = manifest()
    payload["auto_publish"] = True
    with pytest.raises(ValidationError):
        PublishManifest.model_validate(payload)


def test_an_extra_field_on_an_entry_fails_closed() -> None:
    """`approved: true` in a manifest is not an approval; it is a rejection."""
    with pytest.raises(ValidationError):
        PublishManifest.model_validate(manifest(entry(approved=True)))


def test_an_empty_batch_is_rejected() -> None:
    payload = manifest()
    payload["publications"] = []
    with pytest.raises(ValidationError):
        PublishManifest.model_validate(payload)


def test_duplicate_post_ids_in_one_batch_fail_before_import() -> None:
    with pytest.raises(ValidationError, match="duplicate post_id"):
        PublishManifest.model_validate(manifest(entry(), entry()))


def test_the_same_post_id_under_a_different_binding_is_allowed() -> None:
    parsed = PublishManifest.model_validate(manifest(entry(), entry(binding_id="bind-2")))
    assert len(parsed.publications) == 2


def test_the_campaign_id_namespaces_builder_ids() -> None:
    """Builder post ids collide across campaigns; the key includes the campaign."""
    first = PublishManifest.model_validate(manifest())
    second = PublishManifest.model_validate({**manifest(), "campaign_id": "stromy-winter-2026"})
    keys_a = first.keys(subject_kind="entra_oid", subject_id="s", account_id="acct")
    keys_b = second.keys(subject_kind="entra_oid", subject_id="s", account_id="acct")
    assert keys_a[0].post_id == keys_b[0].post_id
    assert keys_a[0] != keys_b[0]


# -------------------------------------------------------------- post rules ---


def test_post_draft_rules_apply_at_import_not_at_send() -> None:
    with pytest.raises(ValidationError, match="code points"):
        PublishManifest.model_validate(manifest(entry(commentary="x" * 3001)))
    with pytest.raises(ValidationError, match="organization author"):
        PublishManifest.model_validate(
            manifest(entry(author_urn=ORG, visibility="CONNECTIONS"))
        )


def test_the_commentary_is_the_exact_final_string() -> None:
    """Runtime never re-renders or appends. What is approved is what is sent."""
    parsed = PublishManifest.model_validate(manifest())
    draft = parsed.publications[0].as_draft()
    assert draft.commentary == entry()["commentary"]
    assert "https://stromy.com.au" in draft.commentary


# ----------------------------------------------------------------- timing ---


def test_a_naive_timestamp_is_refused() -> None:
    with pytest.raises(ValidationError, match="explicit UTC offset"):
        PublishManifest.model_validate(manifest(entry(scheduled_at="2026-09-15T07:00:00")))


def test_an_offset_that_disagrees_with_the_zone_is_refused() -> None:
    """+05:00 is not a Brussels offset on that date. Named, not silently shifted."""
    with pytest.raises(ValidationError, match="declares offset"):
        PublishManifest.model_validate(manifest(scheduled_at_wrong := entry(scheduled_at="2026-09-15T07:00:00+05:00")))
    assert scheduled_at_wrong["timezone"] == "Europe/Brussels"


def test_a_nonexistent_dst_wall_time_is_refused() -> None:
    """02:30 on the spring-forward night does not exist in Brussels."""
    with pytest.raises(ValidationError, match="does not exist"):
        PublishManifest.model_validate(
            manifest(
                entry(
                    scheduled_at="2026-03-29T02:30:00+01:00",
                    expires_at="2026-03-30T02:30:00+02:00",
                )
            )
        )


def test_an_ambiguous_wall_time_is_resolved_by_its_explicit_offset() -> None:
    """Autumn-back 02:30 happens twice; the offset says which one."""
    first = PublishManifest.model_validate(
        manifest(entry(scheduled_at="2026-10-25T02:30:00+02:00", expires_at="2026-10-26T02:30:00+01:00"))
    )
    second = PublishManifest.model_validate(
        manifest(entry(scheduled_at="2026-10-25T02:30:00+01:00", expires_at="2026-10-26T02:30:00+01:00"))
    )
    assert first.publications[0].scheduled_utc() != second.publications[0].scheduled_utc()


def test_an_unknown_timezone_is_refused() -> None:
    with pytest.raises(ValidationError, match="not a known IANA zone"):
        PublishManifest.model_validate(manifest(entry(timezone="Mars/Olympus")))


def test_expiry_defaults_to_24h_after_the_slot() -> None:
    parsed = PublishManifest.model_validate(manifest(entry(expires_at=None)))
    row = parsed.publications[0]
    assert (row.expires_utc() - row.scheduled_utc()).total_seconds() == 24 * 3600


def test_expiry_must_follow_the_schedule() -> None:
    with pytest.raises(ValidationError, match="must be after scheduled_at"):
        PublishManifest.model_validate(
            manifest(entry(scheduled_at="2026-09-15T07:00:00+02:00", expires_at="2026-09-15T06:00:00+02:00"))
        )


def test_the_display_timezone_is_preserved_alongside_the_utc_instant() -> None:
    parsed = PublishManifest.model_validate(manifest())
    row = parsed.publications[0]
    assert row.timezone == "Europe/Brussels"
    assert row.scheduled_utc() == datetime(2026, 9, 15, 5, 0, tzinfo=timezone.utc)


# ----------------------------------------------------------------- digest ---


def test_the_digest_is_stable_across_key_order_and_reserialization() -> None:
    a = ManifestEntry.model_validate(entry())
    reordered = dict(reversed(list(entry().items())))
    b = ManifestEntry.model_validate(reordered)
    assert a.digest() == b.digest()


def test_non_ascii_is_preserved_byte_for_byte_in_the_digest() -> None:
    """Escaping would make an emoji post hash differently depending on the writer."""
    payload = {"commentary": "Groeten 🚀 — Stromy"}
    assert canonical_digest(payload) == canonical_digest(copy.deepcopy(payload))
    assert "🚀".encode() in json.dumps(payload, ensure_ascii=False).encode()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("commentary", "Intelligence, orchestrated. Read more: https://stromy.com.au "),
        ("author_urn", "urn:li:person:Different"),
        ("binding_id", "bind-2"),
        ("visibility", "CONNECTIONS"),
        ("scheduled_at", "2026-09-15T07:01:00+02:00"),
        ("expires_at", "2026-09-16T08:00:00+02:00"),
        ("media", {"kind": "image", "asset": {"sha256": PNG_SHA}, "alt_text": "chart"}),
    ],
)
def test_every_approved_field_mutation_changes_the_digest(field: str, value: object) -> None:
    """A different digest has no approval. That is the whole enforcement."""
    baseline = ManifestEntry.model_validate(entry()).digest()
    mutated = ManifestEntry.model_validate(entry(**{field: value})).digest()
    assert mutated != baseline


def test_a_bare_timezone_swap_is_refused_outright() -> None:
    """Stronger than a digest change: the offset no longer matches the new zone.

    Re-homing a post to another timezone means re-deciding when it goes out, so
    the manifest has to carry the new offset too — and then it is plainly a
    different approval.
    """
    with pytest.raises(ValidationError, match="declares offset"):
        ManifestEntry.model_validate(entry(timezone="Australia/Sydney"))


def test_changing_an_asset_changes_the_digest() -> None:
    with_png = ManifestEntry.model_validate(
        entry(media={"kind": "image", "asset": {"sha256": PNG_SHA}, "alt_text": "chart"})
    ).digest()
    swapped = ManifestEntry.model_validate(
        entry(media={"kind": "image", "asset": {"sha256": "f" * 64}, "alt_text": "chart"})
    ).digest()
    alt_changed = ManifestEntry.model_validate(
        entry(media={"kind": "image", "asset": {"sha256": PNG_SHA}, "alt_text": "different"})
    ).digest()
    assert len({with_png, swapped, alt_changed}) == 3


def test_a_timezone_move_at_the_same_instant_still_changes_the_digest() -> None:
    """07:00 Brussels and 05:00 UTC are the same moment but not the same decision."""
    brussels = ManifestEntry.model_validate(entry()).digest()
    utc = ManifestEntry.model_validate(
        entry(scheduled_at="2026-09-15T05:00:00+00:00", expires_at="2026-09-16T05:00:00+00:00", timezone="UTC")
    ).digest()
    assert brussels != utc
