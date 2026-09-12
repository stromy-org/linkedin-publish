"""Contract rules that must hold before anything reaches the network."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from linkedin_publish import ArticleDraft, DocumentDraft, ImageDraft, MediaRef, PostDraft, PublishReceipt
from linkedin_publish.models import MAX_COMMENTARY_CODEPOINTS
from tests.conftest import ORG, PERSON, PNG_SHA

pytestmark = pytest.mark.unit


def test_blank_commentary_is_rejected() -> None:
    with pytest.raises(ValidationError, match="must not be blank"):
        PostDraft(author_urn=PERSON, commentary="   \n\t ")


def test_exactly_3000_code_points_is_accepted() -> None:
    draft = PostDraft(author_urn=PERSON, commentary="x" * MAX_COMMENTARY_CODEPOINTS)
    assert draft.commentary_length == 3000


def test_3001_code_points_is_rejected() -> None:
    with pytest.raises(ValidationError, match="3001 code points"):
        PostDraft(author_urn=PERSON, commentary="x" * 3001)


def test_length_counts_the_rendered_string_including_cta_and_hashtags() -> None:
    """The cap applies to the final submitted string, not to a `body` field.

    Runtime never appends a CTA or hashtags, so what is measured here is exactly
    what is sent — which is why a body that fits but a rendered post that does
    not is impossible by construction.
    """
    body = "x" * 2960
    rendered = f"{body}\n\nRead more: https://stromy.com.au #ai #orchestration"
    assert len(rendered) > MAX_COMMENTARY_CODEPOINTS
    with pytest.raises(ValidationError, match="code points"):
        PostDraft(author_urn=PERSON, commentary=rendered)


def test_emoji_counts_as_one_code_point_not_its_utf8_bytes() -> None:
    commentary = "🚀" * MAX_COMMENTARY_CODEPOINTS
    assert len(commentary.encode()) > MAX_COMMENTARY_CODEPOINTS
    assert PostDraft(author_urn=PERSON, commentary=commentary).commentary_length == 3000


@pytest.mark.parametrize(
    "urn",
    [
        "urn:li:digitalmediaAsset:xyz",
        "urn:li:share:12345",
        "not-a-urn",
        "urn:li:person:",
        "",
    ],
)
def test_author_must_be_a_person_or_organization_urn(urn: str) -> None:
    with pytest.raises(ValidationError):
        PostDraft(author_urn=urn, commentary="hello")


def test_organization_author_cannot_publish_to_connections() -> None:
    """CONNECTIONS describes a member's own network; a Page has none."""
    with pytest.raises(ValidationError, match="not available to an organization author"):
        PostDraft(author_urn=ORG, commentary="hello", visibility="CONNECTIONS")


def test_person_author_may_publish_to_connections() -> None:
    assert PostDraft(author_urn=PERSON, commentary="hi", visibility="CONNECTIONS").visibility == "CONNECTIONS"


def test_extra_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        PostDraft.model_validate(
            {"author_urn": PERSON, "commentary": "hi", "sponsored": True}
        )


def test_media_union_is_discriminated_by_kind() -> None:
    draft = PostDraft.model_validate(
        {
            "author_urn": PERSON,
            "commentary": "hi",
            "media": {"kind": "image", "asset": {"sha256": PNG_SHA}, "alt_text": "a chart"},
        }
    )
    assert isinstance(draft.media, ImageDraft)
    assert draft.required_capability == "image"


def test_unknown_media_kind_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PostDraft.model_validate(
            {"author_urn": PERSON, "commentary": "hi", "media": {"kind": "video", "asset": {"sha256": PNG_SHA}}}
        )


def test_carousel_is_not_a_media_kind() -> None:
    """An editorial "carousel" is a PDF document or it is refused by name.

    LinkedIn's Carousel API is a sponsored-content surface this library does not
    implement. Accepting the word here is what would let a deck silently ship as
    a text-only post.
    """
    with pytest.raises(ValidationError):
        PostDraft.model_validate(
            {"author_urn": PERSON, "commentary": "hi", "media": {"kind": "carousel", "asset": {"sha256": PNG_SHA}}}
        )
    document = DocumentDraft(asset=MediaRef(sha256=PNG_SHA), title="Q3 deck")
    assert PostDraft(author_urn=PERSON, commentary="hi", media=document).required_capability == "document"


def test_article_url_must_be_https() -> None:
    with pytest.raises(ValidationError, match="https"):
        ArticleDraft(url="http://stromy.com.au")  # type: ignore[arg-type]


def test_media_ref_requires_a_lowercase_sha256() -> None:
    with pytest.raises(ValidationError):
        MediaRef(sha256="A" * 64)
    with pytest.raises(ValidationError):
        MediaRef(sha256="abc")


def test_text_post_requires_the_text_capability() -> None:
    assert PostDraft(author_urn=PERSON, commentary="hi").required_capability == "text"


def test_receipt_requires_an_aware_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        PublishReceipt(
            publication_id="p1",
            post_urn="urn:li:share:1",
            permalink="https://www.linkedin.com/feed/update/urn:li:share:1/",
            adapter="share_ugc",
            published_at=datetime(2026, 9, 15, 7, 0),
        )


def test_receipt_normalizes_to_utc() -> None:
    receipt = PublishReceipt(
        publication_id="p1",
        post_urn="urn:li:share:1",
        permalink="https://www.linkedin.com/feed/update/urn:li:share:1/",
        adapter="share_ugc",
        published_at=datetime(2026, 9, 15, 9, 0, tzinfo=timezone(offset=__import__("datetime").timedelta(hours=2))),
    )
    assert receipt.published_at == datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc)
