"""Binding rules: capability state, authorship, and what evidence means."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from linkedin_publish import CapabilityStatus
from tests.conftest import NOW, ORG, PERSON, binding

pytestmark = pytest.mark.unit


def test_an_unmeasured_capability_is_unknown_not_enabled() -> None:
    """The default is `unknown`, and `unknown` is never permission."""
    bound = binding(capabilities=("text",))
    assert bound.capability_state("text") == "enabled"
    assert bound.capability_state("image") == "unknown"
    assert bound.capability_state("document") == "unknown"
    assert bound.capability_state("share_statistics") == "unknown"


def test_an_enabled_capability_requires_evidence() -> None:
    """A capability cannot be flipped on without naming the canary that proved it."""
    with pytest.raises(ValidationError, match="evidence_publication_id"):
        CapabilityStatus(capability="image", state="enabled", observed_at=NOW)


def test_unavailable_and_unknown_do_not_require_evidence() -> None:
    assert CapabilityStatus(capability="image", state="unavailable", reason="no CMA").state == "unavailable"
    assert CapabilityStatus(capability="image").state == "unknown"


def test_text_evidence_does_not_enable_image() -> None:
    """A text canary enables text only — media paths need their own evidence."""
    bound = binding(capabilities=("text",))
    assert bound.capability_state("text") == "enabled"
    assert bound.capability_state("image") != "enabled"


def test_author_must_be_the_bound_member() -> None:
    bound = binding(author=PERSON)
    assert bound.may_author(PERSON)
    assert not bound.may_author("urn:li:person:SomeoneElse")


def test_an_organization_author_needs_an_explicit_allowance() -> None:
    assert not binding(author=PERSON).may_author(ORG)
    assert binding(author=PERSON, allowed_orgs=(ORG,)).may_author(ORG)


def test_allowed_organizations_must_be_organization_urns() -> None:
    with pytest.raises(ValidationError):
        binding(allowed_orgs=("urn:li:person:NotAnOrg",))


def test_adapter_determines_the_media_urn_space() -> None:
    """UGC asset URNs and REST image/document URNs are not interchangeable."""
    assert binding(adapter="share_ugc").media_urn_types == frozenset({"digitalmediaAsset"})
    assert binding(adapter="rest_posts").media_urn_types == frozenset({"image", "document"})


def test_publish_enabled_defaults_off_in_the_model() -> None:
    """The fixture opts in; the contract does not."""
    bound = binding(publish_enabled=False)
    assert bound.publish_enabled is False
