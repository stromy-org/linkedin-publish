"""LinkedIn URN parsing and resource-type validation.

A URN is never accepted as an arbitrary string. Every field that carries one
declares the resource types it will accept, so a document URN can never land in
an author slot and an app-scoped person URN can never be mistaken for an
organization. Parsing is total: either a `Urn` comes back or `UrnError` is
raised naming the exact reason.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final, Literal

__all__ = [
    "ACTIVITY",
    "AUTHOR_TYPES",
    "DIGITAL_MEDIA_ASSET",
    "DOCUMENT",
    "IMAGE",
    "ORGANIZATION",
    "PERSON",
    "REST_MEDIA_TYPES",
    "SHARE",
    "UGC_MEDIA_TYPES",
    "UGC_POST",
    "Urn",
    "UrnError",
    "is_organization",
    "is_person",
    "parse_urn",
    "validate_urn",
]

PERSON: Final = "person"
ORGANIZATION: Final = "organization"
DIGITAL_MEDIA_ASSET: Final = "digitalmediaAsset"
IMAGE: Final = "image"
DOCUMENT: Final = "document"
SHARE: Final = "share"
UGC_POST: Final = "ugcPost"
ACTIVITY: Final = "activity"

#: Resource types accepted in an author slot.
AUTHOR_TYPES: Final[frozenset[str]] = frozenset({PERSON, ORGANIZATION})

#: Media URNs the legacy UGC surface issues and accepts.
UGC_MEDIA_TYPES: Final[frozenset[str]] = frozenset({DIGITAL_MEDIA_ASSET})

#: Media URNs the versioned REST surface issues and accepts.
REST_MEDIA_TYPES: Final[frozenset[str]] = frozenset({IMAGE, DOCUMENT})

#: Resource types a published post can be identified by.
POST_TYPES: Final[frozenset[str]] = frozenset({SHARE, UGC_POST, ACTIVITY})

# LinkedIn ids are opaque; they are alphanumeric with `_` and `-` in practice.
# The length bound is a denial-of-service guard, not a documented provider limit.
_URN_RE: Final = re.compile(r"^urn:li:(?P<resource>[A-Za-z][A-Za-z0-9]{0,63}):(?P<id>[A-Za-z0-9_\-]{1,128})$")


class UrnError(ValueError):
    """A string is not a URN of an accepted resource type."""


@dataclass(frozen=True, slots=True)
class Urn:
    """A parsed `urn:li:<resource>:<id>`."""

    resource: str
    id: str

    def __str__(self) -> str:
        return f"urn:li:{self.resource}:{self.id}"


def parse_urn(value: str) -> Urn:
    """Parse `value`, raising `UrnError` when it is not a well-formed LinkedIn URN."""
    match = _URN_RE.match(value)
    if match is None:
        raise UrnError(f"not a LinkedIn URN: {value!r}")
    return Urn(resource=match.group("resource"), id=match.group("id"))


def validate_urn(value: str, accepted: frozenset[str] | set[str], *, field: str) -> Urn:
    """Parse `value` and assert its resource type is one of `accepted`.

    `field` names the slot in the error so a rejection identifies which input was
    wrong rather than only that something was.
    """
    urn = parse_urn(value)
    if urn.resource not in accepted:
        expected = ", ".join(sorted(accepted))
        raise UrnError(f"{field}: expected a {expected} URN, got {urn.resource!r} ({value!r})")
    return urn


def is_person(value: str) -> bool:
    """True when `value` is a person URN."""
    try:
        return parse_urn(value).resource == PERSON
    except UrnError:
        return False


def is_organization(value: str) -> bool:
    """True when `value` is an organization URN."""
    try:
        return parse_urn(value).resource == ORGANIZATION
    except UrnError:
        return False


AuthorKind = Literal["person", "organization"]


def author_kind(value: str) -> AuthorKind:
    """Return the author kind for an author URN, raising `UrnError` otherwise."""
    urn = validate_urn(value, AUTHOR_TYPES, field="author_urn")
    return "person" if urn.resource == PERSON else "organization"
