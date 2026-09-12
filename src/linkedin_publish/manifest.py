"""Publish manifest v1 — the editorial → publishing handoff.

`posts.json` stays exactly as it is: a draft editorial artifact with
week-relative schedules and display-name authors. It has no approval and no
absolute time, and reinterpreting it as one is how a calendar sign-off silently
becomes permission to post. This manifest is a *separate*, additive contract
that carries the exact bytes an operator approved.

Everything here fails closed. An unknown `schema_version`, an extra field, a
naive timestamp, a `first_comment` link, a nonexistent DST wall time, a
duplicate key inside one batch — all refusals with the offending row named.

The digest covers every submitted field. Any revision — a character of
commentary, a different binding, a minute of schedule, a swapped asset — yields
a different digest, and a different digest has no approval.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ._json import loads_object
from .models import MediaDraft, PostDraft, Visibility
from .store import PublicationKey

__all__ = [
    "ManifestEntry",
    "PublishManifest",
    "SCHEMA_VERSION",
    "canonical_digest",
    "canonical_json",
    "payload_digest",
]

SCHEMA_VERSION = "1.0"

#: Default approval window: a post that did not go out within a day of its slot
#: needs a fresh decision, not a silent late publication.
DEFAULT_EXPIRY = timedelta(hours=24)


def canonical_json(payload: object) -> bytes:
    """One canonical serialization, used for every digest in this system.

    Sorted keys, no insignificant whitespace, UTF-8, non-ASCII preserved.
    Preserving non-ASCII matters: escaping it would make two byte-identical
    posts with an emoji hash differently depending on the writer.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def canonical_digest(payload: object) -> str:
    """SHA-256 over `canonical_json(payload)`, lowercase hex."""
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def _resolve_wall_time(value: datetime, tz_name: str, *, field_name: str) -> datetime:
    """Validate an aware timestamp against its declared IANA zone.

    Three separate refusals, because they have three different fixes:

    * naive input — no offset at all;
    * an offset that the zone never has at that instant (a nonexistent DST wall
      time, or simply the wrong offset);
    * an ambiguous wall time whose offset matches neither side of the fold.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must carry an explicit UTC offset")

    try:
        zone = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"timezone {tz_name!r} is not a known IANA zone") from exc

    wall = value.replace(tzinfo=None)
    candidates = {
        wall.replace(tzinfo=zone, fold=0).utcoffset(),
        wall.replace(tzinfo=zone, fold=1).utcoffset(),
    }

    # A nonexistent wall time (the hour that DST skips) is one zoneinfo silently
    # normalises. Detect it by round-tripping through UTC and comparing walls.
    localized = wall.replace(tzinfo=zone, fold=0)
    if localized.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) != wall:
        raise ValueError(f"{field_name} {wall.isoformat()} does not exist in {tz_name} (DST transition)")

    declared = value.utcoffset()
    if declared not in candidates:
        offered = ", ".join(sorted(str(candidate) for candidate in candidates))
        raise ValueError(
            f"{field_name} declares offset {declared} but {tz_name} uses {offered} at that wall time"
        )
    return value.astimezone(timezone.utc)


class ManifestEntry(BaseModel):
    """One publication, exactly as it will be submitted."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    post_id: Annotated[str, Field(min_length=1, max_length=128)]
    binding_id: Annotated[str, Field(min_length=1, max_length=128)]
    author_urn: str
    commentary: str
    visibility: Visibility = "PUBLIC"
    media: MediaDraft | None = None
    scheduled_at: datetime
    timezone: Annotated[str, Field(min_length=1, max_length=64)]
    expires_at: datetime | None = None

    @model_validator(mode="after")
    def _times(self) -> Self:
        scheduled_utc = _resolve_wall_time(self.scheduled_at, self.timezone, field_name="scheduled_at")
        if self.expires_at is None:
            object.__setattr__(self, "expires_at", self.scheduled_at + DEFAULT_EXPIRY)
        else:
            expires_utc = _resolve_wall_time(self.expires_at, self.timezone, field_name="expires_at")
            if expires_utc <= scheduled_utc:
                raise ValueError("expires_at must be after scheduled_at")
        return self

    @model_validator(mode="after")
    def _draft_is_valid(self) -> Self:
        # Constructing the draft here means every rule in `PostDraft` — the 3000
        # code point cap over the final string, the organization/CONNECTIONS
        # refusal, URN resource types — applies at import, not at send.
        self.as_draft()
        return self

    def as_draft(self) -> PostDraft:
        """The submitted post. Runtime never re-renders or appends to this."""
        return PostDraft(
            author_urn=self.author_urn,
            commentary=self.commentary,
            visibility=self.visibility,
            media=self.media,
        )

    def scheduled_utc(self) -> datetime:
        return self.scheduled_at.astimezone(timezone.utc)

    def expires_utc(self) -> datetime:
        assert self.expires_at is not None  # noqa: S101 - set by _times
        return self.expires_at.astimezone(timezone.utc)

    def digest_payload(self) -> dict[str, object]:
        """Every field the approval covers, normalized.

        The UTC instant *and* the display timezone are both included: moving a
        post between zones without changing the instant is still an editorial
        change an operator approved or did not.
        """
        media: dict[str, object] | None = None
        if self.media is not None:
            media = loads_object(self.media.model_dump_json())
        return {
            "post_id": self.post_id,
            "binding_id": self.binding_id,
            "author_urn": self.author_urn,
            "commentary": self.commentary,
            "visibility": self.visibility,
            "media": media,
            "scheduled_at_utc": self.scheduled_utc().isoformat(),
            "expires_at_utc": self.expires_utc().isoformat(),
            "timezone": self.timezone,
        }

    def digest(self) -> str:
        return canonical_digest(self.digest_payload())


class PublishManifest(BaseModel):
    """A reviewed batch. Unapproved until the trusted writer says otherwise."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"]
    campaign_id: Annotated[str, Field(min_length=1, max_length=128)]
    source_posts_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    publications: Annotated[list[ManifestEntry], Field(min_length=1)]

    @field_validator("publications")
    @classmethod
    def _unique_post_ids(cls, value: list[ManifestEntry]) -> list[ManifestEntry]:
        seen: set[tuple[str, str]] = set()
        for entry in value:
            key = (entry.post_id, entry.binding_id)
            if key in seen:
                raise ValueError(
                    f"duplicate post_id {entry.post_id!r} for binding {entry.binding_id!r} in one batch"
                )
            seen.add(key)
        return value

    def digest(self) -> str:
        """Digest over the whole manifest, entries included."""
        return canonical_digest(
            {
                "schema_version": self.schema_version,
                "campaign_id": self.campaign_id,
                "source_posts_sha256": self.source_posts_sha256,
                "publications": [entry.digest_payload() for entry in self.publications],
            }
        )

    def keys(self, *, subject_kind: str, subject_id: str, account_id: str) -> list[PublicationKey]:
        """The natural keys this manifest would occupy."""
        return [
            PublicationKey(
                subject_kind=subject_kind,  # type: ignore[arg-type]
                subject_id=subject_id,
                campaign_id=self.campaign_id,
                post_id=entry.post_id,
                account_id=account_id,
            )
            for entry in self.publications
        ]

    @classmethod
    def from_json(cls, raw: bytes | str) -> PublishManifest:
        """Parse manifest bytes, failing closed on an unknown version."""
        payload = loads_object(raw)
        if not payload:
            raise ValueError("manifest must be a non-empty JSON object")
        version = payload.get("schema_version")
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported manifest schema_version {version!r}; this build accepts {SCHEMA_VERSION!r}"
            )
        return cls.model_validate(payload)


def payload_digest(entry: ManifestEntry) -> str:
    """The digest an approval is minted over. One definition, used everywhere."""
    return entry.digest()
