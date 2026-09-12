"""Public contracts (C0).

These models are the boundary the MCP, the workflow and the CLI all speak. They
are strict on purpose: `extra='forbid'` everywhere, discriminated media unions,
URNs validated by resource type, and every rejection carrying the field that
caused it.

Two invariants are enforced here rather than downstream, because downstream is
too late to decline cheaply:

* An organization author cannot publish to CONNECTIONS — that visibility only
  describes a member's own network.
* `commentary` is measured in Unicode code points over the *final rendered*
  string, CTA and hashtags included. Runtime never appends to it, so what is
  counted here is exactly what is submitted.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

from .urns import AUTHOR_TYPES, REST_MEDIA_TYPES, UGC_MEDIA_TYPES, UrnError, author_kind, validate_urn

__all__ = [
    "AccountBinding",
    "DeleteOutcome",
    "PostSnapshot",
    "Adapter",
    "ArticleDraft",
    "Capability",
    "CapabilityState",
    "CapabilityStatus",
    "DocumentDraft",
    "ImageDraft",
    "MediaDraft",
    "MediaRef",
    "PostDraft",
    "PublishReceipt",
    "ShareStatistics",
    "ShareStatisticsPoint",
    "SubjectKind",
    "Visibility",
]

#: Maximum commentary length in Unicode code points, per the Share/Posts contract.
MAX_COMMENTARY_CODEPOINTS = 3000

#: Maximum alt text length. A provider limit to recheck at commissioning.
MAX_ALT_TEXT = 300

Adapter = Literal["share_ugc", "rest_posts"]
Visibility = Literal["PUBLIC", "CONNECTIONS"]
SubjectKind = Literal["entra_oid", "client_slug", "service"]

Capability = Literal[
    "text",
    "article",
    "image",
    "document",
    "delete",
    "read_post",
    "share_statistics",
]

#: `unknown` is the default and is never treated as permission. A capability
#: becomes `enabled` only from recorded commissioning evidence of its own shape;
#: a text canary enables `text` and nothing else.
CapabilityState = Literal["unknown", "unavailable", "enabled"]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=False)


class MediaRef(_Strict):
    """A handle to bytes that already exist in the org asset store.

    A SHA-256 is an *identifier*, not an authorization: `media.py` still asks the
    injected asset reader whether this subject may read it. Hosted tools accept
    this shape only — never a local path, a download URL, or inline base64.
    """

    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    filename: Annotated[str, Field(min_length=1, max_length=255)] | None = None


class ImageDraft(_Strict):
    """A single image attached to a post."""

    kind: Literal["image"] = "image"
    asset: MediaRef
    alt_text: Annotated[str, Field(min_length=1, max_length=MAX_ALT_TEXT)] | None = None


class ArticleDraft(_Strict):
    """A link preview. The URL is what LinkedIn renders, not a redirect we own."""

    kind: Literal["article"] = "article"
    url: HttpUrl
    title: Annotated[str, Field(min_length=1, max_length=400)] | None = None
    description: Annotated[str, Field(min_length=1, max_length=4086)] | None = None
    thumbnail: MediaRef | None = None

    @field_validator("url")
    @classmethod
    def _https_only(cls, value: HttpUrl) -> HttpUrl:
        if value.scheme != "https":
            raise ValueError("article url must be https")
        return value


class DocumentDraft(_Strict):
    """A PDF document post.

    This is the *only* representation of an editorial "carousel". LinkedIn's
    Carousel API is a sponsored-content surface this library does not implement,
    so a carousel either arrives as an approved PDF here or is refused by name —
    it never degrades into a text-only post.
    """

    kind: Literal["document"] = "document"
    asset: MediaRef
    title: Annotated[str, Field(min_length=1, max_length=100)]


MediaDraft = Annotated[ImageDraft | ArticleDraft | DocumentDraft, Field(discriminator="kind")]


class PostDraft(_Strict):
    """One post, exactly as it will be submitted."""

    author_urn: str
    commentary: str
    visibility: Visibility = "PUBLIC"
    media: MediaDraft | None = None

    @field_validator("author_urn")
    @classmethod
    def _author(cls, value: str) -> str:
        try:
            validate_urn(value, AUTHOR_TYPES, field="author_urn")
        except UrnError as exc:
            raise ValueError(str(exc)) from exc
        return value

    @field_validator("commentary")
    @classmethod
    def _commentary(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("commentary must not be blank")
        length = len(value)
        if length > MAX_COMMENTARY_CODEPOINTS:
            raise ValueError(
                f"commentary is {length} code points; the maximum is {MAX_COMMENTARY_CODEPOINTS}"
            )
        return value

    @model_validator(mode="after")
    def _visibility_matches_author(self) -> Self:
        if self.visibility == "CONNECTIONS" and author_kind(self.author_urn) == "organization":
            raise ValueError("CONNECTIONS visibility is not available to an organization author")
        return self

    @property
    def commentary_length(self) -> int:
        """Length of the final rendered string in Unicode code points."""
        return len(self.commentary)

    @property
    def required_capability(self) -> Capability:
        """The capability this draft needs enabled on its binding."""
        if self.media is None:
            return "text"
        return self.media.kind


class PublishReceipt(_Strict):
    """Proof that one publication reached LinkedIn."""

    publication_id: str
    post_urn: str
    permalink: str
    adapter: Adapter
    published_at: datetime

    @field_validator("published_at")
    @classmethod
    def _aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")
        return value.astimezone(timezone.utc)


class CapabilityStatus(_Strict):
    """What commissioning proved about one capability of one binding."""

    capability: Capability
    state: CapabilityState = "unknown"
    observed_at: datetime | None = None
    #: The publication id of the canary that proved it, when state is `enabled`.
    evidence_publication_id: str | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def _enabled_needs_evidence(self) -> Self:
        if self.state == "enabled" and not self.evidence_publication_id:
            raise ValueError("an enabled capability requires evidence_publication_id")
        return self


class AccountBinding(_Strict):
    """Trusted server-side configuration for one app/account pair.

    This is never assembled from a caller's arguments. It is loaded from the
    store by opaque `binding_id` after an access check, which is why a tool
    argument cannot widen what a token may do.

    `account_id` is the stable logical identity that survives token rotation and
    a verified migration between apps; `author_urn` is app-scoped and does not.
    Changing the underlying member requires a new account and fresh approvals.
    """

    binding_id: str
    account_id: str
    subject_kind: SubjectKind
    subject_id: str
    app_id: str
    author_urn: str
    allowed_organization_urns: tuple[str, ...] = ()
    adapter: Adapter
    declared_scopes: tuple[str, ...] = ()
    observed_scopes: tuple[str, ...] = ()
    credential_ref: str
    credential_version: str
    token_expires_at: datetime | None = None
    token_observed_at: datetime | None = None
    capabilities: tuple[CapabilityStatus, ...] = ()
    publish_enabled: bool = False

    @field_validator("author_urn")
    @classmethod
    def _author(cls, value: str) -> str:
        try:
            validate_urn(value, AUTHOR_TYPES, field="author_urn")
        except UrnError as exc:
            raise ValueError(str(exc)) from exc
        return value

    @field_validator("allowed_organization_urns")
    @classmethod
    def _orgs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for urn in value:
            try:
                validate_urn(urn, {"organization"}, field="allowed_organization_urns")
            except UrnError as exc:
                raise ValueError(str(exc)) from exc
        return value

    @property
    def media_urn_types(self) -> frozenset[str]:
        """Media URN resource types this binding's adapter issues and accepts."""
        return UGC_MEDIA_TYPES if self.adapter == "share_ugc" else REST_MEDIA_TYPES

    def capability_state(self, capability: Capability) -> CapabilityState:
        """State of `capability`, defaulting to `unknown` when never measured."""
        for status in self.capabilities:
            if status.capability == capability:
                return status.state
        return "unknown"

    def may_author(self, author_urn: str) -> bool:
        """Whether this binding is allowed to publish as `author_urn`.

        A person author must be the binding's own expected member — one
        app/member binding per deployed process is the pilot limit, so an author
        argument never selects an account.
        """
        if author_urn == self.author_urn:
            return True
        return author_urn in self.allowed_organization_urns


class PostSnapshot(_Strict):
    """What a provider read returned about one post.

    Deliberately thin. This is not a mirror of LinkedIn's response — the fields
    here are the ones an operator needs to confirm a post exists and says what
    they approved. `raw_lifecycle_state` is kept verbatim because its vocabulary
    differs between the two products and normalising it would lose the
    distinction.
    """

    post_urn: str
    author_urn: str | None = None
    commentary: str | None = None
    visibility: str | None = None
    raw_lifecycle_state: str | None = None
    permalink: str | None = None


class DeleteOutcome(_Strict):
    """The provider's actual answer to a delete.

    `existed` is tri-state on purpose. A 404 means LinkedIn has no such post
    *for this credential*, which is not the same as "deleted" — reporting 204 for
    every 404 would turn "you never had access to it" into "it is gone".
    """

    post_urn: str
    deleted: bool
    existed: bool | None
    http_status: int
    reason: str | None = None


class ShareStatisticsPoint(_Strict):
    """Organic statistics for one time bucket.

    Every metric is `int | None`. `None` means the provider did not report it,
    and `reason` says so — it is never coerced to 0, because a measured zero and
    an absent metric lead to different conclusions. Negative counts are also
    preserved verbatim: LinkedIn does emit them (a retraction inside the bucket),
    and clamping them to zero would silently inflate a report.
    """

    start: datetime
    end: datetime
    impressions: int | None = None
    unique_impressions: int | None = None
    clicks: int | None = None
    likes: int | None = None
    comments: int | None = None
    shares: int | None = None
    engagement: float | None = None
    reason: str | None = None


class ShareStatistics(_Strict):
    """A bounded organic-statistics read, with its provenance attached."""

    organization_urn: str
    granularity: Literal["DAY", "MONTH"]
    start: datetime
    end: datetime
    source: str = "organizationalEntityShareStatistics"
    observed_at: datetime
    points: tuple[ShareStatisticsPoint, ...] = ()
    reason: str | None = None
