"""Platform-neutral contracts: bindings, capabilities, the Meta payload, ledger records.

**Capability is declared per account, operation and format.** A binding carries
one `OperationCapability` per (operation, format) pair it has commissioned; an
absent pair is `unknown`, and `unknown` is never permission. The core asks the
binding about the *exact* pair an intent needs. It never looks for a "nearby"
format, so a carousel cannot degrade into an image and an image cannot degrade
into text.

**Native identifiers stay native.** A LinkedIn binding carries an `author_urn`; a
Meta binding carries the numeric Page or Instagram user id in
`native_account_id`. Neither is ever synthesized from the other: there is no
LinkedIn URN for a Meta account.

**The Meta payload is versioned** (`social-publish-meta/1`) and carries the
account identity, the action and, for a reply, the parent target (post, comment
and the comment version the approver saw). Its digest is the canonical digest of
its JSON form, so any change to caption bytes, media order, target, slot or
action is a different digest with no approval.
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .canonical import canonical_digest
from .states import SocialState

__all__ = [
    "META_PAYLOAD_VERSION",
    "MetaAccount",
    "MetaParent",
    "MetaPayload",
    "OperationCapability",
    "Platform",
    "SocialBinding",
    "SocialKey",
    "SocialRecord",
    "meta_payload_digest",
]

Platform = Literal["linkedin", "facebook_page", "instagram_professional"]
META_PLATFORMS: frozenset[str] = frozenset({"facebook_page", "instagram_professional"})
AdapterName = Literal["share_ugc", "rest_posts", "meta_graph"]
CapabilityState = Literal["unknown", "unavailable", "enabled"]
SubjectKind = Literal["entra_oid", "client_slug", "service"]

META_PAYLOAD_VERSION = "social-publish-meta/1"

_META_ID = re.compile(r"^[0-9]{1,32}$")
_LINKEDIN_URN = re.compile(r"^urn:li:(person|organization):[A-Za-z0-9_\-]+$")
_HANDLE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TOKEN = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=False)


def _aware(value: datetime | None, name: str) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


class OperationCapability(_Strict):
    """What commissioning proved about one (operation, format) on one binding."""

    operation: _TOKEN
    format: _TOKEN
    state: CapabilityState = "unknown"
    observed_at: datetime | None = None
    #: The ledger publication that proved it, when `enabled`.
    evidence_publication_id: str | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def _enabled_needs_evidence(self) -> Self:
        if self.state == "enabled" and not self.evidence_publication_id:
            raise ValueError("an enabled capability requires evidence_publication_id")
        return self


class SocialBinding(_Strict):
    """Trusted server-side configuration for one provider account.

    Loaded from the store by opaque id after an access check, never assembled
    from a caller's arguments. The adapter is selected from `adapter` here, never
    from a payload or from a previous failure.
    """

    binding_id: Annotated[str, Field(min_length=1, max_length=128)]
    account_id: Annotated[str, Field(min_length=1, max_length=128)]
    subject_kind: SubjectKind
    subject_id: str
    app_id: str
    platform: Platform
    adapter: AdapterName
    #: The provider's own account id: a Facebook Page id or an Instagram user id.
    native_account_id: str | None = None
    #: LinkedIn only: the app-scoped author URN.
    author_urn: str | None = None
    credential_ref: str
    credential_version: str
    token_expires_at: datetime | None = None
    token_observed_at: datetime | None = None
    capabilities: tuple[OperationCapability, ...] = ()
    publish_enabled: bool = False

    @model_validator(mode="after")
    def _native_identity(self) -> Self:
        if self.platform == "linkedin":
            if self.adapter not in ("share_ugc", "rest_posts"):
                raise ValueError("a linkedin binding uses the share_ugc or rest_posts adapter")
            if self.author_urn is None or not _LINKEDIN_URN.match(self.author_urn):
                raise ValueError("a linkedin binding needs a LinkedIn person or organization author_urn")
            if self.native_account_id is not None:
                raise ValueError("a linkedin binding has no native_account_id; its identity is author_urn")
        else:
            if self.adapter != "meta_graph":
                raise ValueError(f"a {self.platform} binding uses the meta_graph adapter")
            if self.author_urn is not None:
                raise ValueError("a Meta binding has no author_urn; LinkedIn URNs are never invented for it")
            if self.native_account_id is None or not _META_ID.match(self.native_account_id):
                raise ValueError("a Meta binding needs a numeric native_account_id (Page or Instagram user id)")
        return self

    def capability_state(self, operation: str, format: str) -> CapabilityState:  # noqa: A002 - domain word
        """State of exactly (operation, format); `unknown` when never measured."""
        for status in self.capabilities:
            if status.operation == operation and status.format == format:
                return status.state
        return "unknown"


class MetaAccount(_Strict):
    account_id: Annotated[str, Field(min_length=1, max_length=128)]
    native_account_id: Annotated[str, Field(pattern=r"^[0-9]{1,32}$")]


class MetaParent(_Strict):
    """The object a reply answers: the post, the comment, and the comment version approved."""

    post_id: Annotated[str, Field(min_length=1, max_length=128)]
    comment_id: Annotated[str, Field(min_length=1, max_length=128)]
    comment_version: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class MetaPayload(_Strict):
    """One Meta operation, exactly as it will be submitted."""

    schema_version: Literal["social-publish-meta/1"] = "social-publish-meta/1"
    platform: Literal["facebook_page", "instagram_professional"]
    action: Literal["schedule_post", "publish_post", "reply"]
    format: _TOKEN
    account: MetaAccount
    parent: MetaParent | None = None
    caption: Annotated[str, Field(min_length=1, max_length=63206)]
    caption_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    media: tuple[str, ...] = ()
    alt_text: tuple[str | None, ...] = ()
    links: tuple[str, ...] = ()
    ai_label: bool = False
    visibility: Literal["public"] = "public"
    slot_utc: datetime | None = None
    #: The C3 approval envelope this payload was imported from, when it was.
    envelope_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None = None

    @field_validator("slot_utc")
    @classmethod
    def _slot_aware(cls, value: datetime | None) -> datetime | None:
        return _aware(value, "slot_utc")

    @field_validator("media")
    @classmethod
    def _handles(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for handle in value:
            if not _HANDLE.match(handle):
                raise ValueError(f"media handle {handle!r} is not sha256:<64 hex>")
        return value

    @model_validator(mode="after")
    def _shape(self) -> Self:
        if hashlib.sha256(self.caption.encode("utf-8")).hexdigest() != self.caption_sha256:
            raise ValueError("caption_sha256 does not match the caption bytes")
        if self.action == "reply":
            if self.parent is None:
                raise ValueError("a reply names its parent post, comment and comment version")
            if self.media or self.slot_utc is not None:
                raise ValueError("a reply carries text only and no slot")
        else:
            if self.parent is not None:
                raise ValueError("only a reply has a parent target")
            if self.slot_utc is None:
                raise ValueError(f"{self.action} needs slot_utc")
        if self.alt_text and len(self.alt_text) != len(self.media):
            raise ValueError("alt_text, when given, has one entry per media handle")
        expected = {"facebook_page": ("schedule_post", "reply"), "instagram_professional": ("publish_post", "reply")}
        if self.action not in expected[self.platform]:
            raise ValueError(f"{self.platform} does not take action {self.action!r}")
        return self

    def digest(self) -> str:
        return meta_payload_digest(self)


def meta_payload_digest(payload: MetaPayload) -> str:
    """The digest a Meta approval in the ledger is minted over."""
    return canonical_digest(payload.model_dump(mode="json"))


class SocialKey(_Strict):
    """The natural key — the same five columns LinkedIn rows have always used."""

    subject_kind: SubjectKind
    subject_id: str
    campaign_id: str
    post_id: str
    account_id: str

    def __str__(self) -> str:
        return f"{self.subject_kind}:{self.subject_id}/{self.campaign_id}/{self.post_id}@{self.account_id}"


class SocialRecord(BaseModel):
    """One approved payload on any platform, and everything known about its delivery."""

    model_config = ConfigDict(extra="forbid")

    publication_id: str
    key: SocialKey
    binding_id: str
    platform: Platform
    #: The approved payload as JSON: a LinkedIn `PostDraft` or a `MetaPayload`.
    payload: dict[str, object]
    payload_digest: str
    operation: str
    format: str
    state: SocialState = "pending"
    scheduled_at: datetime
    expires_at: datetime
    not_before: datetime | None = None
    attempt_token: str | None = None
    lease_deadline: datetime | None = None
    attempts: int = 0
    #: The provider's id for what was created. LinkedIn: the post URN (stored in
    #: `post_urn`). Meta: the Graph object id (stored in `remote_id`).
    remote_id: str | None = None
    #: Instagram only: the media container, persisted before media_publish.
    container_id: str | None = None
    permalink: str | None = None
    adapter: str | None = None
    published_at: datetime | None = None
    failure_code: str | None = None
    failure_detail: str | None = None
    #: The originating intent (a C3 operation key), unique across the ledger.
    intent_id: str | None = None
    #: Provenance of an imported approval: envelope digest, key, actor, source state.
    origin: dict[str, object] | None = None
    replaces: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def is_due(self, now: datetime) -> bool:
        if self.not_before is not None and now < self.not_before:
            return False
        return self.scheduled_at <= now < self.expires_at

    def is_expired(self, now: datetime) -> bool:
        return now >= self.expires_at

    def meta_payload(self) -> MetaPayload:
        return MetaPayload.model_validate(self.payload)
