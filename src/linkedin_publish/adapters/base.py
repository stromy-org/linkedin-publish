"""The adapter contract both transports implement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..auth import Credentials
from ..media import AssetReader, ResolvedAsset
from ..models import Adapter, PostDraft

__all__ = ["PostAdapter", "PublishOutcome", "UploadedMedia", "permalink_for"]


@dataclass(frozen=True, slots=True)
class UploadedMedia:
    """A media asset that now exists on LinkedIn, in this adapter's URN space."""

    urn: str
    #: The adapter that issued it. An upload from one adapter, app or author is
    #: never reusable in another — the store's uniqueness key includes all three.
    adapter: Adapter


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    """A provider-confirmed publication."""

    post_urn: str
    permalink: str
    adapter: Adapter


def permalink_for(post_urn: str) -> str:
    """The public URL for a published post URN."""
    return f"https://www.linkedin.com/feed/update/{post_urn}/"


class PostAdapter(Protocol):
    """One LinkedIn publishing product."""

    name: Adapter

    async def upload_image(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
    ) -> UploadedMedia: ...

    async def upload_document(
        self,
        credentials: Credentials,
        author_urn: str,
        reader: AssetReader,
        subject_id: str,
        asset: ResolvedAsset,
    ) -> UploadedMedia: ...

    async def create_post(
        self,
        credentials: Credentials,
        draft: PostDraft,
        media_urn: str | None,
    ) -> PublishOutcome: ...
