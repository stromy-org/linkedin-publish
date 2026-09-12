"""Asset resolution, byte validation and upload transport policy.

Three rules shape this module:

1. **A SHA-256 handle is an identifier, not an authorization.** Every read goes
   through an injected `AssetReader` that is given the *subject* as well as the
   digest, so the store decides whether this caller may read those bytes.
2. **The declared media type is never trusted.** It is sniffed from the leading
   bytes, and a mismatch with the declared type is a refusal. That is what stops
   a renamed `.pdf` becoming a silent image failure at the provider.
3. **Upload destinations are constrained.** LinkedIn returns a signed upload URL;
   we accept it only over HTTPS, never follow a redirect off it, and never
   forward the LinkedIn `Authorization` header to it.

Dry-run never reaches any of this — `resolve_media` is called with
`upload=False` and returns a validation result with no network effect.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable
from urllib.parse import urlparse

import httpx

from .errors import ProviderRejected, PublishOutcomeUnknown, TransientReadFailure, ValidationFailure
from .models import ArticleDraft, DocumentDraft, ImageDraft, MediaRef

__all__ = [
    "AssetReader",
    "ResolvedAsset",
    "check_upload_destination",
    "detect_media_type",
    "sniff_and_validate",
    "upload_bytes",
]

#: Format limits, conservative for the pilot. Recheck at commissioning; the
#: provider's published figures move and its enforcement is the real bound.
IMAGE_MAX_BYTES: Final = 8 * 1024 * 1024
DOCUMENT_MAX_BYTES: Final = 100 * 1024 * 1024
THUMBNAIL_MAX_BYTES: Final = 8 * 1024 * 1024

IMAGE_MEDIA_TYPES: Final[frozenset[str]] = frozenset({"image/png", "image/jpeg", "image/gif"})
DOCUMENT_MEDIA_TYPES: Final[frozenset[str]] = frozenset({"application/pdf"})

#: Magic-number prefixes, longest first so a more specific signature wins.
_SIGNATURES: Final[tuple[tuple[bytes, str], ...]] = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"%PDF-", "application/pdf"),
)

_SNIFF_BYTES: Final = 16


@dataclass(frozen=True, slots=True)
class ResolvedAsset:
    """An asset that passed access, digest, type and size checks."""

    sha256: str
    media_type: str
    size: int


@runtime_checkable
class AssetReader(Protocol):
    """Reads asset bytes on behalf of a subject.

    Implemented in the hosted layer over `asset-transport`. The subject is passed
    on every call — this protocol has no notion of an ambient caller, so an
    implementation cannot accidentally serve one subject's bytes to another.
    """

    async def head(self, subject_id: str, sha256: str) -> tuple[int, str | None]:
        """Return `(size_bytes, declared_media_type)` or raise on no access."""
        ...

    def stream(self, subject_id: str, sha256: str) -> AsyncIterator[bytes]:
        """Yield the asset's bytes in chunks."""
        ...


def detect_media_type(head: bytes) -> str | None:
    """Sniff a media type from leading bytes, or None when unrecognised."""
    for signature, media_type in _SIGNATURES:
        if head.startswith(signature):
            return media_type
    return None


def _limits_for(kind: str) -> tuple[frozenset[str], int]:
    if kind == "image":
        return IMAGE_MEDIA_TYPES, IMAGE_MAX_BYTES
    if kind == "thumbnail":
        return IMAGE_MEDIA_TYPES, THUMBNAIL_MAX_BYTES
    if kind == "document":
        return DOCUMENT_MEDIA_TYPES, DOCUMENT_MAX_BYTES
    raise ValidationFailure(f"unknown media kind {kind!r}")


async def sniff_and_validate(
    reader: AssetReader,
    subject_id: str,
    ref: MediaRef,
    *,
    kind: str,
) -> ResolvedAsset:
    """Resolve `ref` and assert access, size and sniffed type.

    `kind` is one of `image`, `thumbnail`, `document`. A document that is not
    actually a PDF is refused here by name — that is the check standing between
    an editorial "carousel" and a post that silently loses its deck.
    """
    accepted, max_bytes = _limits_for(kind)

    try:
        size, declared = await reader.head(subject_id, ref.sha256)
    except PermissionError as exc:
        raise ValidationFailure(f"asset {ref.sha256[:12]}… is not readable by this subject") from exc
    except FileNotFoundError as exc:
        raise ValidationFailure(f"asset {ref.sha256[:12]}… does not exist in the asset store") from exc

    if size <= 0:
        raise ValidationFailure(f"asset {ref.sha256[:12]}… is empty")
    if size > max_bytes:
        raise ValidationFailure(
            f"asset {ref.sha256[:12]}… is {size} bytes; the {kind} maximum is {max_bytes}"
        )

    head = b""
    async for chunk in reader.stream(subject_id, ref.sha256):
        head += chunk
        if len(head) >= _SNIFF_BYTES:
            break
    sniffed = detect_media_type(head[:_SNIFF_BYTES])
    if sniffed is None:
        raise ValidationFailure(f"asset {ref.sha256[:12]}… has an unrecognised format")
    if sniffed not in accepted:
        allowed = ", ".join(sorted(accepted))
        raise ValidationFailure(f"asset {ref.sha256[:12]}… is {sniffed}; {kind} accepts {allowed}")
    if declared is not None and declared != sniffed:
        raise ValidationFailure(
            f"asset {ref.sha256[:12]}… is declared {declared} but its bytes are {sniffed}"
        )

    return ResolvedAsset(sha256=ref.sha256, media_type=sniffed, size=size)


def check_upload_destination(url: str) -> str:
    """Validate a provider-returned upload URL, returning it unchanged.

    Refuses anything but HTTPS. The URL is signed, so it never appears in an
    error message — a rejection names the host at most.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValidationFailure("upload destination must be https")
    if not parsed.hostname:
        raise ValidationFailure("upload destination has no host")
    return url


def redact_url(url: str) -> str:
    """Render a signed URL for logs: scheme, host and path, no query."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.hostname}{parsed.path}"


async def upload_bytes(
    http: httpx.AsyncClient,
    destination: str,
    reader: AssetReader,
    subject_id: str,
    asset: ResolvedAsset,
    *,
    access_token: str | None = None,
) -> None:
    """Stream `asset` to a provider-issued `destination`.

    `access_token` is forwarded only when the provider documents that the upload
    endpoint requires it (the UGC asset path does; the REST signed URL does not).
    Redirects are disabled outright: a 3xx from a signed upload URL would move
    the bytes — and any header we sent — to an origin LinkedIn did not name.
    """
    check_upload_destination(destination)
    headers = {"Content-Type": asset.media_type}
    if access_token is not None:
        headers["Authorization"] = f"Bearer {access_token}"

    async def body() -> AsyncIterator[bytes]:
        async for chunk in reader.stream(subject_id, asset.sha256):
            yield chunk

    try:
        response = await http.put(
            destination,
            content=body(),
            headers=headers,
            follow_redirects=False,
        )
    except httpx.TimeoutException as exc:
        # An upload is not a publication; a timeout here is a safe retry of the
        # upload, but it is surfaced as unknown so the caller re-resolves rather
        # than assuming the asset landed.
        raise PublishOutcomeUnknown(f"upload to {redact_url(destination)} timed out") from exc
    except httpx.HTTPError as exc:
        raise TransientReadFailure(f"upload to {redact_url(destination)} failed: {type(exc).__name__}") from exc

    if response.is_redirect:
        raise ProviderRejected(
            f"upload destination {redact_url(destination)} redirected; refusing to follow",
            http_status=response.status_code,
        )
    if response.status_code >= 400:
        raise ProviderRejected(
            f"upload to {redact_url(destination)} was rejected",
            http_status=response.status_code,
            request_id=response.headers.get("x-li-uuid"),
        )


def media_kind(media: ImageDraft | ArticleDraft | DocumentDraft) -> str:
    """The asset kind a media draft needs resolved, for limit selection."""
    if isinstance(media, ImageDraft):
        return "image"
    if isinstance(media, DocumentDraft):
        return "document"
    return "thumbnail"
