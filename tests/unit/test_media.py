"""Asset access, byte sniffing, and upload transport policy."""

from __future__ import annotations

import httpx
import pytest

from linkedin_publish import MediaRef
from linkedin_publish.errors import ProviderRejected, PublishOutcomeUnknown, ValidationFailure
from linkedin_publish.media import (
    check_upload_destination,
    detect_media_type,
    redact_url,
    sniff_and_validate,
    upload_bytes,
)
from tests.conftest import JPEG_SHA, PDF_SHA, PNG_BYTES, PNG_SHA, FakeAssetReader

pytestmark = pytest.mark.unit

SIGNED = "https://upload.linkedin.test/v1/put?sig=SECRET-SIGNATURE&exp=123"


def test_media_types_are_sniffed_from_bytes() -> None:
    assert detect_media_type(b"\x89PNG\r\n\x1a\n") == "image/png"
    assert detect_media_type(b"\xff\xd8\xff\xe0") == "image/jpeg"
    assert detect_media_type(b"%PDF-1.7") == "application/pdf"
    assert detect_media_type(b"GIF89a") == "image/gif"
    assert detect_media_type(b"MZ\x90\x00") is None


async def test_a_readable_image_resolves(reader: FakeAssetReader) -> None:
    asset = await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PNG_SHA), kind="image")
    assert asset.media_type == "image/png"
    assert asset.size == len(PNG_BYTES)


async def test_a_sha_handle_is_not_authorization(reader: FakeAssetReader) -> None:
    """Another subject's digest is a handle, not a grant. The store decides."""
    with pytest.raises(ValidationFailure, match="not readable by this subject"):
        await sniff_and_validate(reader, "subject-2", MediaRef(sha256=PNG_SHA), kind="image")


async def test_a_missing_asset_is_distinguished_from_a_forbidden_one(reader: FakeAssetReader) -> None:
    with pytest.raises(ValidationFailure, match="does not exist"):
        await sniff_and_validate(reader, "subject-1", MediaRef(sha256="f" * 64), kind="image")


async def test_a_pdf_cannot_be_posted_as_an_image(reader: FakeAssetReader) -> None:
    with pytest.raises(ValidationFailure, match="image accepts"):
        await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PDF_SHA), kind="image")


async def test_a_png_cannot_be_posted_as_a_document(reader: FakeAssetReader) -> None:
    """A "carousel" that is not a PDF is refused, not downgraded."""
    with pytest.raises(ValidationFailure, match="document accepts application/pdf"):
        await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PNG_SHA), kind="document")


async def test_a_declared_type_that_contradicts_the_bytes_is_refused() -> None:
    lying = FakeAssetReader({("s", "d" * 64): (PNG_BYTES, "application/pdf")})
    with pytest.raises(ValidationFailure, match="declared application/pdf but its bytes are image/png"):
        await sniff_and_validate(lying, "s", MediaRef(sha256="d" * 64), kind="image")


async def test_an_undeclared_type_is_accepted_when_the_bytes_are_valid(reader: FakeAssetReader) -> None:
    asset = await sniff_and_validate(reader, "subject-1", MediaRef(sha256=JPEG_SHA), kind="image")
    assert asset.media_type == "image/jpeg"


async def test_an_oversized_asset_is_refused() -> None:
    from linkedin_publish.media import IMAGE_MAX_BYTES

    class Huge:
        async def head(self, subject_id: str, sha256: str) -> tuple[int, str | None]:
            return IMAGE_MAX_BYTES + 1, "image/png"

        async def stream(self, subject_id: str, sha256: str):  # noqa: ANN202
            yield PNG_BYTES

    with pytest.raises(ValidationFailure, match="the image maximum is"):
        await sniff_and_validate(Huge(), "s", MediaRef(sha256=PNG_SHA), kind="image")


async def test_an_empty_asset_is_refused() -> None:
    class Empty:
        async def head(self, subject_id: str, sha256: str) -> tuple[int, str | None]:
            return 0, None

        async def stream(self, subject_id: str, sha256: str):  # noqa: ANN202
            yield b""

    with pytest.raises(ValidationFailure, match="is empty"):
        await sniff_and_validate(Empty(), "s", MediaRef(sha256=PNG_SHA), kind="image")


def test_only_https_upload_destinations_are_accepted() -> None:
    assert check_upload_destination(SIGNED) == SIGNED
    with pytest.raises(ValidationFailure, match="must be https"):
        check_upload_destination("http://upload.linkedin.test/v1/put")
    with pytest.raises(ValidationFailure, match="must be https"):
        check_upload_destination("file:///etc/passwd")


def test_a_signed_url_is_redacted_for_logs() -> None:
    redacted = redact_url(SIGNED)
    assert "SECRET-SIGNATURE" not in redacted
    assert redacted == "https://upload.linkedin.test/v1/put"


async def test_an_upload_never_follows_a_redirect(reader: FakeAssetReader) -> None:
    """A 3xx would move the bytes — and any header — to an origin LinkedIn did not name."""
    asset = await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PNG_SHA), kind="image")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://evil.test/collect"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
    with pytest.raises(ProviderRejected, match="redirected"):
        await upload_bytes(http, SIGNED, reader, "subject-1", asset)
    await http.aclose()


async def test_an_upload_error_never_quotes_the_signed_url(reader: FakeAssetReader) -> None:
    asset = await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PNG_SHA), kind="image")
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(403)), follow_redirects=False
    )
    with pytest.raises(ProviderRejected) as caught:
        await upload_bytes(http, SIGNED, reader, "subject-1", asset)
    assert "SECRET-SIGNATURE" not in str(caught.value)
    await http.aclose()


async def test_an_upload_timeout_is_unknown_not_a_silent_success(reader: FakeAssetReader) -> None:
    asset = await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PNG_SHA), kind="image")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
    with pytest.raises(PublishOutcomeUnknown):
        await upload_bytes(http, SIGNED, reader, "subject-1", asset)
    await http.aclose()


async def test_the_member_token_is_forwarded_only_when_asked(reader: FakeAssetReader) -> None:
    """REST signed URLs do not need it, so it is not sent to them."""
    asset = await sniff_and_validate(reader, "subject-1", MediaRef(sha256=PNG_SHA), kind="image")
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request.read()
        seen.append(dict(request.headers))
        return httpx.Response(201)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
    await upload_bytes(http, SIGNED, reader, "subject-1", asset, access_token=None)
    assert "authorization" not in seen[-1]

    await upload_bytes(http, SIGNED, reader, "subject-1", asset, access_token="tok")
    assert seen[-1]["authorization"] == "Bearer tok"
    await http.aclose()
