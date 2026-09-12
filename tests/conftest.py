"""Shared fixtures.

Everything here is offline. The only thing ever replaced is HTTP: a recording
`httpx.MockTransport` stands in for LinkedIn so a test can assert on the exact
request that *would* have gone out — method, URL, headers, body — and count how
many left. Counting is the point in several tests: "zero remote calls" and "one
remote acceptance across repeated runs" are the assertions that actually prove
the safety properties.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from linkedin_publish import (
    AccountBinding,
    CapabilityStatus,
    Credentials,
    LinkedInClient,
    PostDraft,
)

NOW = datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc)

PERSON = "urn:li:person:AbC123xyz"
ORG = "urn:li:organization:8877"

PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
PDF_BYTES = b"%PDF-1.7\n" + b"\x00" * 64
JPEG_BYTES = b"\xff\xd8\xff\xe0" + b"\x00" * 64

PNG_SHA = "a" * 64
PDF_SHA = "b" * 64
JPEG_SHA = "c" * 64


@dataclass
class RecordedCall:
    method: str
    url: str
    headers: dict[str, str]
    body: bytes


@dataclass
class Recorder:
    """Every request the client attempted, in order."""

    calls: list[RecordedCall] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.calls)

    def paths(self) -> list[str]:
        return [httpx.URL(call.url).path for call in self.calls]

    def last(self) -> RecordedCall:
        return self.calls[-1]


Handler = Callable[[httpx.Request], httpx.Response]


def make_client(
    handler: Handler,
    *,
    recorder: Recorder | None = None,
    **kwargs: object,
) -> tuple[LinkedInClient, Recorder]:
    """A `LinkedInClient` whose only fake is the transport."""
    log = recorder or Recorder()

    def record(request: httpx.Request) -> httpx.Response:
        log.calls.append(
            RecordedCall(
                method=request.method,
                url=str(request.url),
                headers=dict(request.headers),
                body=request.read(),
            )
        )
        return handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(record), follow_redirects=False)
    return LinkedInClient(http=http, api_base="https://api.linkedin.test", **kwargs), log  # type: ignore[arg-type]


class FakeAssetReader:
    """A bounded in-memory asset store keyed by subject and digest.

    Access is per-subject on purpose: a test can prove that one subject's handle
    does not read another subject's bytes, which is the behaviour the real
    `asset-transport` reader is relied on for.
    """

    def __init__(self, assets: dict[tuple[str, str], tuple[bytes, str | None]]) -> None:
        self._assets = assets

    async def head(self, subject_id: str, sha256: str) -> tuple[int, str | None]:
        entry = self._assets.get((subject_id, sha256))
        if entry is None:
            if any(key[1] == sha256 for key in self._assets):
                raise PermissionError(sha256)
            raise FileNotFoundError(sha256)
        blob, declared = entry
        return len(blob), declared

    async def stream(self, subject_id: str, sha256: str) -> AsyncIterator[bytes]:
        entry = self._assets.get((subject_id, sha256))
        if entry is None:
            raise FileNotFoundError(sha256)
        blob, _ = entry
        for offset in range(0, len(blob), 8):
            yield blob[offset : offset + 8]


@pytest.fixture
def reader() -> FakeAssetReader:
    return FakeAssetReader(
        {
            ("subject-1", PNG_SHA): (PNG_BYTES, "image/png"),
            ("subject-1", PDF_SHA): (PDF_BYTES, "application/pdf"),
            ("subject-1", JPEG_SHA): (JPEG_BYTES, None),
        }
    )


@pytest.fixture
def credentials() -> Credentials:
    return Credentials(
        access_token="test-token-not-a-real-credential",
        client_id="app-personal",
        client_secret="test-secret-not-a-real-credential",
        credential_version="v1",
    )


def binding(
    *,
    adapter: str = "share_ugc",
    capabilities: tuple[str, ...] = ("text",),
    publish_enabled: bool = True,
    author: str = PERSON,
    subject_id: str = "subject-1",
    allowed_orgs: tuple[str, ...] = (),
    binding_id: str = "bind-1",
) -> AccountBinding:
    """A commissioned binding. Capabilities carry evidence, as the model requires."""
    return AccountBinding(
        binding_id=binding_id,
        account_id="acct-william",
        subject_kind="entra_oid",
        subject_id=subject_id,
        app_id="app-personal",
        author_urn=author,
        allowed_organization_urns=allowed_orgs,
        adapter=adapter,  # type: ignore[arg-type]
        declared_scopes=("w_member_social", "openid", "profile"),
        observed_scopes=("w_member_social", "openid", "profile"),
        credential_ref="kv://linkedin/member-token",
        credential_version="v1",
        token_expires_at=NOW + timedelta(days=60),
        token_observed_at=NOW,
        capabilities=tuple(
            CapabilityStatus(
                capability=name,  # type: ignore[arg-type]
                state="enabled",
                observed_at=NOW,
                evidence_publication_id=f"canary-{name}",
            )
            for name in capabilities
        ),
        publish_enabled=publish_enabled,
    )


@pytest.fixture
def text_draft() -> PostDraft:
    return PostDraft(author_urn=PERSON, commentary="Intelligence, orchestrated.", visibility="PUBLIC")


def ok(payload: dict[str, object] | None = None, *, status: int = 201, **headers: str) -> httpx.Response:
    return httpx.Response(status, json=payload or {}, headers=headers)


def fail(status: int, *, code: str | None = None, **headers: str) -> httpx.Response:
    body: dict[str, object] = {"message": "Unauthorized to post as that author"}
    if code is not None:
        body["serviceErrorCode"] = code
    return httpx.Response(status, json=body, headers=headers)
