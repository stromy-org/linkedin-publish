"""Renewal through the CLI: `account rotate` → `inspect --record` → `account reenable`.

The gap this closes (ORG-PLAN-285 log, 2026-10-11): `credential_version` is a
binding identity field and `inspect --record` refuses a version mismatch, so a
re-minted token used to cost a new binding, a config PR and a deploy. These
tests walk the new path end to end with the database and HTTP replaced at the
CLI's two seams, and pin the safety edges: rotation disables, the cache cannot
vouch for the new secret with the old one's probe, and re-enable restores only
what the rotation took away.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from click.testing import CliRunner

from linkedin_publish import cli
from linkedin_publish.auth import TokenObservation
from linkedin_publish.bindings import InMemoryBindingStore
from tests.conftest import binding

pytestmark = pytest.mark.contract

TOKEN = "AQX" + "n" * 300
EXPIRY = datetime.now(timezone.utc) + timedelta(days=59)
AUTHOR_SUB = "AbC123xyz"


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> InMemoryBindingStore:
    shared = InMemoryBindingStore()

    @asynccontextmanager
    async def fake_store(_dsn: str) -> AsyncIterator[InMemoryBindingStore]:
        yield shared

    monkeypatch.setattr(cli, "_binding_store", fake_store)
    # An enabled, measured binding on credential v1, with a cached v1 probe.
    shared._rows["bind-1"] = binding().model_copy(  # noqa: SLF001 - seed a commissioned binding
        update={"token_expires_at": datetime.now(timezone.utc) + timedelta(days=5)}
    )
    asyncio.run(
        shared.observations.put(
            "bind-1",
            TokenObservation(
                observed_at=datetime.now(timezone.utc) - timedelta(hours=1),
                credential_version="v1",
                token_active=True,
                scopes_ok=True,
                identity_verified=True,
            ),
        )
    )
    return shared


@pytest.fixture
def new_secret(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    monkeypatch.setenv("LINKEDIN_ACCESS_TOKEN", TOKEN)
    monkeypatch.setenv("LINKEDIN_CLIENT_ID", "app-personal")
    monkeypatch.setenv("LINKEDIN_CLIENT_SECRET", "client-secret-value")
    monkeypatch.setenv("LINKEDIN_CREDENTIAL_VERSION", "v2")
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("introspectToken"):
            return httpx.Response(
                200,
                json={
                    "active": True,
                    "status": "active",
                    "client_id": "app-personal",
                    "scope": "w_member_social,openid,profile",
                    "expires_at": int(EXPIRY.timestamp()),
                },
            )
        return httpx.Response(200, json={"sub": AUTHOR_SUB})

    monkeypatch.setattr(cli, "_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(respond)))
    return calls


def run(*args: str, stdin: str | None = None) -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, list(args), input=stdin)
    return result.exit_code, result.output


ROTATE = [
    "account", "rotate", "--binding", "bind-1",
    "--credential-ref", "operator-env:linkedin.env", "--credential-version", "v2",
    "--dsn", "postgres://unused",
]  # fmt: skip
INSPECT = ["account", "inspect", "--binding", "bind-1", "--dsn", "postgres://unused", "--record"]
REENABLE = ["account", "reenable", "--binding", "bind-1", "--dsn", "postgres://unused"]
STATUS = ["account", "status", "--binding", "bind-1", "--dsn", "postgres://unused", "--json"]


def test_rotate_disables_moves_the_version_and_invalidates_the_cache(store: InMemoryBindingStore) -> None:
    code, output = run(*ROTATE, stdin="y\n")
    assert code == 0, output
    assert "publishing DISABLED" in output
    assert "1 observation(s) dropped" in output

    moved = asyncio.run(store.get("bind-1"))
    assert moved is not None
    assert moved.credential_version == "v2"
    assert moved.credential_ref == "operator-env:linkedin.env"
    assert moved.publish_enabled is False
    assert moved.token_expires_at is None
    assert moved.reenable_after_measure is True
    assert asyncio.run(store.observations.get("bind-1", "v1")) is None


def test_rotate_dry_run_writes_nothing(store: InMemoryBindingStore) -> None:
    code, output = run(*ROTATE, "--dry-run")
    assert code == 0
    assert "nothing written" in output
    unchanged = asyncio.run(store.get("bind-1"))
    assert unchanged is not None and unchanged.credential_version == "v1" and unchanged.publish_enabled


def test_rotate_refuses_a_stale_from_version(store: InMemoryBindingStore) -> None:
    code, output = run(*ROTATE, "--from-version", "v0", stdin="y\n")
    assert code == 1
    assert "CONFLICT" in output
    assert asyncio.run(store.get("bind-1")).credential_version == "v1"  # type: ignore[union-attr]


def test_rotate_refuses_a_secret_shaped_reference(store: InMemoryBindingStore) -> None:
    args = list(ROTATE)
    args[args.index("operator-env:linkedin.env")] = TOKEN
    code, output = run(*args, stdin="y\n")
    assert code == 2
    assert "ABORT" in output


def test_reenable_is_refused_until_the_new_secret_is_measured(store: InMemoryBindingStore) -> None:
    run(*ROTATE, stdin="y\n")
    code, output = run(*REENABLE, stdin="y\n")
    assert code == 1
    assert "REFUSED" in output
    assert asyncio.run(store.get("bind-1")).publish_enabled is False  # type: ignore[union-attr]


def test_the_full_renewal_path_restores_publishing(store: InMemoryBindingStore, new_secret: list[str]) -> None:
    assert run(*ROTATE, stdin="y\n")[0] == 0

    code, output = run(*INSPECT)
    assert code == 0, output
    assert "health cache     cached" in output
    measured = asyncio.run(store.get("bind-1"))
    assert measured is not None and measured.token_expires_at is not None
    assert asyncio.run(store.observations.get("bind-1", "v2")) is not None

    code, output = run(*REENABLE, stdin="y\n")
    assert code == 0, output
    back = asyncio.run(store.get("bind-1"))
    assert back is not None and back.publish_enabled is True and back.reenable_after_measure is False


def test_reenable_never_enables_a_never_enabled_binding(store: InMemoryBindingStore, new_secret: list[str]) -> None:
    store._rows["bind-1"] = store._rows["bind-1"].model_copy(update={"publish_enabled": False})  # noqa: SLF001
    run(*ROTATE, stdin="y\n")
    run(*INSPECT)
    code, output = run(*REENABLE, stdin="y\n")
    assert code == 1
    assert "canary" in output
    assert asyncio.run(store.get("bind-1")).publish_enabled is False  # type: ignore[union-attr]


def test_status_reads_the_measured_expiry_and_warns_without_a_credential(
    store: InMemoryBindingStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("LINKEDIN_ACCESS_TOKEN", "LINKEDIN_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(cli, "_http_client", lambda: pytest.fail("status must make no request"))
    code, output = run(*STATUS)
    assert code == 0, output
    report = json.loads(output)
    assert report["health"]["status"] == "healthy"
    assert report["health"]["expiry_warning"]["code"] == "token_expires_soon"
    assert report["health"]["expiry_warning"]["days_remaining"] in (4, 5)
    assert report["health_cache"] == "hit"
