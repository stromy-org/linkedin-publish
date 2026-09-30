"""`account register` and `account inspect`, end to end through the CLI.

HTTP and the database are replaced at the two seams the CLI exposes
(`_http_client`, `_binding_store`); everything between — option parsing, the
secret-shape refusal, the verdict, the exit codes — is the real code.
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
from linkedin_publish.bindings import InMemoryBindingStore

pytestmark = pytest.mark.contract

TOKEN = "AQX" + "t" * 300  # the shape of a real member token: long
SECRET = "client-secret-value"
EXPIRY = datetime.now(timezone.utc) + timedelta(days=59)

REGISTER = [
    "account", "register",
    "--binding", "bind-1",
    "--account-id", "william-personal",
    "--subject-id", "oid-1",
    "--app-id", "app-personal",
    "--author-urn", "urn:li:person:AbC123",
    "--credential-ref", "linkedin-member-token",
    "--credential-version", "v1",
    "--dsn", "postgres://unused",
]  # fmt: skip


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> InMemoryBindingStore:
    shared = InMemoryBindingStore()

    @asynccontextmanager
    async def fake_store(_dsn: str) -> AsyncIterator[InMemoryBindingStore]:
        yield shared

    monkeypatch.setattr(cli, "_binding_store", fake_store)
    return shared


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LINKEDIN_ACCESS_TOKEN", TOKEN)
    monkeypatch.setenv("LINKEDIN_CLIENT_ID", "app-personal")
    monkeypatch.setenv("LINKEDIN_CLIENT_SECRET", SECRET)
    monkeypatch.setenv("LINKEDIN_CREDENTIAL_VERSION", "v1")


def linkedin(monkeypatch: pytest.MonkeyPatch, *, sub: str = "AbC123", client_id: str = "app-personal") -> list[str]:
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("introspectToken"):
            return httpx.Response(
                200,
                json={
                    "active": True,
                    "status": "active",
                    "client_id": client_id,
                    "scope": "w_member_social,openid,profile",
                    "expires_at": int(EXPIRY.timestamp()),
                },
            )
        return httpx.Response(200, json={"sub": sub})

    monkeypatch.setattr(cli, "_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(respond)))
    return calls


def run(*args: str, stdin: str | None = None) -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, list(args), input=stdin)
    return result.exit_code, result.output


# ------------------------------------------------------------------ register


def test_register_writes_a_disabled_binding_after_confirmation(store: InMemoryBindingStore) -> None:
    code, output = run(*REGISTER, stdin="y\n")
    assert code == 0, output
    assert "registered  bind-1" in output
    assert "DISABLED" in output
    stored = asyncio.run(store.get("bind-1"))
    assert stored is not None
    assert stored.publish_enabled is False
    assert stored.declared_scopes == ("openid", "profile", "w_member_social")


def test_register_declining_the_prompt_writes_nothing(store: InMemoryBindingStore) -> None:
    code, _ = run(*REGISTER, stdin="n\n")
    assert code != 0
    assert asyncio.run(store.get("bind-1")) is None


def test_register_dry_run_needs_no_database(store: InMemoryBindingStore) -> None:
    args = [a for a in REGISTER if a not in ("--dsn", "postgres://unused")]
    code, output = run(*args, "--dry-run")
    assert code == 0
    assert "nothing written" in output
    assert asyncio.run(store.get("bind-1")) is None


def test_register_reports_an_identical_rerun_as_idempotent(store: InMemoryBindingStore) -> None:
    run(*REGISTER, stdin="y\n")
    code, output = run(*REGISTER, stdin="y\n")
    assert code == 0
    assert "already registered (identical)" in output


def test_register_refuses_a_changed_member_under_the_same_id(store: InMemoryBindingStore) -> None:
    run(*REGISTER, stdin="y\n")
    changed = [("urn:li:person:Other" if a == "urn:li:person:AbC123" else a) for a in REGISTER]
    code, output = run(*changed, stdin="y\n")
    assert code == 1
    assert "CONFLICT" in output


def test_register_refuses_a_pasted_token_as_the_reference(store: InMemoryBindingStore, env: None) -> None:
    pasted = [(TOKEN if a == "linkedin-member-token" else a) for a in REGISTER]
    code, output = run(*pasted, stdin="y\n")
    assert code == 2
    assert "never the secret" in output
    assert TOKEN not in output


def test_register_refuses_a_secret_shaped_reference_even_without_env(store: InMemoryBindingStore) -> None:
    pasted = [(TOKEN if a == "linkedin-member-token" else a) for a in REGISTER]
    code, output = run(*pasted, stdin="y\n")
    assert code == 2
    assert "short reference" in output


def test_register_rejects_a_malformed_author(store: InMemoryBindingStore) -> None:
    bad = [("linkedin.com/in/william" if a == "urn:li:person:AbC123" else a) for a in REGISTER]
    code, output = run(*bad)
    assert code == 1
    assert "INVALID" in output


def test_register_offers_no_client_subject_before_c7(store: InMemoryBindingStore) -> None:
    code, _ = run(*REGISTER, "--subject-kind", "client_slug", stdin="y\n")
    assert code == 2


# ------------------------------------------------------------------- inspect


def test_inspect_probe_prints_the_author_urn_to_register(monkeypatch: pytest.MonkeyPatch, env: None) -> None:
    linkedin(monkeypatch)
    code, output = run("account", "inspect")
    assert code == 0, output
    assert "PROBED" in output
    assert "urn:li:person:AbC123" in output
    assert "measured, not assumed" in output
    assert TOKEN not in output
    assert SECRET not in output


def test_inspect_against_a_registered_binding_is_healthy_and_records(
    monkeypatch: pytest.MonkeyPatch, env: None, store: InMemoryBindingStore
) -> None:
    run(*REGISTER, stdin="y\n")
    linkedin(monkeypatch)
    code, output = run("account", "inspect", "--binding", "bind-1", "--dsn", "postgres://unused", "--record", "--json")
    assert code == 0, output
    report = json.loads(output)
    assert report["verdict"] == "HEALTHY"
    assert report["recorded"] is True
    assert report["days_remaining"] in (58, 59)
    assert TOKEN not in output


def test_record_stores_expiry_without_enabling(
    monkeypatch: pytest.MonkeyPatch, env: None, store: InMemoryBindingStore
) -> None:
    run(*REGISTER, stdin="y\n")
    linkedin(monkeypatch)
    run("account", "inspect", "--binding", "bind-1", "--dsn", "postgres://unused", "--record")
    stored = asyncio.run(store.get("bind-1"))
    assert stored is not None
    assert stored.token_expires_at is not None
    assert stored.publish_enabled is False


def test_inspect_flags_a_different_member_as_bad(
    monkeypatch: pytest.MonkeyPatch, env: None, store: InMemoryBindingStore
) -> None:
    run(*REGISTER, stdin="y\n")
    linkedin(monkeypatch, sub="SomeoneElse")
    code, output = run("account", "inspect", "--binding", "bind-1", "--dsn", "postgres://unused")
    assert code == 1
    assert "BAD" in output
    assert "userinfo_member_mismatch" in output


def test_inspect_refuses_a_credential_for_another_app_before_any_request(
    monkeypatch: pytest.MonkeyPatch, env: None, store: InMemoryBindingStore
) -> None:
    run(*REGISTER, stdin="y\n")
    calls = linkedin(monkeypatch)
    monkeypatch.setenv("LINKEDIN_CLIENT_ID", "app-cma")
    code, output = run("account", "inspect", "--binding", "bind-1", "--dsn", "postgres://unused")
    assert code != 0
    assert "does not belong to this binding" in output
    assert calls == []


def test_inspect_names_missing_credentials_without_contacting_anything(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("LINKEDIN_ACCESS_TOKEN", "LINKEDIN_CLIENT_ID", "LINKEDIN_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    calls = linkedin(monkeypatch)
    code, output = run("account", "inspect")
    assert code == 2
    assert "LINKEDIN_ACCESS_TOKEN" in output
    assert calls == []


def test_inspect_record_without_a_binding_is_refused(monkeypatch: pytest.MonkeyPatch, env: None) -> None:
    calls = linkedin(monkeypatch)
    code, output = run("account", "inspect", "--record")
    assert code == 2
    assert calls == []
    assert "nowhere to record" in output


def test_inspect_unreachable_linkedin_is_unknown_not_bad(monkeypatch: pytest.MonkeyPatch, env: None) -> None:
    def down(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down")

    monkeypatch.setattr(cli, "_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(down)))
    code, output = run("account", "inspect")
    assert code == 2
    assert "UNKNOWN" in output
