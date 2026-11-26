"""The canary on the operator CLI: commission → `publish due` → `account enable`.

Before v0.6.0 the CLI could issue a grant but nothing could use it (the shared
service refused the fresh binding's `unknown` text first), and nothing could
enable a never-enabled binding from the canary's receipt (`account reenable`
refuses that by design). These tests walk the whole route with the database and
HTTP replaced at the CLI's seams, the way `test_cli_rotate.py` walks renewal.

They are synchronous on purpose: the CLI calls `asyncio.run` itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from click.testing import CliRunner

from linkedin_publish import (
    InMemoryBudgetStore,
    InMemoryPublicationStore,
    PostDraft,
    PublicationService,
    StaticCredentialProvider,
    cli,
)
from linkedin_publish.auth import Credentials, TokenObservation
from linkedin_publish.bindings import InMemoryBindingStore
from tests.conftest import PERSON, binding, make_client
from tests.contract.test_service import DIGEST, POST_URN, record

pytestmark = pytest.mark.contract

DSN = ["--dsn", "postgres://unused"]
SECOND_URN = "urn:li:share:7100000000000000001"


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """A freshly registered, measured, DISABLED binding; a ledger; a fake LinkedIn."""
    now = datetime.now(timezone.utc)
    bindings = InMemoryBindingStore()
    ledger = InMemoryPublicationStore()
    budgets = InMemoryBudgetStore()
    calls: list[str] = []

    bindings._rows["bind-1"] = binding(publish_enabled=False, capabilities=()).model_copy(  # noqa: SLF001
        update={"token_expires_at": now + timedelta(days=14), "token_observed_at": now - timedelta(hours=1)}
    )
    asyncio.run(
        bindings.observations.put(
            "bind-1",
            TokenObservation(
                observed_at=now - timedelta(hours=1),
                credential_version="v1",
                token_active=True,
                scopes_ok=True,
                identity_verified=True,
            ),
        )
    )
    asyncio.run(
        ledger.upsert_publication(
            record(scheduled_at=now - timedelta(minutes=5), expires_at=now + timedelta(hours=2))
        )
    )

    @asynccontextmanager
    async def fake_bindings(_dsn: str) -> AsyncIterator[InMemoryBindingStore]:
        yield bindings

    @asynccontextmanager
    async def fake_ledger(_dsn: str) -> AsyncIterator[InMemoryPublicationStore]:
        yield ledger

    @asynccontextmanager
    async def fake_budgets(_dsn: str) -> AsyncIterator[InMemoryBudgetStore]:
        yield budgets

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        return httpx.Response(201, json={}, headers={"x-restli-id": POST_URN})

    monkeypatch.setattr(cli, "_binding_store", fake_bindings)
    monkeypatch.setattr(cli, "_publication_store", fake_ledger)
    monkeypatch.setattr(cli, "_budget_store", fake_budgets)
    monkeypatch.setattr(cli, "_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(respond)))
    monkeypatch.setenv("LINKEDIN_ACCESS_TOKEN", "AQX" + "n" * 300)
    monkeypatch.setenv("LINKEDIN_CLIENT_ID", "app-personal")
    monkeypatch.setenv("LINKEDIN_CLIENT_SECRET", "client-secret-value")
    monkeypatch.setenv("LINKEDIN_CREDENTIAL_VERSION", "v1")
    return {"bindings": bindings, "ledger": ledger, "calls": calls, "now": now}


def run(*args: str, stdin: str = "y\n") -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, [*args, *DSN], input=stdin)
    return result.exit_code, result.output


APPROVE = [
    "approval", "record", "--publication", "pub-1", "--binding", "bind-1",
    "--actor", "william", "--digest", DIGEST,
]  # fmt: skip
COMMISSION = [
    "account", "commission", "--publication", "pub-1", "--binding", "bind-1",
    "--capability", "text", "--actor", "william", "--digest", DIGEST,
]  # fmt: skip
SEND = ["publish", "due", "--binding", "bind-1", "--publication", "pub-1", "--actor", "william"]
ENABLE = ["account", "enable", "--binding", "bind-1", "--capability", "text", "--evidence", "pub-1"]


def ledger_of(world: dict[str, object]) -> InMemoryPublicationStore:
    return world["ledger"]  # type: ignore[return-value]


def bindings_of(world: dict[str, object]) -> InMemoryBindingStore:
    return world["bindings"]  # type: ignore[return-value]


def calls_of(world: dict[str, object]) -> list[str]:
    return world["calls"]  # type: ignore[return-value]


def commissioned(world: dict[str, object]) -> None:
    assert run(*APPROVE)[0] == 0
    code, output = run(*COMMISSION)
    assert code == 0, output


# ------------------------------------------------------------- the whole route


def test_the_canary_route_sends_once_then_enables_text_only(world: dict[str, object]) -> None:
    commissioned(world)

    code, output = run(*SEND)
    assert code == 0, output
    assert f"post_urn      {POST_URN}" in output
    assert calls_of(world) == ["POST /v2/ugcPosts"]
    sent = asyncio.run(ledger_of(world).get_publication("pub-1"))
    assert sent is not None and sent.state == "published"
    spent = asyncio.run(ledger_of(world).get_grant("pub-1"))
    assert spent is not None and spent.consumed_at is not None
    assert "account enable --binding bind-1 --capability text --evidence pub-1" in output

    code, output = run(*ENABLE)
    assert code == 0, output
    assert "unknown  ->  enabled" in output
    assert "False  ->  True" in output
    after = asyncio.run(bindings_of(world).get("bind-1"))
    assert after is not None
    assert after.publish_enabled is True
    assert after.capability_state("text") == "enabled"
    assert after.capability_state("image") == "unknown"
    status = next(item for item in after.capabilities if item.capability == "text")
    assert status.evidence_publication_id == "pub-1"

    # An ordinary scheduler tick now accepts the next text post and still
    # refuses an image: the text canary enabled text and nothing else.
    async def tick() -> tuple[int, int, int]:
        now = world["now"]
        assert isinstance(now, datetime)
        ledger = ledger_of(world)
        await ledger.upsert_publication(
            record(
                publication_id="pub-2",
                post_id="post-2",
                scheduled_at=now - timedelta(minutes=1),
                expires_at=now + timedelta(hours=1),
            )
        )
        from tests.contract.test_service import approval

        await ledger.put_approval(approval("pub-2"))
        image = record(
            publication_id="pub-3",
            post_id="post-3",
            scheduled_at=now - timedelta(minutes=1),
            expires_at=now + timedelta(hours=1),
        ).model_copy(
            update={
                "draft": PostDraft.model_validate(
                    {"author_urn": PERSON, "commentary": "c", "media": {"kind": "image", "asset": {"sha256": "a" * 64}}}
                )
            }
        )
        await ledger.upsert_publication(image)
        await ledger.put_approval(approval("pub-3"))
        client, log = make_client(lambda _r: httpx.Response(201, json={}, headers={"x-restli-id": SECOND_URN}))
        service = PublicationService(
            ledger,
            client,
            StaticCredentialProvider(
                Credentials(access_token="t", client_id="app-personal", client_secret="s", credential_version="v1")
            ),
        )
        enabled = await bindings_of(world).get("bind-1")
        assert enabled is not None
        result = await service.publish_due(enabled, campaign_id="camp-1", now=now, dry_run=True)
        await client.aclose()
        return result.due, result.skipped_capability, log.count

    due, skipped, requests = asyncio.run(tick())
    assert (due, skipped, requests) == (2, 1, 0)


# ------------------------------------------------------------- `account commission`


def test_commission_refuses_a_capability_the_post_does_not_need(world: dict[str, object]) -> None:
    assert run(*APPROVE)[0] == 0
    code, output = run(*[arg if arg != "text" else "image" for arg in COMMISSION])
    assert code == 1
    assert "would license nothing" in output
    assert asyncio.run(ledger_of(world).get_grant("pub-1")) is None


# ------------------------------------------------------------------ `publish due`


def test_publish_due_without_a_grant_sends_nothing(world: dict[str, object]) -> None:
    assert run(*APPROVE)[0] == 0
    code, output = run(*SEND)
    assert code == 1
    assert "capability_unavailable" in output
    assert calls_of(world) == []
    still = asyncio.run(ledger_of(world).get_publication("pub-1"))
    assert still is not None and still.state == "pending"


def test_publish_due_as_another_actor_cannot_use_the_grant(world: dict[str, object]) -> None:
    commissioned(world)
    code, output = run(*[arg if arg != "william" else "someone-else" for arg in SEND])
    assert code == 1, output
    assert calls_of(world) == []
    unspent = asyncio.run(ledger_of(world).get_grant("pub-1"))
    assert unspent is not None and unspent.consumed_at is None

    code, output = run(*SEND)
    assert code == 0, output
    assert calls_of(world) == ["POST /v2/ugcPosts"]


def test_publish_due_refuses_a_credential_version_the_binding_does_not_name(
    world: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    commissioned(world)
    monkeypatch.setenv("LINKEDIN_CREDENTIAL_VERSION", "v9")
    code, output = run(*SEND)
    assert code == 2
    assert "names 'v1'" in output
    assert calls_of(world) == []


def test_declining_the_send_prompt_sends_nothing(world: dict[str, object]) -> None:
    commissioned(world)
    code, _output = run(*SEND, stdin="n\n")
    assert code != 0
    assert calls_of(world) == []


def test_an_unknown_outcome_exits_3_and_never_renews_the_grant(
    world: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    commissioned(world)
    monkeypatch.setattr(
        cli,
        "_http_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: httpx.Response(503))),
    )
    code, output = run(*SEND)
    assert code == 3, output
    assert "never re-send" in output
    lost = asyncio.run(ledger_of(world).get_publication("pub-1"))
    assert lost is not None and lost.state == "unknown"
    spent = asyncio.run(ledger_of(world).get_grant("pub-1"))
    assert spent is not None and spent.consumed_at is not None

    code, output = run(*SEND)
    assert code == 1
    assert "only a pending publication is sent" in output


# --------------------------------------------------------------- `account enable`


def test_enable_without_a_published_receipt_is_refused(world: dict[str, object]) -> None:
    commissioned(world)
    code, output = run(*ENABLE)
    assert code == 1
    assert "no published receipt" in output
    after = asyncio.run(bindings_of(world).get("bind-1"))
    assert after is not None and after.publish_enabled is False
    assert after.capability_state("text") == "unknown"


def test_enable_for_another_capability_is_refused(world: dict[str, object]) -> None:
    commissioned(world)
    assert run(*SEND)[0] == 0
    code, output = run(*[arg if arg != "text" else "image" for arg in ENABLE])
    assert code == 1
    assert "it enables 'text' only, not 'image'" in output
    after = asyncio.run(bindings_of(world).get("bind-1"))
    assert after is not None and after.capabilities == () and after.publish_enabled is False


def test_enable_from_another_bindings_receipt_is_refused(world: dict[str, object]) -> None:
    commissioned(world)
    assert run(*SEND)[0] == 0
    bindings = bindings_of(world)
    bindings._rows["bind-2"] = bindings._rows["bind-1"].model_copy(update={"binding_id": "bind-2"})  # noqa: SLF001
    asyncio.run(bindings.observations.put("bind-2", asyncio.run(bindings.observations.get("bind-1", "v1"))))  # type: ignore[arg-type]
    code, output = run(*[arg if arg != "bind-1" else "bind-2" for arg in ENABLE])
    assert code == 1
    assert "belongs to binding bind-1" in output


def test_enable_needs_a_healthy_measurement(world: dict[str, object]) -> None:
    commissioned(world)
    assert run(*SEND)[0] == 0
    bindings = bindings_of(world)
    asyncio.run(bindings.observations.invalidate("bind-1"))
    code, output = run(*ENABLE)
    assert code == 1
    assert "no health observation" in output


def test_enable_dry_run_and_a_declined_prompt_write_nothing(world: dict[str, object]) -> None:
    commissioned(world)
    assert run(*SEND)[0] == 0
    code, output = run(*ENABLE, "--dry-run")
    assert code == 0 and "dry run" in output
    code, _ = run(*ENABLE, stdin="n\n")
    assert code != 0
    after = asyncio.run(bindings_of(world).get("bind-1"))
    assert after is not None and after.publish_enabled is False


def test_enable_twice_is_refused_the_second_time(world: dict[str, object]) -> None:
    commissioned(world)
    assert run(*SEND)[0] == 0
    assert run(*ENABLE)[0] == 0
    code, output = run(*ENABLE)
    assert code == 1
    assert "already enabled" in output


def test_the_new_commands_take_no_credential_or_auto_yes_argument() -> None:
    for command in (["publish", "due"], ["account", "enable"]):
        result = CliRunner().invoke(cli.main, [*command, "--help"])
        assert result.exit_code == 0
        for flag in ("--token", "--access-token", "--yes", "--secret"):
            assert flag not in result.output, (command, flag)


def test_help_lists_publish_due_and_account_enable() -> None:
    import re

    publish = CliRunner().invoke(cli.main, ["publish", "--help"])
    assert publish.exit_code == 0
    assert re.search(r"^\s+due\s", publish.output, re.MULTILINE)
    account = CliRunner().invoke(cli.main, ["account", "--help"])
    assert re.search(r"^\s+enable\s", account.output, re.MULTILINE)
