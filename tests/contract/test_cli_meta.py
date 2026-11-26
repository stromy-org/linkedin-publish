"""The Meta canary on the operator CLI: inspect --record → commission → publish-due → enable.

The database and Graph are replaced at the CLI's seams (`_social_store`,
`_social_binding_store`, `_http_client`), the way `test_cli_commission.py` walks
the LinkedIn route. Synchronous on purpose: the CLI calls `asyncio.run` itself.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from click.testing import CliRunner

from linkedin_publish import cli
from social_publish import InMemorySocialBindingStore, InMemorySocialStore, SocialBinding
from tests.social_fakes import TOKEN, FakeGraph, approval_for, meta_binding, meta_payload, record_for

pytestmark = pytest.mark.contract

DSN = ["--dsn", "postgres://unused"]
ACTOR = "fixture-operator"


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """A registered, DISABLED Facebook binding (all unknown), one approved due canary, a fake Graph."""
    now = datetime.now(UTC)
    binding = meta_binding(publish_enabled=False)
    bindings = InMemorySocialBindingStore([binding])
    ledger = InMemorySocialStore([binding])
    graph = FakeGraph()
    debug: dict[str, object] = {"expires_at": 0}
    record = record_for(
        binding,
        meta_payload(slot=now + timedelta(hours=2)),
        publication_id="canary-1",
        scheduled_at=now - timedelta(minutes=5),
        expires_at=now + timedelta(hours=1),
    )
    asyncio.run(ledger.import_intent(record, approval_for(record), actor=ACTOR, at=now))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/debug_token"):
            data = {"app_id": binding.app_id, "is_valid": True, "scopes": ["pages_manage_posts"], **debug}
            return httpx.Response(200, json={"data": data})
        return graph.handler(request)

    @asynccontextmanager
    async def fake_ledger(_dsn: str) -> AsyncIterator[InMemorySocialStore]:
        yield ledger

    @asynccontextmanager
    async def fake_bindings(_dsn: str) -> AsyncIterator[InMemorySocialBindingStore]:
        yield bindings

    monkeypatch.setattr(cli, "_social_store", fake_ledger)
    monkeypatch.setattr(cli, "_social_binding_store", fake_bindings)
    monkeypatch.setattr(cli, "_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    monkeypatch.setenv("META_ACCESS_TOKEN", TOKEN)
    monkeypatch.setenv("META_CREDENTIAL_VERSION", "v1")
    return {"bindings": bindings, "ledger": ledger, "graph": graph, "record": record, "debug": debug}


def run(*args: str, stdin: str = "y\n") -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, [*args, *DSN], input=stdin)
    return result.exit_code, result.output


def binding_of(world: dict[str, object]) -> SocialBinding:
    found = asyncio.run(world["bindings"].get("bind-facebook_page"))  # type: ignore[attr-defined]
    assert found is not None
    return found


def commission(world: dict[str, object], *extra: str) -> list[str]:
    digest = world["record"].payload_digest  # type: ignore[attr-defined]
    return [
        "meta", "commission", "--publication", "canary-1", "--binding", "bind-facebook_page", "--digest", digest,
        "--operation", "schedule_post", "--format", "text", "--actor", ACTOR, *extra,
    ]  # fmt: skip


SEND = ["meta", "publish-due", "--binding", "bind-facebook_page", "--publication", "canary-1", "--actor", ACTOR]
ENABLE = [
    "meta", "enable", "--binding", "bind-facebook_page", "--operation", "schedule_post", "--format", "text",
    "--evidence", "canary-1",
]  # fmt: skip
INSPECT = ["meta", "inspect", "--binding", "bind-facebook_page"]


def test_the_whole_meta_canary_route(world: dict[str, object]) -> None:
    code, out = run(*INSPECT, "--record", "--dry-run")
    assert code == 0 and "dry run" in out and binding_of(world).token_observed_at is None

    code, out = run(*INSPECT, "--record")
    assert code == 0, out
    assert "explicit no-expiry evidence" in out and binding_of(world).token_never_expires

    code, out = run("meta", "status", "--binding", "bind-facebook_page", "--json")
    assert code == 0 and json.loads(out)["token"]["token_never_expires"] is True

    code, out = run(*commission(world, "--dry-run"))
    assert code == 0 and "dry run" in out
    assert asyncio.run(world["ledger"].get_grant("canary-1")) is None  # type: ignore[attr-defined]
    code, out = run(*commission(world))
    assert code == 0 and "granted grant_" in out, out

    code, out = run(*SEND, "--dry-run")
    assert code == 0 and world["graph"].writes() == []  # type: ignore[attr-defined]
    code, out = run(*SEND)
    assert code == 0, out
    assert "scheduled  canary-1" in out and "next: check it on Meta, then `meta enable" in out
    assert len(world["graph"].writes()) == 1  # type: ignore[attr-defined]

    code, out = run(*ENABLE, "--dry-run")
    assert code == 0 and "unknown  ->  enabled" in out and binding_of(world).publish_enabled is False
    code, out = run(*ENABLE)
    assert code == 0, out
    enabled = binding_of(world)
    assert enabled.publish_enabled and enabled.capability_state("schedule_post", "text") == "enabled"
    assert enabled.capability_state("schedule_post", "feed_image") == "unknown"


def test_publish_due_without_a_grant_sends_nothing(world: dict[str, object]) -> None:
    code, out = run(*SEND)
    assert code == 1 and "publish_disabled" in out
    assert world["graph"].writes() == []  # type: ignore[attr-defined]


def test_publish_due_as_another_actor_cannot_use_the_grant(world: dict[str, object]) -> None:
    assert run(*commission(world))[0] == 0
    code, out = run("meta", "publish-due", "--binding", "bind-facebook_page", "--publication", "canary-1",
                    "--actor", "someone-else")  # fmt: skip
    assert code == 1 and "publish_disabled" in out
    assert world["graph"].writes() == []  # type: ignore[attr-defined]


def test_commission_refuses_another_format_and_a_wrong_digest(world: dict[str, object]) -> None:
    args = commission(world)
    args[args.index("text")] = "feed_image"
    code, out = run(*args)
    assert code == 1 and "would license nothing" in out
    args = commission(world)
    args[args.index("--digest") + 1] = "f" * 64
    code, out = run(*args)
    assert code == 1 and "what you reviewed is not what is stored" in out
    assert asyncio.run(world["ledger"].get_grant("canary-1")) is None  # type: ignore[attr-defined]


def test_enable_without_a_receipt_is_refused(world: dict[str, object]) -> None:
    assert run(*INSPECT, "--record")[0] == 0
    code, out = run(*ENABLE)
    assert code == 1 and "REFUSED" in out and "receipt" in out
    assert binding_of(world).publish_enabled is False


def test_inspect_refuses_to_record_a_reading_without_an_expiry_fact(world: dict[str, object]) -> None:
    world["debug"].update({"expires_at": 0, "is_valid": False})  # type: ignore[attr-defined]
    code, out = run(*INSPECT, "--record")
    assert code == 1 and "records nothing" in out
    assert binding_of(world).token_observed_at is None


def test_inspect_and_status_report_a_measured_expiry_and_its_warning(world: dict[str, object]) -> None:
    soon = datetime.now(UTC) + timedelta(days=3)
    world["debug"]["expires_at"] = int(soon.timestamp())  # type: ignore[index]
    assert run(*INSPECT, "--record")[0] == 0
    code, out = run("meta", "status", "--binding", "bind-facebook_page", "--json")
    report = json.loads(out)
    assert code == 0 and report["token"]["expiry_warning"]["code"] == "token_expires_soon"


def test_inspect_refuses_a_credential_of_another_version(
    world: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("META_CREDENTIAL_VERSION", "v9")
    code, out = run(*INSPECT)
    assert code == 2 and "names 'v1'" in out


def test_the_meta_verbs_refuse_a_linkedin_binding(world: dict[str, object]) -> None:
    from social_publish.adapters.linkedin import neutral_binding
    from tests.conftest import binding as li_binding

    li = neutral_binding(li_binding())
    world["bindings"]._bindings[li.binding_id] = li  # type: ignore[attr-defined]  # noqa: SLF001
    code, out = run("meta", "status", "--binding", li.binding_id)
    assert code == 1 and "use the `account` verbs" in out
