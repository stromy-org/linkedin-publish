"""`meta register`, `meta rotate` and `meta reenable` (ORG-PLAN-652 C5a, F41 and F39).

The Meta twins of LinkedIn's `account register`, `account rotate` and `account
reenable`. The database and Graph are replaced at the CLI's seams
(`_social_binding_store`, `_social_store`, `_http_client`), the way
`test_cli_meta.py` walks the canary. Synchronous on purpose: the CLI calls
`asyncio.run` itself.

Negative controls (each fails without its change; see the PR body):
* `test_register_writes_a_disabled_binding_with_nothing_commissioned` — the binding
  handed to the store is the one the CLI built: an enabled one fails it.
* `test_rotate_holds_the_binding_and_a_grant_is_not_honoured_while_held` — a rotation
  that leaves no hold lets the open grant send.
* `test_reenable_needs_a_fresh_measurement_of_the_new_version` — a re-enable that
  skips the measurement check turns publishing back on from nothing.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from click.testing import CliRunner

from linkedin_publish import cli
from social_publish import (
    DispatchService,
    InMemorySocialBindingStore,
    InMemorySocialStore,
    OperationCapability,
    Refused,
    SocialBinding,
    new_grant,
    reenable_problems,
    registration_problems,
)
from social_publish.errors import StoreConflict
from tests.social_fakes import TOKEN, FakeGraph, adapter, approval_for, meta_binding, meta_payload, record_for

pytestmark = pytest.mark.contract

DSN = ["--dsn", "postgres://unused"]
ACTOR = "fixture-operator"
BIND = "bind-facebook_page"
PAGE = "100000000000001"
IG = "170000000000001"


class SpyBindings(InMemorySocialBindingStore):
    """The in-memory store, recording exactly what the CLI asked it to register."""

    def __init__(self, bindings: list[SocialBinding] | None = None) -> None:
        super().__init__(bindings)
        self.registered: list[SocialBinding] = []

    async def register(self, binding: SocialBinding) -> bool:
        self.registered.append(binding)
        return await super().register(binding)


def _live(binding: SocialBinding) -> SocialBinding:
    """An enabled, measured Facebook binding on credential v1, one capability proven."""
    now = datetime.now(UTC)
    return binding.model_copy(
        update={
            "publish_enabled": True,
            "token_never_expires": True,
            "token_observed_at": now - timedelta(hours=2),
            "capabilities": (
                OperationCapability(
                    operation="schedule_post",
                    format="text",
                    state="enabled",
                    observed_at=now - timedelta(days=1),
                    evidence_publication_id="house-receipt",
                ),
            ),
        }
    )


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    binding = _live(meta_binding(publish_enabled=False))
    bindings = SpyBindings([binding])
    debug: dict[str, object] = {"expires_at": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/debug_token"), request.url
        data = {"app_id": binding.app_id, "is_valid": True, "scopes": ["pages_manage_posts"], **debug}
        return httpx.Response(200, json={"data": data})

    @asynccontextmanager
    async def fake_bindings(_dsn: str) -> AsyncIterator[SpyBindings]:
        yield bindings

    monkeypatch.setattr(cli, "_social_binding_store", fake_bindings)
    monkeypatch.setattr(cli, "_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    monkeypatch.setenv("META_ACCESS_TOKEN", TOKEN)
    monkeypatch.setenv("META_CREDENTIAL_VERSION", "v1")
    return {"bindings": bindings, "debug": debug}


def run(*args: str, stdin: str = "y\n") -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, [*args, *DSN], input=stdin)
    return result.exit_code, result.output


def stored(world: dict[str, object], binding_id: str = BIND) -> SocialBinding:
    found = asyncio.run(world["bindings"].get(binding_id))  # type: ignore[attr-defined]
    assert found is not None
    return found


def register_args(**overrides: str) -> list[str]:
    values = {
        "--binding": "bind-ig-new",
        "--account-id": "ig-main",
        "--platform": "instagram_professional",
        "--native-account-id": IG,
        "--app-id": "app-organic",
        "--subject-id": "fixture-brand",
        "--credential-ref": "kv-meta-fixture-brand",
        "--credential-version": "ver-1",
        **overrides,
    }
    return ["meta", "register", *(part for pair in values.items() for part in pair)]


ROTATE = [
    "meta",
    "rotate",
    "--binding",
    BIND,
    "--credential-ref",
    "kv-meta-fixture-brand",
    "--credential-version",
    "v2",
]
REENABLE = ["meta", "reenable", "--binding", BIND]


# ------------------------------------------------------------------ register


def test_register_writes_a_disabled_binding_with_nothing_commissioned(world: dict[str, object]) -> None:
    code, out = run(*register_args(), "--dry-run")
    assert code == 0 and "registered DISABLED" in out and "dry run" in out, out
    assert world["bindings"].registered == []  # type: ignore[attr-defined]
    assert asyncio.run(world["bindings"].get("bind-ig-new")) is None  # type: ignore[attr-defined]

    code, out = run(*register_args(), stdin="n\n")
    assert code == 1 and world["bindings"].registered == []  # type: ignore[attr-defined]  # a "no" writes nothing

    code, out = run(*register_args())
    assert code == 0 and "registered  bind-ig-new" in out and "meta inspect --binding bind-ig-new" in out, out
    (sent,) = world["bindings"].registered  # type: ignore[attr-defined]
    assert sent.publish_enabled is False and sent.capabilities == ()  # what the CLI built, not what the store forced
    assert sent.reenable_after_measure is False and sent.token_observed_at is None
    assert (sent.platform, sent.native_account_id, sent.adapter) == ("instagram_professional", IG, "meta_graph")
    assert (sent.app_id, sent.credential_version, sent.subject_id) == ("app-organic", "ver-1", "fixture-brand")
    assert sent.author_urn is None
    got = stored(world, "bind-ig-new")
    assert got.publish_enabled is False and got.capability_state("publish_post", "feed_image") == "unknown"


def test_register_refuses_by_name_and_writes_nothing(world: dict[str, object]) -> None:
    cases = [
        (register_args(**{"--platform": "linkedin"}), "`account register`"),
        (register_args(**{"--platform": "tiktok"}), "not a Meta platform"),
        (register_args(**{"--native-account-id": "page-name"}), "not a numeric Page or Instagram user id"),
        (register_args(**{"--adapter": "share_ugc"}), "uses the meta_graph adapter"),
        (register_args(**{"--binding": BIND, "--platform": "instagram_professional"}), "already registered"),
        (register_args(**{"--binding": "bind-dup", "--platform": "facebook_page", "--native-account-id": PAGE}),
         "already names facebook_page account"),
        (register_args(**{"--credential-version": " "}), "credential version"),
    ]  # fmt: skip
    for args, message in cases:
        code, out = run(*args)
        assert code == 1 and "REFUSED" in out and message in out, (args, out)
    assert world["bindings"].registered == []  # type: ignore[attr-defined]


def test_register_accepts_no_token_and_refuses_one_pasted_as_a_reference(
    world: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    code, out = run(*register_args(), "--access-token", TOKEN)
    assert code == 2 and "No such option" in out
    code, out = run(*register_args(**{"--credential-ref": TOKEN}))
    assert code == 2 and "META_ACCESS_TOKEN" in out and TOKEN not in out.replace("META_ACCESS_TOKEN", "")
    code, out = run(*register_args(**{"--credential-ref": "x" * 300}))
    assert code == 2 and "short reference" in out
    assert world["bindings"].registered == []  # type: ignore[attr-defined]


def test_register_dry_run_needs_no_database(monkeypatch: pytest.MonkeyPatch) -> None:
    result = CliRunner().invoke(cli.main, [*register_args(), "--dry-run"])
    assert result.exit_code == 0 and "dry run" in result.output, result.output
    result = CliRunner().invoke(cli.main, register_args(), input="y\n")
    assert result.exit_code == 2 and "--dsn" in result.output


def test_registration_problems_is_pure_and_names_each_refusal() -> None:
    ok = {
        "platform": "facebook_page",
        "native_account_id": PAGE,
        "adapter": "meta_graph",
        "app_id": "app-organic",
        "credential_version": "v1",
        "existing": None,
        "same_account": None,
    }
    assert registration_problems(**ok) == []  # type: ignore[arg-type]
    twin = meta_binding()
    assert any("one account has one binding" in p for p in registration_problems(**{**ok, "same_account": twin}))  # type: ignore[arg-type]


# -------------------------------------------------------------------- rotate


def test_rotate_holds_the_binding_and_a_grant_is_not_honoured_while_held(world: dict[str, object]) -> None:
    before = stored(world)
    assert before.publish_enabled and not before.reenable_after_measure

    code, out = run(*ROTATE, "--dry-run")
    assert code == 0 and "v1  ->  v2" in out and "dry run" in out, out
    assert stored(world) == before  # nothing written

    code, out = run(*ROTATE, stdin="n\n")
    assert code == 1 and stored(world) == before

    code, out = run(*ROTATE)
    assert code == 0 and "publishing HELD" in out and "meta reenable --binding" in out, out
    held = stored(world)
    assert held.credential_version == "v2" and held.publish_enabled is False
    assert held.reenable_after_measure is True and held.rotated_at is not None
    assert held.token_observed_at is None and held.token_expires_at is None and not held.token_never_expires
    assert held.capabilities == before.capabilities  # what was proven about the account is kept

    # End to end: an open grant for an unknown pair is NOT honoured on the held binding.
    async def scenario() -> None:
        now = datetime.now(UTC)
        binding = held
        record = record_for(
            binding,
            meta_payload(fmt="link", links=("https://fixture-brand.example/news",), slot=now + timedelta(hours=2)),
            publication_id="canary-2",
            scheduled_at=now - timedelta(minutes=5),
            expires_at=now + timedelta(hours=1),
        )
        ledger = InMemorySocialStore([binding])
        await ledger.import_intent(record, approval_for(record), actor=ACTOR, at=now)
        await ledger.put_grant(new_grant(record, actor=ACTOR, now=now))
        graph = FakeGraph()
        service = DispatchService(ledger, {"meta_graph": adapter(graph)}, actor=ACTOR, commissioning_actor=ACTOR)
        with pytest.raises(Refused, match="publish_disabled"):
            await service.dispatch_one(binding, "canary-2", now=now)
        assert graph.writes() == []
        assert await ledger.get_grant("canary-2") is not None  # not spent either

    asyncio.run(scenario())

    # And the re-enable path names what is missing.
    problems = reenable_problems(held, now=datetime.now(UTC))
    assert any("measure" in p or "no token evidence" in p for p in problems)


def test_rotate_refuses_a_stale_view_the_same_version_and_a_linkedin_binding(world: dict[str, object]) -> None:
    code, out = run(*ROTATE, "--from-version", "v0")
    assert code == 1 and "CONFLICT" in out and "'v0'" in out
    same = [*ROTATE[:-1], "v1"]
    code, out = run(*same)
    assert code == 1 and "a rotation needs a new version" in out
    assert stored(world).credential_version == "v1" and stored(world).publish_enabled

    code, out = run("meta", "rotate", "--binding", BIND, "--credential-ref", TOKEN, "--credential-version", "v2")
    assert code == 2 and "META_ACCESS_TOKEN" in out

    from social_publish.adapters.linkedin import neutral_binding
    from tests.conftest import binding as li_binding

    li = neutral_binding(li_binding())
    world["bindings"]._bindings[li.binding_id] = li  # type: ignore[attr-defined]  # noqa: SLF001
    code, out = run(
        "meta", "rotate", "--binding", li.binding_id, "--credential-ref", "kv", "--credential-version", "v2"
    )
    assert code == 1 and "use the `account` verbs" in out


def test_a_second_rotation_before_reenabling_keeps_the_hold_and_a_never_enabled_binding_gets_none(
    world: dict[str, object],
) -> None:
    assert run(*ROTATE)[0] == 0
    again = [*ROTATE[:-1], "v3"]
    assert run(*again)[0] == 0
    assert stored(world).reenable_after_measure is True and stored(world).credential_version == "v3"

    fresh = meta_binding("instagram_professional", publish_enabled=False, binding_id="bind-ig-never")
    asyncio.run(world["bindings"].register(fresh))  # type: ignore[attr-defined]
    code, out = run(
        "meta", "rotate", "--binding", "bind-ig-never", "--credential-ref", "kv", "--credential-version", "v2"
    )
    assert code == 0
    never = stored(world, "bind-ig-never")
    assert never.reenable_after_measure is False and never.publish_enabled is False
    code, out = run("meta", "reenable", "--binding", "bind-ig-never")
    assert code == 1 and "first enablement is the canary path" in out


# ------------------------------------------------------------------ reenable


def test_reenable_needs_a_fresh_measurement_of_the_new_version(
    world: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(*ROTATE)[0] == 0

    # Held and unmeasured: refused, and the refusal names the measurement.
    code, out = run(*REENABLE)
    assert code == 1 and "stays held" in out and "no token evidence" in out and "meta inspect" in out, out
    assert stored(world).publish_enabled is False and stored(world).reenable_after_measure is True

    # The measurement of the OLD secret cannot vouch for the new one: inspect refuses the version mismatch.
    code, out = run("meta", "inspect", "--binding", BIND, "--record")
    assert code == 2 and "names 'v2'" in out
    assert stored(world).token_observed_at is None

    # Measure the new secret, then re-enable.
    monkeypatch.setenv("META_CREDENTIAL_VERSION", "v2")
    code, out = run("meta", "inspect", "--binding", BIND, "--record")
    assert code == 0, out
    code, out = run(*REENABLE, "--dry-run")
    assert code == 0 and "dry run" in out and stored(world).publish_enabled is False
    code, out = run(*REENABLE, stdin="n\n")
    assert code == 1 and stored(world).publish_enabled is False
    code, out = run(*REENABLE)
    assert code == 0 and f"re-enabled  {BIND}" in out, out
    back = stored(world)
    assert back.publish_enabled and back.reenable_after_measure is False and back.credential_version == "v2"
    assert back.capability_state("schedule_post", "text") == "enabled"  # restored, not re-proven

    code, out = run(*REENABLE)
    assert code == 1 and "publishing is already enabled" in out


def test_reenable_refuses_a_stale_or_expired_measurement_and_the_store_rechecks() -> None:
    now = datetime.now(UTC)
    rotated_at = now - timedelta(minutes=10)
    held = _live(meta_binding(publish_enabled=False)).model_copy(
        update={
            "publish_enabled": False,
            "reenable_after_measure": True,
            "rotated_at": rotated_at,
            "credential_version": "v2",
        }
    )
    ok = held.model_copy(update={"token_observed_at": now - timedelta(minutes=1)})
    assert reenable_problems(ok, now=now) == []
    stale = held.model_copy(update={"token_observed_at": rotated_at - timedelta(hours=1)})
    assert any("predates the rotation" in p for p in reenable_problems(stale, now=now))
    expired = held.model_copy(
        update={
            "token_never_expires": False,
            "token_observed_at": now - timedelta(minutes=1),
            "token_expires_at": now - timedelta(seconds=1),
        }
    )
    assert any("expired" in p for p in reenable_problems(expired, now=now))
    unmeasured = held.model_copy(update={"token_observed_at": None, "token_never_expires": False})
    assert any("no token evidence" in p for p in reenable_problems(unmeasured, now=now))

    # The store re-checks under its own lock: a caller that skipped the CLI's check is refused too.
    store = InMemorySocialBindingStore([unmeasured])
    with pytest.raises(StoreConflict, match="no token evidence"):
        asyncio.run(store.reenable(unmeasured.binding_id, expected_version="v2", now=now))
    assert asyncio.run(store.get(unmeasured.binding_id)).publish_enabled is False  # type: ignore[union-attr]
    with pytest.raises(StoreConflict, match="changed since"):
        asyncio.run(InMemorySocialBindingStore([ok]).reenable(ok.binding_id, expected_version="v1", now=now))
