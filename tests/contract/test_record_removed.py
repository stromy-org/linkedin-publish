"""`publication record-removed`: a published post deleted outside the rail (Fleet Social T29).

The ledger kept saying `published` after the account owner deleted a post by
hand, and that receipt could still be offered as `account enable --evidence`.
These tests pin the disposition: only `published` moves, to the terminal
`removed`; the receipt stays; who/why is an event; LinkedIn is never called; and
a removed post is never enable evidence.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from click.testing import CliRunner

from linkedin_publish import InMemoryPublicationStore, ValidationFailure, cli
from linkedin_publish.bindings import enable_problems
from linkedin_publish.service import record_publication_removed
from linkedin_publish.store import StoreConflict
from social_publish.states import LINKEDIN_STATES, LINKEDIN_TRANSITIONS, NEVER_FRESH, TRANSITIONS
from tests.conftest import NOW, binding
from tests.contract.test_cli_ledger import ledger, run, seed  # noqa: F401 - the shared ledger fixture
from tests.contract.test_service import POST_URN, record

pytestmark = pytest.mark.contract

PERMALINK = f"https://www.linkedin.com/feed/update/{POST_URN}/"
OTHER_STATES = ["pending", "claimed", "sending", "unknown", "failed", "expired", "cancelled", "removed"]
REASON = "deleted by the account owner on LinkedIn"
REMOVE = ["publication", "record-removed", "--publication", "pub-1", "--actor", "william", "--reason", REASON]


def published(**extra: object):  # noqa: ANN201
    return record(state="published", **extra).model_copy(
        update={"post_urn": POST_URN, "permalink": PERMALINK, "published_at": NOW, "adapter": "share_ugc"}
    )


async def stored(**extra: object) -> InMemoryPublicationStore:
    store = InMemoryPublicationStore()
    await store.upsert_publication(published(**extra))
    return store


async def test_a_published_record_becomes_removed_and_keeps_its_receipt() -> None:
    store = await stored()
    moment = NOW + timedelta(hours=3)
    updated = await record_publication_removed(store, "pub-1", actor="william", reason=REASON, now=moment)
    assert updated.state == "removed"
    assert (updated.post_urn, updated.permalink, updated.published_at) == (POST_URN, PERMALINK, NOW)
    assert updated.failure_code is None
    (event,) = await store.events("pub-1")
    assert (event.kind, event.actor, event.at) == ("removed_externally", "william", moment)
    assert event.detail["reason"] == REASON
    assert event.detail["post_urn"] == POST_URN


@pytest.mark.parametrize("state", OTHER_STATES)
async def test_every_other_source_state_is_refused_and_unchanged(state: str) -> None:
    store = InMemoryPublicationStore()
    await store.upsert_publication(record(state=state))
    with pytest.raises(ValidationFailure, match="only published records"):
        await record_publication_removed(store, "pub-1", actor="william", reason=REASON)
    after = await store.get_publication("pub-1")
    assert after is not None and after.state == state
    assert await store.events("pub-1") == []


async def test_an_unknown_publication_is_refused() -> None:
    with pytest.raises(ValidationFailure, match="unknown publication"):
        await record_publication_removed(InMemoryPublicationStore(), "nope", actor="w", reason=REASON)


@pytest.mark.parametrize(("actor", "reason"), [("", REASON), ("  ", REASON), ("william", ""), ("william", "  ")])
async def test_actor_and_reason_are_required(actor: str, reason: str) -> None:
    store = await stored()
    with pytest.raises(ValidationFailure):
        await record_publication_removed(store, "pub-1", actor=actor, reason=reason)
    after = await store.get_publication("pub-1")
    assert after is not None and after.state == "published"


async def test_removed_is_terminal() -> None:
    store = await stored()
    await record_publication_removed(store, "pub-1", actor="william", reason=REASON)
    for target in ("published", "pending", "failed", "unknown"):
        with pytest.raises(StoreConflict, match="illegal transition"):
            await store.compare_and_set(
                "pub-1", expected_state="removed", expected_attempt_token=None, updates={"state": target}
            )


def test_the_state_tables_know_removed() -> None:
    assert "removed" in LINKEDIN_STATES and "removed" in NEVER_FRESH
    assert LINKEDIN_TRANSITIONS["published"] == {"removed"} and TRANSITIONS["published"] == {"removed"}
    # nothing else may reach it
    assert [s for s, t in TRANSITIONS.items() if "removed" in t] == ["published"]
    assert TRANSITIONS["removed"] == frozenset()


async def test_a_removed_post_is_refused_as_enable_evidence_and_a_published_one_is_not() -> None:
    bound = binding(capabilities=())
    live = published()
    gone = live.model_copy(update={"state": "removed"})
    live_problems = enable_problems(bound, live, capability="text", observation=None, now=NOW)
    gone_problems = enable_problems(bound, gone, capability="text", observation=None, now=NOW)
    assert not [p for p in live_problems if "evidence" in p]
    assert any("removed outside the rail" in p for p in gone_problems)


# --------------------------------------------------------------------- CLI


def test_the_cli_records_the_removal(ledger: InMemoryPublicationStore) -> None:  # noqa: F811
    asyncio.run(ledger.upsert_publication(published()))
    code, output = run(*REMOVE)
    assert code == 0, output
    after = asyncio.run(ledger.get_publication("pub-1"))
    assert after is not None and (after.state, after.post_urn) == ("removed", POST_URN)
    events = asyncio.run(ledger.events("pub-1"))
    assert [(e.kind, e.actor) for e in events] == [("removed_externally", "william")]


@pytest.mark.parametrize("state", ["unknown", "pending", "failed", "removed"])
def test_the_cli_refuses_every_other_state(ledger: InMemoryPublicationStore, state: str) -> None:  # noqa: F811
    seed(ledger, state=state)
    code, output = run(*REMOVE)
    assert code == 1 and "only published records" in output
    after = asyncio.run(ledger.get_publication("pub-1"))
    assert after is not None and after.state == state


def test_the_cli_needs_actor_and_reason(ledger: InMemoryPublicationStore) -> None:  # noqa: F811
    asyncio.run(ledger.upsert_publication(published()))
    no_reason = ["publication", "record-removed", "--publication", "pub-1", "--actor", "william"]
    assert run(*no_reason)[0] == 2
    blank = ["publication", "record-removed", "--publication", "pub-1", "--actor", "william", "--reason", " "]
    assert run(*blank)[0] == 2
    assert CliRunner().invoke(cli.main, ["publication", "record-removed", "--publication", "pub-1",
                                         "--reason", REASON, "--dsn", "x"]).exit_code == 2
    after = asyncio.run(ledger.get_publication("pub-1"))
    assert after is not None and after.state == "published"


def test_declining_the_prompt_changes_nothing(ledger: InMemoryPublicationStore) -> None:  # noqa: F811
    asyncio.run(ledger.upsert_publication(published()))
    code, _output = run(*REMOVE, stdin="n\n")
    assert code != 0
    after = asyncio.run(ledger.get_publication("pub-1"))
    assert after is not None and after.state == "published"
