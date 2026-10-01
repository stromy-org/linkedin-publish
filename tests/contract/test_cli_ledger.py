"""The operator commands that write the ledger: approve, commission, reconcile.

Until v0.3.0 `approval record` and `account commission` only printed a JSON
record, and nothing ever put it in the ledger — so an operator had no way to
approve anything the publisher would send, and no command reached an `unknown`
send at all. These tests run the real CLI against the real in-memory ledger
through the one seam (`_publication_store`), and the end-to-end case proves an
approval written here is the one `publish_due` honours.

The tests are synchronous on purpose: the CLI calls `asyncio.run` itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from click.testing import CliRunner

import linkedin_publish
from linkedin_publish import InMemoryPublicationStore, PublicationService, StaticCredentialProvider
from linkedin_publish import cli
from tests.conftest import NOW, binding, make_client
from tests.contract.test_service import DIGEST, POST_URN, created, record

pytestmark = pytest.mark.contract

OTHER = "e" * 64


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> InMemoryPublicationStore:
    shared = InMemoryPublicationStore()

    @asynccontextmanager
    async def fake(_dsn: str) -> AsyncIterator[InMemoryPublicationStore]:
        yield shared

    monkeypatch.setattr(cli, "_publication_store", fake)
    return shared


def seed(store: InMemoryPublicationStore, **kwargs: object) -> None:
    asyncio.run(store.upsert_publication(record(**kwargs)))  # type: ignore[arg-type]


def run(*args: str, stdin: str = "y\n") -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, [*args, "--dsn", "postgres://unused"], input=stdin)
    return result.exit_code, result.output


APPROVE = ["approval", "record", "--publication", "pub-1", "--binding", "bind-1", "--actor", "william"]


# ------------------------------------------------------------------ approval


def test_approval_shows_the_stored_payload_and_writes_the_approval(ledger: InMemoryPublicationStore) -> None:
    seed(ledger)
    code, output = run(*APPROVE, "--digest", DIGEST)
    assert code == 0, output
    assert "stored publication (from the ledger)" in output
    assert "Intelligence, orchestrated." in output
    stored = asyncio.run(ledger.get_approval("pub-1"))
    assert stored is not None
    assert stored.payload_digest == DIGEST
    assert stored.approved_by == "william"


def test_approval_refuses_a_digest_that_is_not_the_stored_one(ledger: InMemoryPublicationStore) -> None:
    seed(ledger)
    code, output = run(*APPROVE, "--digest", OTHER)
    assert code == 1
    assert "not what is stored" in output
    assert asyncio.run(ledger.get_approval("pub-1")) is None


def test_declining_the_prompt_writes_no_approval(ledger: InMemoryPublicationStore) -> None:
    seed(ledger)
    code, _output = run(*APPROVE, "--digest", DIGEST, stdin="n\n")
    assert code != 0
    assert asyncio.run(ledger.get_approval("pub-1")) is None


def test_approval_refuses_another_bindings_publication(ledger: InMemoryPublicationStore) -> None:
    seed(ledger)
    code, output = run(
        "approval", "record", "--publication", "pub-1", "--binding", "bind-2", "--actor", "w", "--digest", DIGEST
    )
    assert code == 1
    assert "belongs to binding bind-1" in output


def test_approval_refuses_a_publication_that_is_no_longer_pending(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="unknown")
    code, output = run(*APPROVE, "--digest", DIGEST)
    assert code == 1
    assert "only a pending publication" in output


def test_approval_takes_no_local_payload_file() -> None:
    """The bytes approved are the ledger's, never a file the operator points at."""
    result = CliRunner().invoke(cli.main, ["approval", "record", "--help"])
    assert "--payload" not in result.output
    assert "--yes" not in result.output


def test_an_approval_written_by_the_cli_is_the_one_the_publisher_honours(
    ledger: InMemoryPublicationStore,
) -> None:
    """The gap this release closes: before it, nothing could ever be sent."""
    seed(ledger)

    async def tick() -> tuple[int, int]:
        client, log = make_client(created)
        credentials = StaticCredentialProvider(
            linkedin_publish.Credentials(
                access_token="t", client_id="app-personal", client_secret="s", credential_version="v1"
            )
        )
        service = PublicationService(ledger, client, credentials)
        result = await service.publish_due(binding(), campaign_id="camp-1", now=NOW, dry_run=False)
        await client.aclose()
        return result.published, log.count

    published, requests = asyncio.run(tick())
    assert (published, requests) == (0, 0), "an unapproved row must not be sent"

    code, output = run(*APPROVE, "--digest", DIGEST)
    assert code == 0, output
    published, requests = asyncio.run(tick())
    assert published == 1
    assert requests == 1


# ---------------------------------------------------------------- commission


COMMISSION = [
    "account", "commission", "--publication", "pub-1", "--binding", "bind-1",
    "--capability", "text", "--actor", "william",
]  # fmt: skip


def test_commission_refuses_an_unapproved_publication(ledger: InMemoryPublicationStore) -> None:
    seed(ledger)
    code, output = run(*COMMISSION, "--digest", DIGEST)
    assert code == 1
    assert "no active approval" in output
    assert asyncio.run(ledger.get_grant("pub-1")) is None


def test_commission_writes_one_grant_for_an_approved_publication(ledger: InMemoryPublicationStore) -> None:
    seed(ledger)
    assert run(*APPROVE, "--digest", DIGEST)[0] == 0
    code, output = run(*COMMISSION, "--digest", DIGEST)
    assert code == 0, output
    grant = asyncio.run(ledger.get_grant("pub-1"))
    assert grant is not None
    assert (grant.payload_digest, grant.binding_id, grant.capability) == (DIGEST, "bind-1", "text")
    assert grant.consumed_at is None


# ----------------------------------------------------------------- reconcile


RECONCILE = ["publication", "reconcile", "--publication", "pub-1", "--actor", "william"]


def test_reconcile_records_a_post_the_operator_found(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="unknown")
    code, output = run(*RECONCILE, "--post-urn", POST_URN)
    assert code == 0, output
    stored = asyncio.run(ledger.get_publication("pub-1"))
    assert stored is not None
    assert stored.state == "published"
    assert stored.post_urn == POST_URN
    events = asyncio.run(ledger.events("pub-1"))
    assert [(e.kind, e.actor) for e in events] == [("reconciled_published", "william")]


def test_reconcile_records_a_send_that_never_appeared(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="unknown")
    code, _output = run(*RECONCILE, "--failure-reason", "no such post on the profile at 09:10")
    assert code == 0
    stored = asyncio.run(ledger.get_publication("pub-1"))
    assert stored is not None
    assert (stored.state, stored.failure_code) == ("failed", "operator_disposition")


def test_reconcile_never_touches_a_record_with_a_known_outcome(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="pending")
    code, output = run(*RECONCILE, "--post-urn", POST_URN)
    assert code == 1
    assert "only unknown records" in output
    stored = asyncio.run(ledger.get_publication("pub-1"))
    assert stored is not None and stored.state == "pending"


def test_reconcile_refuses_something_that_is_not_a_post_urn(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="unknown")
    code, output = run(*RECONCILE, "--post-urn", "urn:li:person:AbC123xyz")
    assert code == 1
    assert "post_urn" in output
    stored = asyncio.run(ledger.get_publication("pub-1"))
    assert stored is not None and stored.state == "unknown"


def test_reconcile_needs_exactly_one_outcome(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="unknown")
    assert run(*RECONCILE)[0] == 2
    assert run(*RECONCILE, "--post-urn", POST_URN, "--failure-reason", "both")[0] == 2


def test_declining_the_reconcile_prompt_changes_nothing(ledger: InMemoryPublicationStore) -> None:
    seed(ledger, state="unknown")
    code, _output = run(*RECONCILE, "--post-urn", POST_URN, stdin="n\n")
    assert code != 0
    stored = asyncio.run(ledger.get_publication("pub-1"))
    assert stored is not None and stored.state == "unknown"
