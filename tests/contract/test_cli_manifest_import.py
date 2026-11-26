"""`manifest import`: the operator-CLI route into the ledger (ORG-PLAN-285 C1).

The first canary needs a stored, pending publication to approve, and until this
command the only importer was linkedin-mcp's `import_manifest` tool, which needs a
deployed, authenticated server. These tests run the real CLI against the real
in-memory stores through the two seams and prove the records match the hosted
tool's shape: digest-derived ids, keys taken from the binding, idempotent
re-import, a refused conflicting payload, and never an approval.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from click.testing import CliRunner

from linkedin_publish import InMemoryPublicationStore, cli
from linkedin_publish.bindings import InMemoryBindingStore
from linkedin_publish.manifest import PublishManifest
from linkedin_publish.store import PublicationKey
from tests.conftest import binding

pytestmark = pytest.mark.contract

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "publish-manifest.valid.json"
BINDING = "bind-william-personal"


@pytest.fixture
def stores(monkeypatch: pytest.MonkeyPatch) -> tuple[InMemoryBindingStore, InMemoryPublicationStore]:
    bindings, ledger = InMemoryBindingStore(), InMemoryPublicationStore()
    asyncio.run(bindings.register(binding(binding_id=BINDING, author="urn:li:person:AbC123xyz")))

    @asynccontextmanager
    async def fake_bindings(_dsn: str) -> AsyncIterator[InMemoryBindingStore]:
        yield bindings

    @asynccontextmanager
    async def fake_ledger(_dsn: str) -> AsyncIterator[InMemoryPublicationStore]:
        yield ledger

    monkeypatch.setattr(cli, "_binding_store", fake_bindings)
    monkeypatch.setattr(cli, "_publication_store", fake_ledger)
    return bindings, ledger


def run(path: Path, *extra: str, binding_id: str = BINDING) -> tuple[int, str]:
    result = CliRunner().invoke(
        cli.main,
        ["manifest", "import", str(path), "--binding", binding_id, "--dsn", "postgres://unused", *extra],
    )
    return result.exit_code, result.output


def test_import_stores_unapproved_pending_rows_keyed_like_the_hosted_tool(
    stores: tuple[InMemoryBindingStore, InMemoryPublicationStore],
) -> None:
    _bindings, ledger = stores
    code, output = run(FIXTURE, "--json")
    assert code == 0, output
    body = json.loads(output)
    assert body["approved"] is False
    manifest = PublishManifest.from_json(FIXTURE.read_bytes())
    assert len(body["imported"]) == len(manifest.publications) == 2
    for entry, row in zip(manifest.publications, body["imported"], strict=True):
        digest = entry.digest()
        assert row == {
            "post_id": entry.post_id,
            "publication_id": f"pub_{digest[:24]}",
            "payload_digest": digest,
            "state": "pending",
        }
        stored = asyncio.run(ledger.get_publication(row["publication_id"]))
        assert stored is not None
        assert stored.key == PublicationKey(
            subject_kind="entra_oid",
            subject_id="subject-1",
            campaign_id=manifest.campaign_id,
            post_id=entry.post_id,
            account_id="acct-william",
        )
        assert stored.draft.commentary == entry.commentary
        assert asyncio.run(ledger.get_approval(row["publication_id"])) is None


def test_reimporting_identical_bytes_is_a_no_op(
    stores: tuple[InMemoryBindingStore, InMemoryPublicationStore],
) -> None:
    first = run(FIXTURE, "--json")
    second = run(FIXTURE, "--json")
    assert first[0] == second[0] == 0
    assert json.loads(first[1])["imported"] == json.loads(second[1])["imported"]


def test_a_changed_payload_under_the_same_key_is_refused(
    stores: tuple[InMemoryBindingStore, InMemoryPublicationStore], tmp_path: Path
) -> None:
    assert run(FIXTURE)[0] == 0
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    doc["publications"][0]["commentary"] += " Edited."
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(doc), encoding="utf-8")
    code, output = run(changed)
    assert code == 1
    assert "different payload digest" in output


def test_rows_for_another_binding_store_nothing(
    stores: tuple[InMemoryBindingStore, InMemoryPublicationStore],
) -> None:
    _bindings, ledger = stores
    code, output = run(FIXTURE, binding_id="bind-other")
    assert code == 1
    assert "do not name binding bind-other" in output
    for entry in PublishManifest.from_json(FIXTURE.read_bytes()).publications:
        assert asyncio.run(ledger.get_publication(f"pub_{entry.digest()[:24]}")) is None


def test_an_unregistered_binding_stores_nothing(
    stores: tuple[InMemoryBindingStore, InMemoryPublicationStore], tmp_path: Path
) -> None:
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for row in doc["publications"]:
        row["binding_id"] = "bind-unknown"
    other = tmp_path / "unknown.json"
    other.write_text(json.dumps(doc), encoding="utf-8")
    code, output = run(other, binding_id="bind-unknown")
    assert code == 1
    assert "no binding bind-unknown" in output


def test_an_invalid_manifest_is_refused(
    stores: tuple[InMemoryBindingStore, InMemoryPublicationStore], tmp_path: Path
) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text('{"schema_version": "1.0"}', encoding="utf-8")
    code, output = run(bad)
    assert code == 1
    assert "invalid manifest" in output
