"""The CLI surface, and the schema artifact it ships.

`manifest validate` is the one command an operator runs before anything exists —
no database, no credential, no network. The tests below assert exactly that, by
running it against the shipped fixture and checking it touches nothing else.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from linkedin_publish.cli import main
from linkedin_publish.manifest import PublishManifest

pytestmark = pytest.mark.contract

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
SCHEMA = Path(__file__).resolve().parents[2] / "src" / "linkedin_publish" / "schemas" / "publish-manifest-v1.json"


def run(*args: str) -> tuple[int, str]:
    result = CliRunner().invoke(main, list(args))
    return result.exit_code, result.output


def test_manifest_validate_reports_valid_and_prints_the_digest() -> None:
    code, output = run("manifest", "validate", str(FIXTURES / "publish-manifest.valid.json"))
    assert code == 0
    assert "VALID" in output
    expected = PublishManifest.from_json((FIXTURES / "publish-manifest.valid.json").read_bytes()).digest()
    assert expected in output


def test_manifest_validate_says_plainly_that_validation_is_not_approval() -> None:
    """An operator must not read "VALID" as "cleared to publish"."""
    _code, output = run("manifest", "validate", str(FIXTURES / "publish-manifest.valid.json"))
    assert "UNAPPROVED" in output


def test_manifest_validate_emits_machine_readable_json() -> None:
    code, output = run("manifest", "validate", str(FIXTURES / "publish-manifest.valid.json"), "--json")
    assert code == 0
    payload = json.loads(output)
    assert payload["valid"] is True
    assert payload["approved"] is False
    assert payload["publications"] == 2
    assert len(payload["manifest_digest"]) == 64
    assert all(len(entry["digest"]) == 64 for entry in payload["entries"])


def test_manifest_validate_reads_no_credential_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fully offline: clearing every credential variable changes nothing."""
    for name in ("LINKEDIN_ACCESS_TOKEN", "LINKEDIN_CLIENT_ID", "LINKEDIN_CLIENT_SECRET", "LINKEDIN_PUBLISH_DSN"):
        monkeypatch.delenv(name, raising=False)
    code, _output = run("manifest", "validate", str(FIXTURES / "publish-manifest.valid.json"))
    assert code == 0


def test_an_invalid_manifest_exits_non_zero_and_names_the_problem(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"schema_version": "9.9", "campaign_id": "x", "publications": []}))
    code, output = run("manifest", "validate", str(bad), "--json")
    assert code == 1
    assert json.loads(output)["valid"] is False
    assert "schema_version" in output


def test_approval_record_has_no_auto_yes_flag() -> None:
    """Production automation consumes approvals. It never creates one."""
    _code, output = run("approval", "record", "--help")
    assert "--yes" not in output
    assert "--force" not in output
    assert "--digest" in output


def test_db_plan_lists_migrations_without_connecting() -> None:
    code, output = run("db", "plan")
    assert code == 0
    assert "0001_initial" in output
    assert "0002_roles" in output
    assert "runtime requires" in output


def test_the_shipped_json_schema_matches_the_model() -> None:
    """A schema artifact that drifts from the model is worse than none."""
    current = PublishManifest.model_json_schema()
    shipped = json.loads(SCHEMA.read_text())
    for key in ("$id", "title"):
        shipped.pop(key, None)
        current.pop(key, None)
    assert shipped == current, "regenerate src/linkedin_publish/schemas/publish-manifest-v1.json"
