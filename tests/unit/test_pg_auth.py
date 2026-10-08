"""The ledger's Postgres login: password by default, Entra as a plug-in, anything else by callable."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from linkedin_publish import cli
from linkedin_publish.pg_auth import ENTRA_SCOPE, PgAuthError, entra_password_provider, pool_kwargs

pytestmark = pytest.mark.unit


class FakeCredential:
    def __init__(self) -> None:
        self.scopes: list[str] = []
        self.issued = 0

    async def get_token(self, scope: str) -> SimpleNamespace:
        self.scopes.append(scope)
        self.issued += 1
        return SimpleNamespace(token=f"token-{self.issued}")


def test_password_mode_adds_nothing_so_the_dsn_is_used_as_written() -> None:
    assert pool_kwargs() == {}
    assert pool_kwargs("password") == {}
    assert pool_kwargs(" Password ") == {}


async def test_entra_asks_for_the_postgres_resource_and_a_fresh_token_per_connection() -> None:
    credential = FakeCredential()
    provide = entra_password_provider(credential=credential)

    assert await provide() == "token-1"
    assert await provide() == "token-2"
    assert credential.scopes == [ENTRA_SCOPE, ENTRA_SCOPE]


def test_an_unknown_mode_is_refused_rather_than_falling_back_to_the_dsn() -> None:
    with pytest.raises(PgAuthError, match="unknown Postgres auth mode 'aws'"):
        pool_kwargs("aws")


async def test_a_caller_supplied_provider_is_the_door_for_any_other_cloud() -> None:
    async def rds_iam_token() -> str:
        return "iam-token"

    kwargs = pool_kwargs("entra", password_provider=rds_iam_token)
    assert kwargs == {"password": rds_iam_token}
    assert await kwargs["password"]() == "iam-token"


def test_entra_without_the_azure_extra_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def no_azure(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("azure.identity"):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_azure)
    with pytest.raises(Exception, match=r"linkedin-publish\[azure\]"):
        entra_password_provider()


def _migrate_capturing(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    async def fake_apply(dsn: str, *, applied_by: str, connect_kwargs: dict[str, Any] | None = None) -> list[str]:
        seen["dsn"] = dsn
        seen["connect_kwargs"] = connect_kwargs
        return []

    monkeypatch.setattr("linkedin_publish.postgres.apply_migrations", fake_apply)
    return seen


def test_cli_defaults_to_password_login(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _migrate_capturing(monkeypatch)
    result = CliRunner().invoke(cli.main, ["db", "migrate", "--dsn", "postgresql://u:p@h/db"], env={})
    assert result.exit_code == 0, result.output
    assert seen["connect_kwargs"] == {}


def test_cli_entra_login_from_the_environment_reaches_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _migrate_capturing(monkeypatch)
    calls: list[str | None] = []

    def fake_provider(*, managed_identity_client_id: str | None = None) -> Any:
        calls.append(managed_identity_client_id)
        return "provider"

    monkeypatch.setattr("linkedin_publish.pg_auth.entra_password_provider", fake_provider)
    result = CliRunner().invoke(
        cli.main,
        ["db", "migrate", "--dsn", "postgresql://principal@h/db?sslmode=require"],
        env={"LINKEDIN_PUBLISH_PG_AUTH": "entra", "LINKEDIN_PUBLISH_PG_IDENTITY_CLIENT_ID": "client-1"},
    )
    assert result.exit_code == 0, result.output
    assert seen["connect_kwargs"] == {"password": "provider"}
    assert calls == ["client-1"]
