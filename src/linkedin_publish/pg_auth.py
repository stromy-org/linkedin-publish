"""How the ledger authenticates to Postgres — plain Postgres by default, cloud logins as plug-ins.

The ledger needs nothing but Postgres, and that is what keeps it portable: a
client can point it at their own Azure server, AWS RDS, or any managed Postgres.
What differs between those is only *how a connection proves who it is*, so that
is the one seam this module owns:

* ``password`` (the default) — whatever the DSN carries. A local container, a
  fixture, or a server that still accepts password logins. Nothing is fetched.
* ``entra`` — Azure Database for PostgreSQL with Microsoft Entra authentication.
  The DSN names the database principal and carries no password; each new
  connection presents a short-lived access token instead. Needs the ``azure``
  extra.

Any other cloud login (an IAM auth token, a secrets-manager rotation) is a
:data:`PasswordProvider` the caller passes to :func:`pool_kwargs` directly — the
library never needs to learn the cloud's name.

asyncpg calls a callable ``password`` once per new physical connection, which is
exactly the cadence a short-lived token needs: a pool that reconnects after an
hour gets a fresh token, never the one it was born with. Like the rest of this
library, nothing here reads the environment; the CLI and the deployment do.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Final

from .exceptions import DependencyError, LinkedinPublishError

__all__ = [
    "AUTH_MODES",
    "ENTRA_SCOPE",
    "PasswordProvider",
    "PgAuthError",
    "entra_password_provider",
    "pool_kwargs",
]

#: A coroutine function returning the secret for one new connection.
PasswordProvider = Callable[[], Awaitable[str]]

#: The modes :func:`pool_kwargs` resolves by name.
AUTH_MODES: Final = ("password", "entra")

#: The resource Azure Database for PostgreSQL accepts Entra tokens for.
ENTRA_SCOPE: Final = "https://ossrdbms-aad.database.windows.net/.default"


class PgAuthError(LinkedinPublishError):
    """The requested Postgres authentication mode cannot be used."""


def entra_password_provider(
    *,
    managed_identity_client_id: str | None = None,
    credential: Any = None,
) -> PasswordProvider:
    """Return a provider that answers each new connection with an Entra token.

    ``credential`` is any object with an async ``get_token(scope)`` — tests pass a
    fake; production leaves it ``None`` and gets ``DefaultAzureCredential``, which
    resolves a user-assigned managed identity in Azure and the operator's
    ``az login`` on a laptop. Pass ``managed_identity_client_id`` when the host
    carries more than one identity, so the token is for the principal the DSN names.
    """
    if credential is None:
        try:
            from azure.identity.aio import DefaultAzureCredential
        except ModuleNotFoundError as exc:
            raise DependencyError("azure", "azure-identity") from exc
        credential = DefaultAzureCredential(managed_identity_client_id=managed_identity_client_id)

    async def provide() -> str:
        token = await credential.get_token(ENTRA_SCOPE)
        return str(token.token)

    return provide


def pool_kwargs(
    auth: str = "password",
    *,
    managed_identity_client_id: str | None = None,
    password_provider: PasswordProvider | None = None,
) -> dict[str, Any]:
    """Extra keyword arguments for ``asyncpg.connect`` / ``asyncpg.create_pool``.

    An explicit ``password_provider`` wins over ``auth`` — that is the door for a
    login this module does not name. An unknown mode is refused rather than
    silently falling back to the DSN, because a fallback is how a deployment
    meant to use a managed identity ends up trying an empty password.
    """
    if password_provider is not None:
        return {"password": password_provider}
    mode = (auth or "password").strip().lower()
    if mode == "password":
        return {}
    if mode == "entra":
        return {"password": entra_password_provider(managed_identity_client_id=managed_identity_client_id)}
    raise PgAuthError(f"unknown Postgres auth mode {auth!r}; expected one of {', '.join(AUTH_MODES)}")
