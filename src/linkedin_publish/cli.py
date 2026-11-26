"""Command-line interface.

The CLI is the *trusted operator path*. Two things follow from that and are not
negotiable:

* `approval record` reads the publication FROM THE LEDGER, previews its exact
  stored payload, requires the digest you reviewed as an argument, and requires
  an interactive confirmation before writing the approval. There is no `--yes`.
  Production automation consumes approvals; it never creates them.
* `account commission` writes a grant scoped to one stored, approved
  publication, one digest, one binding and one capability, expiring shortly. A
  scheduler identity cannot reach this command's writer role.
* `publish due` is the one route by which a commissioning grant is used: it sends
  that one granted, approved, due publication through the shared service, as
  the commissioning actor the grant names, with the local credential. The
  scheduler and the MCP never construct a service that can use a grant.
* `account enable` is the first enablement, from the canary's own receipt: it
  marks exactly the receipt's capability `enabled`, names the publication as its
  evidence, and turns publishing on — only once that receipt is `published` on
  the same binding and the current credential is measured healthy.
* `publication reconcile` records what the operator found on LinkedIn for an
  `unknown` send — the post URN, or a failure reason. Nothing else moves an
  `unknown` record, by design.

`account register` and `account inspect` never take a credential as an
argument: registration stores a reference and a version, and inspection reads
the secret from the environment. Registration enables nothing.

Renewal is `account rotate` → `account inspect --record` → `account reenable`.
Rotation moves the binding to the new secret version and DISABLES it in the same
write; `reenable` restores publishing only for a binding that was enabled before
the rotation, and only once the new version has been measured healthy. The
publication ledger is never touched: published and unknown records stay what
they are, and pending approved records go out only after the re-enable.
`account status` reads the stored measurement and the expiry warning without a
credential and without a network call.

`manifest import` stores a manifest as unapproved pending rows (the CLI twin of
the hosted `import_manifest` tool); it approves nothing.

`manifest validate` is fully offline: no credential is read, no network touched,
no database opened. That is the command to reach for when checking a manifest.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, get_args

import click
import httpx

from . import __version__
from .auth import Credentials, TokenObservation, inspect_credentials, member_sub_from_author
from .bindings import IDENTITY_FIELDS, BindingStore, enable_problems, reenable_problems
from .client import default_limits, default_timeout
from .errors import LinkedInPublishError, PublishOutcomeUnknown
from .health import ObservationCacheUnavailable, assess, expiry_warning
from .limits import BudgetStore
from .manifest import PublishManifest
from .models import AccountBinding, Capability, PublishReceipt
from .store import (
    ApprovalRecord,
    CommissioningGrant,
    PublicationKey,
    PublicationRecord,
    PublicationStore,
    StoreConflict,
)
from .version import API_BASE

#: How long a commissioning grant stays usable. Short on purpose — it exists to
#: cover one deliberate canary, not to sit waiting for a convenient moment.
GRANT_TTL = timedelta(minutes=30)

#: The personal app's scopes (plan C1 step 3). Not `email`: it is not needed.
PERSONAL_SCOPES: tuple[str, ...] = ("openid", "profile", "w_member_social")

#: Every capability name, for the commands that take one.
CAPABILITIES: tuple[str, ...] = get_args(Capability)

#: A credential *reference*: a vault secret name or a short URI. A LinkedIn
#: access token is hundreds of characters, so this shape cannot hold one.
_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,126}$")


def _refuse_secret_shaped(credential_ref: str) -> None:
    """Stop a pasted secret from being stored where a reference belongs."""
    for name in ("LINKEDIN_ACCESS_TOKEN", "LINKEDIN_CLIENT_SECRET"):
        value = os.environ.get(name)
        if value and value in credential_ref:
            click.echo(
                f"ABORT: --credential-ref contains the value of {name}. Pass a reference, never the secret.",
                err=True,
            )
            sys.exit(2)
    if not _REFERENCE.match(credential_ref):
        click.echo(
            "ABORT: --credential-ref must be a short reference such as a Key Vault secret name "
            "(letters, digits, . _ : / @ -, at most 127 characters).",
            err=True,
        )
        sys.exit(2)


def _credentials_from_env() -> Credentials:
    """Read the credential from the environment, naming what is missing — never echoing values."""
    missing = [
        name
        for name in ("LINKEDIN_ACCESS_TOKEN", "LINKEDIN_CLIENT_ID", "LINKEDIN_CLIENT_SECRET")
        if not os.environ.get(name)
    ]
    if missing:
        click.echo(
            f"ABORT: set {', '.join(missing)} in the environment (for example from a .env you "
            "filled yourself). Credentials are never accepted as arguments.",
            err=True,
        )
        sys.exit(2)
    return Credentials(
        access_token=os.environ["LINKEDIN_ACCESS_TOKEN"],
        client_id=os.environ["LINKEDIN_CLIENT_ID"],
        client_secret=os.environ["LINKEDIN_CLIENT_SECRET"],
        credential_version=os.environ.get("LINKEDIN_CREDENTIAL_VERSION") or "unversioned",
    )


def _http_client() -> httpx.AsyncClient:
    """The HTTP client the account commands use. A seam for tests."""
    return httpx.AsyncClient(timeout=default_timeout(), limits=default_limits())


def _pg_connect_kwargs() -> dict[str, Any]:
    """How this invocation logs in to Postgres: the root group's --pg-auth, resolved once."""
    from .pg_auth import PgAuthError, pool_kwargs

    context = click.get_current_context(silent=True)
    params = context.find_root().params if context is not None else {}
    try:
        return pool_kwargs(
            str(params.get("pg_auth") or "password"),
            managed_identity_client_id=params.get("pg_identity_client_id") or None,
        )
    except PgAuthError as exc:
        raise click.UsageError(str(exc)) from exc


@asynccontextmanager
async def _binding_store(dsn: str) -> AsyncIterator[BindingStore]:
    """Open the Postgres binding store for one command. A seam for tests."""
    from .postgres import PostgresBindingStore, check_compatible, require_asyncpg

    asyncpg = require_asyncpg()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, **_pg_connect_kwargs())
    try:
        await check_compatible(pool)
        yield PostgresBindingStore(pool)
    finally:
        await pool.close()


@asynccontextmanager
async def _publication_store(dsn: str) -> AsyncIterator[PublicationStore]:
    """Open the Postgres publication ledger for one command. A seam for tests."""
    from .postgres import PostgresPublicationStore, check_compatible, require_asyncpg

    asyncpg = require_asyncpg()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, **_pg_connect_kwargs())
    try:
        await check_compatible(pool)
        yield PostgresPublicationStore(pool)
    finally:
        await pool.close()


@asynccontextmanager
async def _budget_store(dsn: str) -> AsyncIterator[BudgetStore]:
    """Open the shared request counters for one command. A seam for tests.

    The same Postgres counters the scheduler and the MCP reserve against: a CLI
    send is not a free extra quota.
    """
    from .postgres import PostgresBudgetStore, check_compatible, require_asyncpg

    asyncpg = require_asyncpg()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, **_pg_connect_kwargs())
    try:
        await check_compatible(pool)
        yield PostgresBudgetStore(pool)
    finally:
        await pool.close()


async def _load_publication(dsn: str, publication_id: str) -> PublicationRecord:
    async with _publication_store(dsn) as store:
        record = await store.get_publication(publication_id)
    if record is None:
        click.echo(f"ABORT: no publication {publication_id} in the ledger.", err=True)
        sys.exit(1)
    return record


def _show_stored(record: PublicationRecord) -> None:
    """Print exactly what the ledger holds — the bytes an approval is about."""
    click.echo("--- stored publication (from the ledger) ---")
    click.echo(f"  publication  {record.publication_id}   state {record.state}")
    click.echo(f"  binding      {record.binding_id}")
    click.echo(f"  campaign     {record.key.campaign_id}   post {record.key.post_id}")
    click.echo(f"  window       {record.scheduled_at.isoformat()} -> {record.expires_at.isoformat()}")
    click.echo(f"  digest       {record.payload_digest}")
    click.echo(json.dumps(record.draft.model_dump(mode="json"), indent=2, ensure_ascii=False))
    click.echo("--- end ---")


def _require_match(record: PublicationRecord, *, binding_id: str, expected_digest: str) -> None:
    if record.binding_id != binding_id:
        click.echo(
            f"ABORT: {record.publication_id} belongs to binding {record.binding_id}, not {binding_id}.", err=True
        )
        sys.exit(1)
    if record.payload_digest != expected_digest:
        click.echo(
            f"ABORT: the ledger holds digest {record.payload_digest}, not the {expected_digest} you reviewed. "
            "What you reviewed is not what is stored.",
            err=True,
        )
        sys.exit(1)
    if record.state != "pending":
        click.echo(
            f"ABORT: {record.publication_id} is {record.state}; "
            "only a pending publication can be approved or commissioned.",
            err=True,
        )
        sys.exit(1)


def _verdict(observation: TokenObservation, *, probe: bool) -> str:
    """One word for the operator; the fields beside it carry the detail."""
    if observation.token_active is None:
        return "UNKNOWN"
    if observation.known_bad or observation.identity_verified is False or observation.scopes_ok is False:
        return "BAD"
    if probe:
        identity_seen = observation.member_sub is not None or observation.reason == "identity_not_checked_no_oidc"
        return "PROBED" if observation.scopes_ok and identity_seen else "UNKNOWN"
    return "HEALTHY" if observation.healthy else "UNKNOWN"


def _tri(value: bool | None) -> str:
    if value is None:
        return "unknown"
    return "yes" if value else "NO"


def _days_remaining(observation: TokenObservation) -> int | None:
    if observation.expires_at is None:
        return None
    return (observation.expires_at - observation.observed_at).days


@click.group()
@click.version_option(__version__)
@click.option(
    "--pg-auth",
    envvar="LINKEDIN_PUBLISH_PG_AUTH",
    type=click.Choice(["password", "entra"], case_sensitive=False),
    default="password",
    show_default=True,
    help="How ledger connections log in: the DSN's own password, or an Entra token (needs the azure extra).",
)
@click.option(
    "--pg-identity-client-id",
    envvar="LINKEDIN_PUBLISH_PG_IDENTITY_CLIENT_ID",
    default=None,
    help="With --pg-auth entra: the managed identity to take the token as, when the host has several.",
)
def main(pg_auth: str, pg_identity_client_id: str | None) -> None:
    """Official LinkedIn publishing client and durable publication service.

    Read by every command that opens the ledger, through the click context.
    """
    del pg_auth, pg_identity_client_id


# --------------------------------------------------------------------- manifest


@main.group()
def manifest() -> None:
    """Work with publish manifests."""


@manifest.command("validate")
@click.argument("path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def manifest_validate(path: Path, as_json: bool) -> None:
    """Validate a manifest offline and print its canonical digest.

    Reads no credentials and makes no network call.
    """
    try:
        parsed = PublishManifest.from_json(path.read_bytes())
    except Exception as exc:  # noqa: BLE001 - the CLI reports, it does not raise
        if as_json:
            click.echo(json.dumps({"valid": False, "error": str(exc)}, indent=2))
        else:
            click.echo(f"INVALID  {path}", err=True)
            click.echo(f"  {exc}", err=True)
        sys.exit(1)

    entries = [
        {
            "post_id": entry.post_id,
            "binding_id": entry.binding_id,
            "author_urn": entry.author_urn,
            "commentary_code_points": len(entry.commentary),
            "visibility": entry.visibility,
            "media": None if entry.media is None else entry.media.kind,
            "scheduled_at_utc": entry.scheduled_utc().isoformat(),
            "expires_at_utc": entry.expires_utc().isoformat(),
            "digest": entry.digest(),
        }
        for entry in parsed.publications
    ]
    payload = {
        "valid": True,
        "campaign_id": parsed.campaign_id,
        "schema_version": parsed.schema_version,
        "publications": len(parsed.publications),
        "manifest_digest": parsed.digest(),
        "approved": False,
        "entries": entries,
    }

    if as_json:
        click.echo(json.dumps(payload, indent=2))
        return

    click.echo(f"VALID    {path}")
    click.echo(f"campaign {parsed.campaign_id}   schema {parsed.schema_version}")
    click.echo(f"digest   {parsed.digest()}")
    click.echo("status   UNAPPROVED — validation is not authorization to publish")
    for entry in entries:
        click.echo(
            f"  · {entry['post_id']}  {entry['scheduled_at_utc']}  "
            f"{entry['commentary_code_points']} cp  {entry['media'] or 'text'}  "
            f"{entry['digest'][:12]}…"
        )


@manifest.command("import")
@click.argument("path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--binding", "binding_id", required=True, help="The binding every row must name.")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def manifest_import(path: Path, binding_id: str, dsn: str, as_json: bool) -> None:
    """Store a validated manifest as UNAPPROVED pending publications.

    The operator-CLI twin of linkedin-mcp's `import_manifest` tool, for when the
    hosted server is not deployed (the first canary). Same records: the
    publication id is derived from the payload digest and the key from the
    binding, so importing identical bytes twice is a no-op and a changed payload
    under an existing key is refused, never a second post. Nothing here can
    approve: the next step is `approval record` over the printed digest.
    """
    try:
        parsed = PublishManifest.from_json(path.read_bytes())
    except Exception as exc:  # noqa: BLE001 - the CLI reports, it does not raise
        click.echo(f"ABORT: invalid manifest {path}: {exc}", err=True)
        sys.exit(1)
    foreign = [entry.post_id for entry in parsed.publications if entry.binding_id != binding_id]
    if foreign:
        click.echo(f"ABORT: rows {foreign} do not name binding {binding_id}; nothing was stored.", err=True)
        sys.exit(1)

    async def _go() -> list[dict[str, str]]:
        async with _binding_store(dsn) as bindings:
            binding = await bindings.get(binding_id)
        if binding is None:
            click.echo(f"ABORT: no binding {binding_id} in the ledger; nothing was stored.", err=True)
            sys.exit(1)
        stored: list[dict[str, str]] = []
        async with _publication_store(dsn) as store:
            for entry in parsed.publications:
                digest = entry.digest()
                record = PublicationRecord(
                    publication_id=f"pub_{digest[:24]}",
                    key=PublicationKey(
                        subject_kind=binding.subject_kind,
                        subject_id=binding.subject_id,
                        campaign_id=parsed.campaign_id,
                        post_id=entry.post_id,
                        account_id=binding.account_id,
                    ),
                    binding_id=binding.binding_id,
                    payload_digest=digest,
                    draft=entry.as_draft(),
                    scheduled_at=entry.scheduled_utc(),
                    expires_at=entry.expires_utc(),
                )
                try:
                    saved = await store.upsert_publication(record)
                except StoreConflict as exc:
                    click.echo(f"ABORT: {entry.post_id}: {exc}", err=True)
                    sys.exit(1)
                stored.append(
                    {
                        "post_id": entry.post_id,
                        "publication_id": saved.publication_id,
                        "payload_digest": saved.payload_digest,
                        "state": saved.state,
                    }
                )
        return stored

    stored = asyncio.run(_go())
    if as_json:
        click.echo(json.dumps({"campaign_id": parsed.campaign_id, "imported": stored, "approved": False}, indent=2))
        return
    click.echo(f"IMPORTED {len(stored)} publication(s) from {path}, campaign {parsed.campaign_id}")
    for row in stored:
        click.echo(f"  · {row['post_id']}  {row['publication_id']}  {row['state']}  digest {row['payload_digest']}")
    click.echo("status   UNAPPROVED — approve each digest with `linkedin-publish approval record`")


# ---------------------------------------------------------------------- account


@main.group()
def account() -> None:
    """Register, inspect and commission account bindings."""


@account.command("register")
@click.option("--binding", "binding_id", default=None, help="Opaque binding id; generated when omitted.")
@click.option("--account-id", required=True, help="Stable logical account id; survives token rotation.")
@click.option(
    "--subject-kind",
    type=click.Choice(["entra_oid", "service"]),
    default="entra_oid",
    show_default=True,
    help="Kind of the trusted subject allowed to use this binding. Client subjects wait for C7.",
)
@click.option("--subject-id", required=True, help="The trusted subject (e.g. the operator's Entra object id).")
@click.option("--app-id", required=True, help="The LinkedIn app's client id. Not its secret.")
@click.option("--author-urn", required=True, help="urn:li:person:<sub> from `account inspect`, or an organization URN.")
@click.option("--org-urn", "org_urns", multiple=True, help="Organization URN this binding may also author as.")
@click.option(
    "--adapter", type=click.Choice(["share_ugc", "rest_posts"]), default="share_ugc", show_default=True
)
@click.option("--scope", "scopes", multiple=True, help="Declared scope; repeat. Defaults to the personal-app set.")
@click.option(
    "--credential-ref", required=True, help="Where the secret lives (e.g. a Key Vault secret name). Never the value."
)
@click.option("--credential-version", required=True, help="The secret version the reference points at.")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", default=None, help="Writer-role DSN. Omit with --dry-run.")
@click.option("--dry-run", is_flag=True, help="Validate and preview only; write nothing.")
def account_register(
    binding_id: str | None,
    account_id: str,
    subject_kind: str,
    subject_id: str,
    app_id: str,
    author_urn: str,
    org_urns: tuple[str, ...],
    adapter: str,
    scopes: tuple[str, ...],
    credential_ref: str,
    credential_version: str,
    dsn: str | None,
    dry_run: bool,
) -> None:
    """Register an account binding. It is created DISABLED, with nothing commissioned.

    Registration writes configuration, not permission: `publish_enabled` is false
    and every capability is `unknown` until a recorded canary proves it. No
    credential value is accepted here — only a reference and its version.
    """
    _refuse_secret_shaped(credential_ref)
    try:
        binding = AccountBinding(
            binding_id=binding_id or f"bind_{secrets.token_urlsafe(9)}",
            account_id=account_id,
            subject_kind=subject_kind,  # type: ignore[arg-type] - narrowed by click.Choice
            subject_id=subject_id,
            app_id=app_id,
            author_urn=author_urn,
            allowed_organization_urns=org_urns,
            adapter=adapter,  # type: ignore[arg-type] - narrowed by click.Choice
            declared_scopes=tuple(sorted(set(scopes or PERSONAL_SCOPES))),
            credential_ref=credential_ref,
            credential_version=credential_version,
        )
    except ValueError as exc:
        click.echo(f"INVALID  {exc}", err=True)
        sys.exit(1)

    click.echo("Account binding — registered DISABLED; this enables no publishing.")
    for name in ("binding_id", *IDENTITY_FIELDS):
        value = getattr(binding, name)
        rendered = " ".join(value) if isinstance(value, tuple) else str(value)
        click.echo(f"  {name:<26}{rendered or '—'}")
    click.echo(f"  {'publish_enabled':<26}False")

    if dry_run:
        click.echo("dry run — nothing written")
        return
    if not dsn:
        click.echo("ABORT: --dsn (or LINKEDIN_PUBLISH_DSN) is required unless --dry-run.", err=True)
        sys.exit(2)

    click.confirm("Register this binding?", abort=True)

    async def _register() -> bool:
        async with _binding_store(dsn) as store:
            return await store.register(binding)

    try:
        created = asyncio.run(_register())
    except StoreConflict as exc:
        click.echo(f"CONFLICT  {exc}", err=True)
        sys.exit(1)
    click.echo(f"{'registered' if created else 'already registered (identical)'}  {binding.binding_id}")


@account.command("inspect")
@click.option("--binding", "binding_id", default=None, help="Registered binding to check against. Omit to probe.")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", default=None, help="Needed with --binding.")
@click.option("--scope", "scopes", multiple=True, help="Probe mode: scopes to expect. Defaults to the personal set.")
@click.option("--no-identity", is_flag=True, help="Skip /v2/userinfo — for an app without the OIDC product.")
@click.option("--record", is_flag=True, help="Store the measured scopes and expiry on the binding (writer role).")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def account_inspect(
    binding_id: str | None,
    dsn: str | None,
    scopes: tuple[str, ...],
    no_identity: bool,
    record: bool,
    as_json: bool,
) -> None:
    """Measure a token: is it active, which scopes, which app, whose account, when it expires.

    The credential comes from the environment only — LINKEDIN_ACCESS_TOKEN,
    LINKEDIN_CLIENT_ID, LINKEDIN_CLIENT_SECRET and optionally
    LINKEDIN_CREDENTIAL_VERSION — never from an argument, so it cannot land in
    shell history. Nothing is written unless --record is given.

    Without --binding this is a probe: it reports the app-scoped member id you
    need for `account register --author-urn`. Exit 0 healthy, 1 known bad or
    mismatched, 2 unknown (the probe could not complete, or input was missing).
    """
    credentials = _credentials_from_env()
    if record and not binding_id:
        click.echo("ABORT: --record needs --binding; a probe has nowhere to record.", err=True)
        sys.exit(2)
    if binding_id and not dsn:
        click.echo("ABORT: --binding needs --dsn (or LINKEDIN_PUBLISH_DSN) to load it.", err=True)
        sys.exit(2)

    async def _inspect() -> tuple[TokenObservation, AccountBinding | None]:
        binding: AccountBinding | None = None
        if binding_id and dsn:
            async with _binding_store(dsn) as store:
                binding = await store.get(binding_id)
            if binding is None:
                raise click.ClickException(f"binding {binding_id!r} is not registered")
            if binding.app_id != credentials.client_id:
                # Refuse before any request: the secret in hand is for another app.
                raise click.ClickException(
                    f"LINKEDIN_CLIENT_ID is not binding {binding_id!r}'s app ({binding.app_id}); "
                    "this credential does not belong to this binding"
                )
        declared = binding.declared_scopes if binding else tuple(sorted(set(scopes or PERSONAL_SCOPES)))
        async with _http_client() as http:
            observation = await inspect_credentials(
                http,
                credentials,
                declared_scopes=declared,
                expected_app_id=binding.app_id if binding else credentials.client_id,
                expected_member_sub=member_sub_from_author(binding.author_urn) if binding else None,
                check_identity=not no_identity,
                api_base=API_BASE,
            )
        if record and binding and dsn:
            async with _binding_store(dsn) as store:
                binding = await store.record_observation(binding.binding_id, observation)
                # A forced refresh: registration, rotation and renewal all end here,
                # so the health cache holds this probe, keyed by its own version.
                try:
                    await store.observations.put(binding.binding_id, observation)
                    cache_note.append("cached")
                except ObservationCacheUnavailable as exc:
                    cache_note.append(f"not cached: {exc}")
        return observation, binding

    cache_note: list[str] = []
    try:
        observation, binding = asyncio.run(_inspect())
    except StoreConflict as exc:
        click.echo(f"CONFLICT  {exc}", err=True)
        sys.exit(1)

    declared = binding.declared_scopes if binding else tuple(sorted(set(scopes or PERSONAL_SCOPES)))
    verdict = _verdict(observation, probe=binding is None)
    missing = sorted(set(declared) - set(observation.observed_scopes)) if observation.token_active else []
    report: dict[str, object] = {
        "verdict": verdict,
        "binding_id": binding.binding_id if binding else None,
        "credential_version": observation.credential_version,
        "app_id": observation.app_id,
        "token_active": observation.token_active,
        "scopes_ok": observation.scopes_ok,
        "observed_scopes": list(observation.observed_scopes),
        "missing_scopes": missing,
        "identity_verified": observation.identity_verified,
        "member_sub": observation.member_sub,
        "author_urn": f"urn:li:person:{observation.member_sub}" if observation.member_sub else None,
        "expires_at": observation.expires_at.isoformat() if observation.expires_at else None,
        "days_remaining": _days_remaining(observation),
        "observed_at": observation.observed_at.isoformat(),
        "reason": observation.reason,
        "recorded": bool(record and binding),
        "health_cache": cache_note[0] if cache_note else None,
    }
    if binding is not None:
        warning = expiry_warning(binding, observation, now=observation.observed_at)
        report["expiry_warning"] = warning.as_dict() if warning else None

    if as_json:
        click.echo(json.dumps(report, indent=2))
    else:
        click.echo(f"{verdict:<9}{'probe (no binding)' if binding is None else binding.binding_id}")
        click.echo(f"  token active     {_tri(observation.token_active)}")
        click.echo(f"  app              {observation.app_id or '—'}")
        click.echo(f"  scopes           {' '.join(observation.observed_scopes) or '—'}")
        if missing:
            click.echo(f"  missing scopes   {' '.join(missing)}")
        click.echo(f"  identity         {_tri(observation.identity_verified)}")
        if observation.member_sub:
            click.echo(f"  author urn       urn:li:person:{observation.member_sub}")
        if observation.expires_at:
            click.echo(
                f"  expires          {observation.expires_at.isoformat()}  "
                f"({report['days_remaining']} days; measured, not assumed)"
            )
        if observation.reason:
            click.echo(f"  reason           {observation.reason}")
        if record and binding:
            click.echo("  recorded         scopes + expiry stored on the binding")
            if cache_note:
                click.echo(f"  health cache     {cache_note[0]}")
        if report.get("expiry_warning"):
            click.echo(f"  WARNING          {report['expiry_warning']['code']} — re-mint, then `account rotate`")
    sys.exit({"HEALTHY": 0, "PROBED": 0, "BAD": 1}.get(verdict, 2))


@account.command("rotate")
@click.option("--binding", "binding_id", required=True, help="The binding whose credential is being renewed.")
@click.option(
    "--credential-ref",
    required=True,
    help="Where the NEW secret lives (e.g. a Key Vault secret name). Never the value.",
)
@click.option("--credential-version", required=True, help="The new secret version the reference points at.")
@click.option(
    "--from-version",
    "expected_version",
    default=None,
    help="The version being replaced. Defaults to the binding's current one; pass it to refuse a stale view.",
)
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
@click.option("--dry-run", is_flag=True, help="Show the change and write nothing.")
def account_rotate(
    binding_id: str,
    credential_ref: str,
    credential_version: str,
    expected_version: str | None,
    dsn: str,
    dry_run: bool,
) -> None:
    """Move a binding to a new credential version, and DISABLE it until re-measured.

    The same app, member and adapter: approvals and the publication ledger carry
    over, and nothing already published or unknown is reset. The stored
    measurement and the health cache are cleared, so nothing reports the new
    secret healthy on the old one's probe. Then run `account inspect --binding
    <id> --record` with the new secret in the environment, and `account
    reenable` to restore publishing if the rotation turned it off.
    """
    _refuse_secret_shaped(credential_ref)

    async def _load() -> AccountBinding | None:
        async with _binding_store(dsn) as store:
            return await store.get(binding_id)

    current = asyncio.run(_load())
    if current is None:
        click.echo(f"ABORT: binding {binding_id!r} is not registered.", err=True)
        sys.exit(1)
    replacing = expected_version or current.credential_version

    click.echo("Credential rotation — the binding is DISABLED until the new secret is measured.")
    click.echo(f"  binding                   {current.binding_id}")
    click.echo(f"  credential_ref            {current.credential_ref}  ->  {credential_ref}")
    click.echo(f"  credential_version        {replacing}  ->  {credential_version}")
    click.echo(f"  publish_enabled           {current.publish_enabled}  ->  False")
    click.echo(
        "  re-enable after measure   "
        f"{'yes' if current.publish_enabled or current.reenable_after_measure else 'no (it was not enabled)'}"
    )
    if dry_run:
        click.echo("dry run — nothing written")
        return
    click.confirm("Rotate this binding's credential?", abort=True)

    async def _rotate() -> tuple[AccountBinding, str]:
        async with _binding_store(dsn) as store:
            moved = await store.rotate(
                binding_id,
                expected_version=replacing,
                credential_ref=credential_ref,
                credential_version=credential_version,
                at=datetime.now(timezone.utc),
            )
            try:
                dropped = await store.observations.invalidate(binding_id)
                note = f"health cache invalidated ({dropped} observation(s) dropped)"
            except ObservationCacheUnavailable as exc:
                note = f"health cache not invalidated: {exc}"
        return moved, note

    try:
        moved, note = asyncio.run(_rotate())
    except StoreConflict as exc:
        click.echo(f"CONFLICT  {exc}", err=True)
        sys.exit(1)
    click.echo(f"rotated  {moved.binding_id}  now {moved.credential_version}, publishing DISABLED")
    click.echo(f"  {note}")
    click.echo(
        f"next: with the new secret in the environment, `account inspect --binding {moved.binding_id} --record`"
        + (f", then `account reenable --binding {moved.binding_id}`" if moved.reenable_after_measure else "")
    )


@account.command("reenable")
@click.option("--binding", "binding_id", required=True)
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
def account_reenable(binding_id: str, dsn: str) -> None:
    """Restore publishing on a rotated binding, after a healthy re-measure.

    Refuses unless the binding was enabled before its rotation, the current
    credential version was measured after the rotation, and that measurement is
    healthy with the declared scopes and a future expiry. This is not the
    canary's first enablement, and it cannot become one.
    """

    async def _check() -> tuple[AccountBinding | None, list[str]]:
        async with _binding_store(dsn) as store:
            binding = await store.get(binding_id)
            if binding is None:
                return None, []
            try:
                observation = await store.observations.get(binding_id, binding.credential_version)
            except ObservationCacheUnavailable:
                observation = None
        return binding, reenable_problems(binding, observation, now=datetime.now(timezone.utc))

    binding, problems = asyncio.run(_check())
    if binding is None:
        click.echo(f"ABORT: binding {binding_id!r} is not registered.", err=True)
        sys.exit(1)
    if problems:
        click.echo(f"REFUSED  {binding_id} stays disabled:", err=True)
        for problem in problems:
            click.echo(f"  - {problem}", err=True)
        sys.exit(1)

    click.echo(f"Re-enable publishing on {binding_id} (credential {binding.credential_version}).")
    click.echo("Pending approved publications that are due will be sent by the next live tick.")
    click.confirm("Re-enable?", abort=True)

    async def _enable() -> AccountBinding:
        async with _binding_store(dsn) as store:
            return await store.reenable(binding_id, expected_version=binding.credential_version)

    try:
        asyncio.run(_enable())
    except StoreConflict as exc:
        click.echo(f"CONFLICT  {exc}", err=True)
        sys.exit(1)
    click.echo(f"re-enabled  {binding_id}")


@account.command("status")
@click.option("--binding", "binding_id", required=True)
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="A DSN that can read the binding.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def account_status(binding_id: str, dsn: str, as_json: bool) -> None:
    """The stored token health of a binding: no credential read, no request made.

    Reports the last measured expiry, the cached observation for the current
    credential version, the verdict over them and the <=14-day expiry warning.
    Exit 0 healthy, 1 blocked (known bad or expired), 2 unknown.
    """

    async def _read() -> tuple[AccountBinding | None, object, str | None]:
        async with _binding_store(dsn) as store:
            binding = await store.get(binding_id)
            if binding is None:
                return None, None, None
            try:
                return binding, await store.observations.get(binding_id, binding.credential_version), None
            except ObservationCacheUnavailable as exc:
                return binding, None, str(exc)

    binding, observation, cache_problem = asyncio.run(_read())
    if binding is None:
        click.echo(f"ABORT: binding {binding_id!r} is not registered.", err=True)
        sys.exit(1)
    now = datetime.now(timezone.utc)
    health = assess(binding, observation, now=now)  # type: ignore[arg-type] - narrowed above
    report: dict[str, object] = {
        "binding_id": binding.binding_id,
        "credential_version": binding.credential_version,
        "publish_enabled": binding.publish_enabled,
        "token_expires_at": binding.token_expires_at.isoformat() if binding.token_expires_at else None,
        "token_observed_at": binding.token_observed_at.isoformat() if binding.token_observed_at else None,
        "rotated_at": binding.rotated_at.isoformat() if binding.rotated_at else None,
        "reenable_after_measure": binding.reenable_after_measure,
        "health": health.as_dict(),
        "health_cache": cache_problem or ("hit" if observation is not None else "miss"),
        "checked_at": now.isoformat(),
    }
    if as_json:
        click.echo(json.dumps(report, indent=2))
    else:
        click.echo(f"{health.status.upper():<9}{binding.binding_id}  ({health.reason or 'measured healthy'})")
        click.echo(f"  credential       {binding.credential_version}   publish_enabled {binding.publish_enabled}")
        click.echo(f"  measured expiry  {report['token_expires_at'] or '—'}")
        click.echo(f"  last measured    {report['token_observed_at'] or '—'}")
        click.echo(f"  health cache     {report['health_cache']}")
        if health.expiry_warning is not None:
            warning = health.expiry_warning
            click.echo(
                f"  WARNING          {warning.code}: {warning.days_remaining} days (window {warning.window_days})"
            )
    sys.exit({"healthy": 0, "blocked": 1}.get(health.status, 2))


@account.command("commission")
@click.option("--publication", "publication_id", required=True)
@click.option("--digest", "expected_digest", required=True, help="The digest you reviewed.")
@click.option("--binding", "binding_id", required=True)
@click.option(
    "--capability",
    required=True,
    type=click.Choice(CAPABILITIES),
    help="The capability this publication's shape needs (a text canary: text).",
)
@click.option("--actor", required=True, help="The operator commissioning identity.")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
def account_commission(
    publication_id: str,
    expected_digest: str,
    binding_id: str,
    capability: str,
    actor: str,
    dsn: str,
) -> None:
    """Write a one-shot grant for exactly one approved canary publication.

    The grant does not enable the binding and does not enable the capability. It
    licenses a single send so that the capability can be enabled afterwards, from
    the receipt it produces. The publication must already carry an active
    approval for the same digest.
    """
    record = asyncio.run(_load_publication(dsn, publication_id))
    _show_stored(record)
    _require_match(record, binding_id=binding_id, expected_digest=expected_digest)
    if record.draft.required_capability != capability:
        click.echo(
            f"ABORT: {publication_id} is a {record.draft.required_capability!r} post; a grant for "
            f"{capability!r} would license nothing. A canary enables its own shape only.",
            err=True,
        )
        sys.exit(1)

    async def approved() -> bool:
        async with _publication_store(dsn) as store:
            approval = await store.get_approval(publication_id)
        return approval is not None and approval.is_active(digest=expected_digest, now=datetime.now(timezone.utc))

    if not asyncio.run(approved()):
        click.echo(
            f"ABORT: {publication_id} has no active approval for this digest. Run `approval record` first.",
            err=True,
        )
        sys.exit(1)

    click.echo("Commissioning grant — this authorizes ONE real outward-facing post.")
    click.echo(f"  capability   {capability}")
    click.echo(f"  expires      {GRANT_TTL}")
    click.confirm("Issue this grant?", abort=True)

    now = datetime.now(timezone.utc)
    grant = CommissioningGrant(
        grant_id=f"grant_{secrets.token_urlsafe(12)}",
        publication_id=publication_id,
        payload_digest=expected_digest,
        binding_id=binding_id,
        capability=capability,
        issued_by=actor,
        issued_at=now,
        expires_at=now + GRANT_TTL,
    )

    async def write() -> None:
        async with _publication_store(dsn) as store:
            await store.put_grant(grant)

    asyncio.run(write())
    click.echo(f"granted {grant.grant_id} (expires {grant.expires_at.isoformat()})")
    click.echo(
        f"next: `publish due --binding {binding_id} --publication {publication_id} --actor {actor}` "
        "with the local credential in the environment"
    )


@account.command("enable")
@click.option("--binding", "binding_id", required=True)
@click.option("--capability", required=True, type=click.Choice(CAPABILITIES), help="The capability the receipt proved.")
@click.option(
    "--evidence",
    "evidence_publication_id",
    required=True,
    help="The canary publication whose recorded receipt proves the capability.",
)
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
@click.option("--dry-run", is_flag=True, help="Show the change and write nothing.")
def account_enable(binding_id: str, capability: str, evidence_publication_id: str, dsn: str, dry_run: bool) -> None:
    """Enable ONE capability from its canary receipt, and turn publishing on.

    Plan C1 step 8. Refuses unless the evidence publication is a `published`
    receipt with a post URN on this binding, of exactly this capability's shape,
    the capability is still `unknown`, the binding is not held after a rotation,
    and the current credential is measured healthy. A text canary enables text
    and nothing else.
    """
    cap: Capability = capability  # type: ignore[assignment] - narrowed by click.Choice

    async def _read() -> tuple[AccountBinding | None, TokenObservation | None, PublicationRecord | None]:
        async with _binding_store(dsn) as store:
            binding = await store.get(binding_id)
            observation = None
            if binding is not None:
                try:
                    observation = await store.observations.get(binding_id, binding.credential_version)
                except ObservationCacheUnavailable:
                    observation = None
        async with _publication_store(dsn) as ledger:
            evidence = await ledger.get_publication(evidence_publication_id)
        return binding, observation, evidence

    binding, observation, evidence = asyncio.run(_read())
    if binding is None:
        click.echo(f"ABORT: binding {binding_id!r} is not registered.", err=True)
        sys.exit(1)
    problems = enable_problems(
        binding, evidence, capability=cap, observation=observation, now=datetime.now(timezone.utc)
    )
    if problems:
        click.echo(f"REFUSED  {binding_id} capability {capability} stays {binding.capability_state(cap)}:", err=True)
        for problem in problems:
            click.echo(f"  - {problem}", err=True)
        sys.exit(1)
    assert evidence is not None and evidence.published_at is not None  # noqa: S101 - enable_problems checked it

    click.echo("First enablement from a canary receipt.")
    click.echo(f"  binding            {binding.binding_id}   credential {binding.credential_version}")
    click.echo(f"  evidence           {evidence.publication_id}   {evidence.post_urn}")
    click.echo(f"  published at       {evidence.published_at.isoformat()}   via {evidence.adapter or binding.adapter}")
    click.echo(f"  capability {capability:<7} {binding.capability_state(cap)}  ->  enabled")
    click.echo(f"  publish_enabled    {binding.publish_enabled}  ->  True")
    others = [f"{item.capability}={item.state}" for item in binding.capabilities if item.capability != capability]
    click.echo(f"  other capabilities {', '.join(others) or 'none (all unknown)'}  (unchanged)")
    if dry_run:
        click.echo("dry run — nothing written")
        return
    click.confirm(f"Enable {capability} on {binding_id} and turn publishing on?", abort=True)

    async def _write() -> AccountBinding:
        async with _binding_store(dsn) as store:
            return await store.enable_capability(
                binding_id,
                capability=cap,
                evidence_publication_id=evidence.publication_id,
                observed_at=evidence.published_at,  # type: ignore[arg-type] - asserted above
                expected_version=binding.credential_version,
            )

    try:
        updated = asyncio.run(_write())
    except StoreConflict as exc:
        click.echo(f"CONFLICT  {exc}", err=True)
        sys.exit(1)
    click.echo(f"enabled  {updated.binding_id}  {capability} (evidence {evidence.publication_id}); publishing ON")


# --------------------------------------------------------------------- publish


@main.group()
def publish() -> None:
    """Send stored, approved publications from the operator's machine."""


@publish.command("due")
@click.option("--binding", "binding_id", required=True)
@click.option("--publication", "publication_id", required=True, help="The one commissioned publication to send.")
@click.option(
    "--actor",
    required=True,
    help="The commissioning identity the grant was issued to (`account commission --actor`).",
)
@click.option(
    "--dsn",
    envvar="LINKEDIN_PUBLISH_DSN",
    required=True,
    help="A DSN acting as linkedin_publish_commissioner (migration 0006): send transitions, the grant and quota only.",
)
def publish_due_command(binding_id: str, publication_id: str, actor: str, dsn: str) -> None:
    """Send ONE commissioned canary through the shared service, as the commissioning actor.

    Plan C1 step 7. The same `PublicationService.publish_one` path the scheduler
    uses — claim, every gate re-read, durable `sending`, then the request — with
    the service built for this one actor, so it may use a grant issued to that
    actor for exactly this publication, digest, binding and capability. The grant
    is consumed at `sending`. The credential is read from the environment and
    must be the version the binding names. Exit 0 published, 1 refused or
    failed, 3 outcome unknown (look at LinkedIn, then `publication reconcile`).
    """
    from .auth import StaticCredentialProvider
    from .client import LinkedInClient
    from .limits import QuotaLimiter, profile_for
    from .service import PublicationService

    async def _read() -> tuple[AccountBinding | None, PublicationRecord | None, CommissioningGrant | None]:
        async with _binding_store(dsn) as store:
            binding = await store.get(binding_id)
        async with _publication_store(dsn) as ledger:
            record = await ledger.get_publication(publication_id)
            grant = await ledger.get_grant(publication_id)
        return binding, record, grant

    binding, record, grant = asyncio.run(_read())
    if binding is None:
        click.echo(f"ABORT: binding {binding_id!r} is not registered.", err=True)
        sys.exit(1)
    if record is None:
        click.echo(f"ABORT: no publication {publication_id} in the ledger.", err=True)
        sys.exit(1)
    _show_stored(record)
    if record.binding_id != binding_id:
        click.echo(f"ABORT: {publication_id} belongs to binding {record.binding_id}, not {binding_id}.", err=True)
        sys.exit(1)
    if record.state != "pending":
        click.echo(f"ABORT: {publication_id} is {record.state}; only a pending publication is sent.", err=True)
        sys.exit(1)
    credentials = _credentials_from_env()
    if credentials.credential_version != binding.credential_version:
        click.echo(
            f"ABORT: the environment holds credential version {credentials.credential_version!r} but binding "
            f"{binding_id} names {binding.credential_version!r}. Set LINKEDIN_CREDENTIAL_VERSION to the "
            "measured version, or rotate first.",
            err=True,
        )
        sys.exit(2)

    capability = record.draft.required_capability
    click.echo("Send — this is ONE real outward-facing post.")
    click.echo(f"  binding          {binding.binding_id}   adapter {binding.adapter}")
    click.echo(f"  publish_enabled  {binding.publish_enabled}")
    click.echo(f"  capability       {capability} ({binding.capability_state(capability)})")
    if grant is None:
        click.echo("  grant            none")
    else:
        spent = f"consumed {grant.consumed_at.isoformat()}" if grant.consumed_at else "unconsumed"
        click.echo(
            f"  grant            {grant.grant_id} for {grant.capability}, issued to {grant.issued_by}, "
            f"expires {grant.expires_at.isoformat()}, {spent}"
        )
    click.confirm(f"Send publication {publication_id} now, as {actor}?", abort=True)

    async def _send() -> tuple[PublishReceipt | None, LinkedInPublishError | None]:
        async with _binding_store(dsn) as bindings, _publication_store(dsn) as ledger, _budget_store(dsn) as budgets:
            async with _http_client() as http:
                client = LinkedInClient(http=http, limiter=QuotaLimiter(budgets, profile_for(binding.adapter)))
                service = PublicationService(
                    ledger,
                    client,
                    StaticCredentialProvider(credentials),
                    actor=actor,
                    commissioning_actor=actor,
                    observations=bindings.observations,
                )
                try:
                    receipt = await service.publish_one(
                        publication_id, binding, expected_digest=record.payload_digest
                    )
                except LinkedInPublishError as exc:
                    return None, exc
                return receipt, None

    receipt, failure = asyncio.run(_send())
    if failure is not None:
        detail = failure.as_dict()
        verdict = "UNKNOWN" if isinstance(failure, PublishOutcomeUnknown) else "REFUSED"
        click.echo(f"{verdict}  {publication_id}: {detail.get('code')} — {failure.message}", err=True)
        for key in ("http_status", "provider_code", "request_id", "capability"):
            if detail.get(key) is not None:
                click.echo(f"  {key:<14} {detail[key]}", err=True)
        if verdict == "UNKNOWN":
            click.echo(
                "  never re-send: look at LinkedIn, then `publication reconcile` with what you found", err=True
            )
            sys.exit(3)
        sys.exit(1)
    assert receipt is not None  # noqa: S101 - one of the two is set
    click.echo(f"published  {publication_id}")
    click.echo(f"  post_urn      {receipt.post_urn}")
    click.echo(f"  permalink     {receipt.permalink}")
    click.echo(f"  adapter       {receipt.adapter}")
    click.echo(f"  published_at  {receipt.published_at.isoformat()}")
    if binding.capability_state(capability) != "enabled":
        click.echo(
            f"next: check the post, then `account enable --binding {binding_id} --capability {capability} "
            f"--evidence {publication_id}`"
        )


# --------------------------------------------------------------------- approval


@main.group()
def approval() -> None:
    """Record and revoke approvals. There is no auto-yes flag here."""


@approval.command("record")
@click.option("--publication", "publication_id", required=True)
@click.option("--digest", "expected_digest", required=True, help="The digest you reviewed.")
@click.option("--binding", "binding_id", required=True)
@click.option("--actor", required=True, help="The operator approval-writer identity.")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
def approval_record(
    publication_id: str,
    expected_digest: str,
    binding_id: str,
    actor: str,
    dsn: str,
) -> None:
    """Approve the exact stored payload of one publication.

    The payload is read from the ledger and shown in full before the prompt, and
    the digest you pass must equal the stored one. A mismatch aborts — it means
    what you reviewed is not what is stored.
    """
    record = asyncio.run(_load_publication(dsn, publication_id))
    _show_stored(record)
    _require_match(record, binding_id=binding_id, expected_digest=expected_digest)

    click.confirm(f"Approve publication {publication_id} for binding {binding_id}?", abort=True)

    approval_row = ApprovalRecord(
        approval_id=f"appr_{secrets.token_urlsafe(12)}",
        publication_id=publication_id,
        payload_digest=record.payload_digest,
        binding_id=binding_id,
        approved_by=actor,
        approved_at=datetime.now(timezone.utc),
    )

    async def write() -> None:
        async with _publication_store(dsn) as store:
            await store.put_approval(approval_row)

    asyncio.run(write())
    click.echo(f"approved {approval_row.approval_id}")


# ----------------------------------------------------------------- publication


@main.group()
def publication() -> None:
    """Operator dispositions on stored publications."""


@publication.command("reconcile")
@click.option("--publication", "publication_id", required=True)
@click.option("--actor", required=True, help="The operator identity recording the evidence.")
@click.option("--post-urn", default=None, help="The post URN found on LinkedIn (urn:li:share:… or urn:li:ugcPost:…).")
@click.option("--failure-reason", default=None, help="What you found instead: the post does not exist.")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True, help="Writer-role DSN.")
def publication_reconcile(
    publication_id: str,
    actor: str,
    post_urn: str | None,
    failure_reason: str | None,
    dsn: str,
) -> None:
    """Record what actually happened to an `unknown` send.

    Look at LinkedIn first. If the post is there, pass its URN; if it is not,
    pass a failure reason. Nothing automated ever moves an `unknown` record, so
    this is the only way out of that state — and it never re-sends.
    """
    from .errors import ValidationFailure
    from .service import reconcile_publication

    if (post_urn is None) == (failure_reason is None):
        click.echo("ABORT: pass exactly one of --post-urn or --failure-reason.", err=True)
        sys.exit(2)

    record = asyncio.run(_load_publication(dsn, publication_id))
    _show_stored(record)
    if record.state != "unknown":
        click.echo(f"ABORT: {publication_id} is {record.state}; only unknown records are reconciled.", err=True)
        sys.exit(1)

    outcome = f"published as {post_urn}" if post_urn else f"failed: {failure_reason}"
    click.confirm(f"Record {publication_id} as {outcome}?", abort=True)

    async def write() -> PublicationRecord:
        async with _publication_store(dsn) as store:
            return await reconcile_publication(
                store, publication_id, actor=actor, post_urn=post_urn, failure_reason=failure_reason
            )

    try:
        updated = asyncio.run(write())
    except ValidationFailure as exc:
        click.echo(f"ABORT: {exc}", err=True)
        sys.exit(1)
    click.echo(f"reconciled {publication_id}: {updated.state}")


# ---------------------------------------------------------------------- db


@main.group()
def db() -> None:
    """Schema management. Run under the migration identity, never the runtime."""


@db.command("migrate")
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True)
@click.option("--actor", default="migrator")
def db_migrate(dsn: str, actor: str) -> None:
    """Apply pending migrations under an advisory lock and checksum ledger."""
    from .postgres import apply_migrations

    applied = asyncio.run(apply_migrations(dsn, applied_by=actor, connect_kwargs=_pg_connect_kwargs()))
    if applied:
        for version in applied:
            click.echo(f"applied {version}")
    else:
        click.echo("already up to date")


@db.command("plan")
def db_plan() -> None:
    """List the migrations this build ships, without connecting to anything."""
    from .postgres import REQUIRED_MIGRATION, migration_files

    for path in migration_files():
        click.echo(f"{path.stem}  {path.name}")
    click.echo(f"runtime requires: {REQUIRED_MIGRATION}")


if __name__ == "__main__":
    main()
