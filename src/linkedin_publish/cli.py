"""Command-line interface.

The CLI is the *trusted operator path*. Two things follow from that and are not
negotiable:

* `approval record` previews the exact stored bytes, requires the expected
  digest as an argument, and requires an interactive confirmation. There is no
  `--yes`. Production automation consumes approvals; it never creates them.
* `account commission` issues a grant scoped to one stored publication, one
  digest, one binding and one capability, expiring shortly. A scheduler identity
  cannot reach this command's writer role.

`account register` and `account inspect` never take a credential as an
argument: registration stores a reference and a version, and inspection reads
the secret from the environment. Registration enables nothing.

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

import click
import httpx

from . import __version__
from .auth import Credentials, TokenObservation, inspect_credentials, member_sub_from_author
from .bindings import IDENTITY_FIELDS, BindingStore
from .client import default_limits, default_timeout
from .manifest import PublishManifest
from .models import AccountBinding
from .store import ApprovalRecord, CommissioningGrant, StoreConflict
from .version import API_BASE

#: How long a commissioning grant stays usable. Short on purpose — it exists to
#: cover one deliberate canary, not to sit waiting for a convenient moment.
GRANT_TTL = timedelta(minutes=30)

#: The personal app's scopes (plan C1 step 3). Not `email`: it is not needed.
PERSONAL_SCOPES: tuple[str, ...] = ("openid", "profile", "w_member_social")

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


@asynccontextmanager
async def _binding_store(dsn: str) -> AsyncIterator[BindingStore]:
    """Open the Postgres binding store for one command. A seam for tests."""
    from .postgres import PostgresBindingStore, check_compatible, require_asyncpg

    asyncpg = require_asyncpg()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        await check_compatible(pool)
        yield PostgresBindingStore(pool)
    finally:
        await pool.close()


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
def main() -> None:
    """Official LinkedIn publishing client and durable publication service."""


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
        return observation, binding

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
    }

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
    sys.exit({"HEALTHY": 0, "PROBED": 0, "BAD": 1}.get(verdict, 2))


@account.command("commission")
@click.option("--publication", "publication_id", required=True)
@click.option("--digest", "expected_digest", required=True)
@click.option("--binding", "binding_id", required=True)
@click.option("--capability", required=True)
@click.option("--actor", required=True, help="The operator commissioning identity.")
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), default=None)
def account_commission(
    publication_id: str,
    expected_digest: str,
    binding_id: str,
    capability: str,
    actor: str,
    out: Path | None,
) -> None:
    """Issue a one-shot grant for exactly one approved canary publication.

    The grant does not enable the binding and does not enable the capability. It
    licenses a single send so that the capability can be enabled afterwards, from
    the receipt it produces.
    """
    click.echo("Commissioning grant — this authorizes ONE real outward-facing post.")
    click.echo(f"  publication  {publication_id}")
    click.echo(f"  digest       {expected_digest}")
    click.echo(f"  binding      {binding_id}")
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
    rendered = grant.model_dump_json(indent=2)
    if out is not None:
        out.write_text(rendered)
        click.echo(f"wrote {out}")
    else:
        click.echo(rendered)


# --------------------------------------------------------------------- approval


@main.group()
def approval() -> None:
    """Record and revoke approvals. There is no auto-yes flag here."""


@approval.command("record")
@click.option("--publication", "publication_id", required=True)
@click.option("--digest", "expected_digest", required=True, help="The digest you reviewed.")
@click.option("--binding", "binding_id", required=True)
@click.option("--actor", required=True, help="The operator approval-writer identity.")
@click.option("--payload", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path), default=None)
def approval_record(
    publication_id: str,
    expected_digest: str,
    binding_id: str,
    actor: str,
    payload: Path,
    out: Path | None,
) -> None:
    """Approve the exact stored bytes for one publication.

    The payload is shown in full before the prompt, and the digest you passed is
    recomputed from those bytes. A mismatch aborts — it means what you reviewed
    is not what is stored.
    """
    from .manifest import canonical_digest

    stored = json.loads(payload.read_text())
    actual = canonical_digest(stored)

    click.echo("--- exact bytes to be approved ---")
    click.echo(json.dumps(stored, indent=2, ensure_ascii=False))
    click.echo("--- end ---")
    click.echo(f"computed digest {actual}")

    if actual != expected_digest:
        click.echo(
            f"ABORT: the stored payload digests to {actual}, not the {expected_digest} you reviewed.",
            err=True,
        )
        sys.exit(1)

    click.confirm(f"Approve publication {publication_id} for binding {binding_id}?", abort=True)

    record = ApprovalRecord(
        approval_id=f"appr_{secrets.token_urlsafe(12)}",
        publication_id=publication_id,
        payload_digest=actual,
        binding_id=binding_id,
        approved_by=actor,
        approved_at=datetime.now(timezone.utc),
    )
    rendered = record.model_dump_json(indent=2)
    if out is not None:
        out.write_text(rendered)
        click.echo(f"wrote {out}")
    else:
        click.echo(rendered)


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

    applied = asyncio.run(apply_migrations(dsn, applied_by=actor))
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
