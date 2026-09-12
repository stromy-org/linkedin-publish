"""Command-line interface.

The CLI is the *trusted operator path*. Two things follow from that and are not
negotiable:

* `approval record` previews the exact stored bytes, requires the expected
  digest as an argument, and requires an interactive confirmation. There is no
  `--yes`. Production automation consumes approvals; it never creates them.
* `account commission` issues a grant scoped to one stored publication, one
  digest, one binding and one capability, expiring shortly. A scheduler identity
  cannot reach this command's writer role.

`manifest validate` is fully offline: no credential is read, no network touched,
no database opened. That is the command to reach for when checking a manifest.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click

from . import __version__
from .manifest import PublishManifest
from .store import ApprovalRecord, CommissioningGrant

#: How long a commissioning grant stays usable. Short on purpose — it exists to
#: cover one deliberate canary, not to sit waiting for a convenient moment.
GRANT_TTL = timedelta(minutes=30)


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


@account.command("inspect")
@click.option("--binding", "binding_id", required=True)
@click.option("--dsn", envvar="LINKEDIN_PUBLISH_DSN", required=True)
def account_inspect(binding_id: str, dsn: str) -> None:
    """Measure a binding's token: introspection, scopes, and OIDC identity.

    Reports the three facts separately. A probe that cannot complete is
    `unknown`, never "no token".
    """
    click.echo(
        "account inspect requires a provisioned binding and credential provider.\n"
        "It is wired by the hosted layer (C3) and the commissioning walkthrough (C1);\n"
        f"binding={binding_id!r} was not contacted and no credential was read.",
        err=True,
    )
    sys.exit(2)


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
