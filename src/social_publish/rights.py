"""Rights evidence kept from a C3 approval, and the re-check a media issuer runs before signing.

A C3 envelope carries `rights_evidence`: for each media handle, the platforms it
may be published on and an optional expiry (`null` = no expiry). The importer
keeps that list on the ledger row, under `origin["rights_evidence"]`.

**Why `origin`.** `origin` is provenance beside the row, not part of the payload:
the payload digest, the approval id and the envelope digest are all computed
before and without it, so keeping the evidence there changes no existing digest
(`tests/contract/test_social_rights.py` pins them against the C3 golden
vectors). It needs no migration: `origin` is free-form jsonb that every reader
passes through, and a row imported before this change simply has no list.

**Why re-check.** The approval proves the rights held when it was signed. A
signed media URL is minted later, possibly after an asset's licence ran out, so
the issuer calls `rights_problems` (or `require_rights`) right before it signs.
A record with media and no retained evidence is refused, never assumed covered:
absent evidence is `unknown`, and `unknown` is never permission.

Pure; nothing here touches the network, a clock other than the one passed in, or
a credential.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, cast

from .errors import ApprovalRefused
from .model import SocialRecord

__all__ = ["normalize_rights_evidence", "require_rights", "rights_evidence_of", "rights_problems"]


def normalize_rights_evidence(raw: object) -> list[dict[str, Any]] | None:
    """The envelope's `rights_evidence` as stored on the ledger, or None when it has none.

    Keeps `handle`, `expires` (an ISO string or None), `platforms` and `source`
    exactly as signed. Raises `ApprovalRefused("rights_evidence_invalid")` for a
    malformed list, so a bad entry is never stored as if it were evidence.
    """
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ApprovalRefused("rights_evidence_invalid", "rights_evidence is not a list")
    kept: list[dict[str, Any]] = []
    for item in cast("list[object]", raw):
        if not isinstance(item, dict):
            raise ApprovalRefused("rights_evidence_invalid", "a rights_evidence entry is not an object")
        entry = cast("dict[str, Any]", item)
        handle = entry.get("handle")
        if not isinstance(handle, str):
            raise ApprovalRefused("rights_evidence_invalid", "a rights_evidence entry has no asset handle")
        platforms: object = entry.get("platforms")
        if not isinstance(platforms, list) or not all(isinstance(p, str) for p in cast("list[object]", platforms)):
            raise ApprovalRefused("rights_evidence_invalid", f"{handle}: platforms is not a list of names")
        expires: object = entry.get("expires")
        if expires is not None and not isinstance(expires, str):
            raise ApprovalRefused("rights_evidence_invalid", f"{handle}: expires is not an ISO string")
        kept.append(
            {
                "handle": handle,
                "expires": expires,
                "platforms": [str(p) for p in cast("list[object]", platforms)],
                "source": entry.get("source"),
            }
        )
    return kept


def rights_evidence_of(record: SocialRecord) -> list[dict[str, Any]] | None:
    """The rights evidence the importer kept on `record`, or None (not imported, or imported before 0.10.0)."""
    if record.origin is None:
        return None
    kept: object = record.origin.get("rights_evidence")
    if not isinstance(kept, list):
        return None
    return cast("list[dict[str, Any]]", kept)


def _expiry(value: object) -> datetime | None | str:
    """The entry's expiry as an aware datetime, None for no expiry, or a problem string."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return f"expiry {value!r} is not ISO 8601"
    if parsed.tzinfo is None:
        return f"expiry {value!r} has no offset"
    return parsed.astimezone(UTC)


def rights_problems(
    record: SocialRecord,
    *,
    now: datetime,
    platform: str | None = None,
    handles: Sequence[str] | None = None,
) -> list[str]:
    """Why the media of `record` may not be signed for `platform` at `now`. Empty means it may.

    Checks every handle in `handles` (default: the payload's media). Each needs a
    retained entry that covers `platform` (default: the record's own) and has not
    expired: an entry whose expiry is at or before `now` is expired.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    media: object = record.payload.get("media") or ()
    declared = [str(h) for h in cast("Sequence[object]", media)]
    wanted = tuple(handles) if handles is not None else tuple(declared)
    if not wanted:
        return []
    target = platform or record.platform
    evidence = rights_evidence_of(record)
    if evidence is None:
        return [
            f"{record.publication_id} retains no rights evidence (imported before it was kept, or not from a C3 "
            "approval); re-import the approval before signing media"
        ]
    problems: list[str] = []
    for handle in wanted:
        entries = [e for e in evidence if e.get("handle") == handle]
        if not entries:
            problems.append(f"{handle}: the approval carries no rights evidence for this asset")
            continue
        covering = [e for e in entries if target in (e.get("platforms") or [])]
        if not covering:
            problems.append(f"{handle}: the rights do not cover {target}")
            continue
        live = False
        for entry in covering:
            expiry = _expiry(entry.get("expires"))
            if isinstance(expiry, str):
                problems.append(f"{handle}: {expiry}")
            elif expiry is None or expiry > now:
                live = True
            else:
                problems.append(f"{handle}: the rights expired at {expiry.isoformat()}")
        if live:
            problems = [p for p in problems if not p.startswith(f"{handle}:")]
    return problems


def require_rights(
    record: SocialRecord,
    *,
    now: datetime,
    platform: str | None = None,
    handles: Sequence[str] | None = None,
) -> None:
    """Raise `ApprovalRefused` (`rights_not_covered` / `rights_expired`) unless `rights_problems` is empty."""
    problems = rights_problems(record, now=now, platform=platform, handles=handles)
    if problems:
        code = "rights_expired" if any("expired" in p for p in problems) else "rights_not_covered"
        raise ApprovalRefused(code, "; ".join(problems))
