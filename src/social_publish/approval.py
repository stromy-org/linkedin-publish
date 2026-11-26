# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
#
# This module reads untrusted JSON documents (approval records, trust files) whose
# shape is checked field by field; every value that leaves it is validated through
# a Pydantic model (`MetaPayload`, `SocialRecord`, `SocialApproval`).
"""Trusted import of a C3 operator approval (`social-ops-approval/1`) into the ledger.

A recomputed checksum on an editable file proves consistency, not consent. So
an import verifies, in order, everything the C3 publisher verifies (the
reference implementation is `social-ops/scripts/_approval.py`; the golden
vectors in `assets/fixtures/approval/golden-vectors.json` pin both sides):

1. **Origin.** The record's attestation is an Ed25519 signature (RFC 8032) by a
   key in the operator's trust set; the attested actor is that key's actor; the
   attested digest is the canonical digest of the envelope.
2. **Revocation.** The key is not revoked now; the grant covers the instance and
   account; no later revoke record supersedes the approval.
3. **Digest.** The caption bytes reproduce `caption_sha256` and `caption_bytes`;
   the envelope's target equals the trusted binding's platform and native id.

Then it imports the **original intent id** (the C3 operation key) **with its
dispatch state**, atomically, through `SocialStore.import_intent`. The mapping
never turns work that may already exist at the provider into fresh work:

| C3 journal state                                   | Ledger state |
|----------------------------------------------------|--------------|
| none, `pending`, `held`                            | `pending`    |
| `claimed`                                          | refused: `source_not_quiesced` |
| `sending`, `created`, `unknown`, `cancel_failed`, `withdraw_required` | `unknown` |
| `scheduled`                                        | `scheduled`  |
| `handoff_open`, `handoff_recorded`                 | `handoff`    |
| `published`, `cancelled`, `failed`                 | the same     |

The source must be quiesced for the binding (the C3 publisher paused, so it
cannot dispatch while ownership moves). A failed import writes nothing, so the
C3 journal stays the one accountable sender; a successful one makes the ledger
the owner. Importing the same approval again returns the existing row.

This module never touches the network and never holds a private key.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from .canonical import canonical_digest, canonical_json
from .errors import ApprovalRefused
from .ledger import ImportOutcome, SocialApproval, SocialStore
from .model import MetaAccount, MetaParent, MetaPayload, SocialBinding, SocialKey, SocialRecord
from .states import SocialState

__all__ = [
    "C3_ENVELOPE_VERSION",
    "C3_RECORD_VERSION",
    "C3_TRUST_VERSION",
    "IG_API_ROUTE",
    "ImportSource",
    "TrustedKey",
    "import_c3_approval",
    "key_id_for",
    "load_trust",
    "verify_record",
]

C3_ENVELOPE_VERSION = "social-ops-approval/1"
C3_RECORD_VERSION = "social-ops-approval-record/1"
C3_TRUST_VERSION = "social-ops-trust/1"

#: The route value an Instagram envelope must carry for the ledger to publish it
#: through the API. C3 v1 approves Instagram for `human_handoff` only; such an
#: envelope imports only as an existing handoff, never as API work.
IG_API_ROUTE = "graph_api_publish"
ROUTES = {
    ("facebook_page", "schedule_post"): {"graph_native_schedule"},
    ("instagram_professional", "publish_post"): {IG_API_ROUTE},
    ("facebook_page", "reply"): {"graph_comment_reply"},
    ("instagram_professional", "reply"): {"graph_comment_reply"},
}
#: Facebook native schedules are created at least this long before the slot.
FB_MIN_LEAD = timedelta(minutes=10)

_STATE_MAP: dict[str, SocialState] = {
    "pending": "pending",
    "held": "pending",
    "sending": "unknown",
    "created": "unknown",
    "unknown": "unknown",
    "cancel_failed": "unknown",
    "withdraw_required": "unknown",
    "scheduled": "scheduled",
    "handoff_open": "handoff",
    "handoff_recorded": "handoff",
    "published": "published",
    "cancelled": "cancelled",
    "failed": "failed",
}


def _unb64(text: str, what: str) -> bytes:
    try:
        return base64.b64decode(text.encode("ascii"), validate=True)
    except (binascii.Error, ValueError, AttributeError) as exc:
        raise ApprovalRefused("approval_untrusted", f"{what} is not base64") from exc


def _parse(value: object, what: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApprovalRefused("approval_untrusted", f"{what} is not ISO 8601") from exc
    if parsed.tzinfo is None:
        raise ApprovalRefused("approval_untrusted", f"{what} has no offset")
    return parsed.astimezone(UTC)


def key_id_for(public_key: bytes) -> str:
    return "ed25519:" + hashlib.sha256(public_key).hexdigest()[:16]


@dataclass
class TrustedKey:
    key_id: str
    public_key: bytes
    actor: str
    role: str  # "operator" (any instance) or "approver" (its grants only)
    grants: list[dict[str, Any]] = field(default_factory=list)
    revoked_at: str | None = None


def load_trust(doc: object) -> dict[str, TrustedKey]:
    """Parse a `social-ops-trust/1` document. Malformed trust is an error, never empty trust."""
    if not isinstance(doc, dict) or doc.get("schema_version") != C3_TRUST_VERSION:
        raise ApprovalRefused("trust_invalid", f"trust document must have schema_version {C3_TRUST_VERSION}")
    keys: dict[str, TrustedKey] = {}
    for entry in doc.get("keys") or []:
        try:
            public = _unb64(entry["public_key_b64"], "public_key_b64")
            key = TrustedKey(
                key_id=entry["key_id"],
                public_key=public,
                actor=entry["actor"],
                role=entry["role"],
                grants=list(entry.get("grants") or []),
                revoked_at=entry.get("revoked_at"),
            )
        except (KeyError, TypeError) as exc:
            raise ApprovalRefused("trust_invalid", f"trust key entry is incomplete: {exc}") from exc
        if len(public) != 32 or key.key_id != key_id_for(public):
            raise ApprovalRefused("trust_invalid", f"trust key {key.key_id!r} does not match its public key")
        if key.role not in ("operator", "approver"):
            raise ApprovalRefused("trust_invalid", f"trust key {key.key_id}: unknown role {key.role!r}")
        keys[key.key_id] = key
    if not keys:
        raise ApprovalRefused("trust_invalid", "trust document lists no keys")
    return keys


def _ed25519_verify(public_key: bytes, signature: bytes, message: bytes) -> bool:
    try:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by the extra
        raise ApprovalRefused(
            "verifier_unavailable", "install the `meta` extra (cryptography) to verify approvals"
        ) from exc
    try:
        Ed25519PublicKey.from_public_bytes(public_key).verify(signature, message)
    except (InvalidSignature, ValueError):
        return False
    return True


def _granted(key: TrustedKey, instance: dict[str, Any], account_id: str) -> bool:
    if key.role == "operator":
        return True
    names = {a.get("name") for a in instance.get("approvers") or []}
    if key.actor not in names:
        return False
    for grant in key.grants:
        if grant.get("instance_id") != instance.get("instance_id"):
            continue
        accounts = grant.get("accounts")
        if accounts is None or account_id in accounts:
            return True
    return False


def verify_record(
    record: object, trust: dict[str, TrustedKey], instance: dict[str, Any], now: datetime
) -> dict[str, Any]:
    """Authenticate one approval record; return its attestation (C3's checks, in C3's order)."""
    if not isinstance(record, dict) or record.get("record_version") != C3_RECORD_VERSION:
        raise ApprovalRefused("approval_untrusted", "not an approval record of this version")
    envelope: object = record.get("envelope")
    attestation: object = record.get("attestation")
    if not isinstance(envelope, dict) or not isinstance(attestation, dict):
        raise ApprovalRefused("approval_untrusted", "record lacks an envelope or an attestation")
    if envelope.get("envelope_version") != C3_ENVELOPE_VERSION:
        raise ApprovalRefused("approval_untrusted", "unknown envelope version")
    digest = canonical_digest(envelope)
    if attestation.get("envelope_sha256") != digest:
        raise ApprovalRefused("approval_untrusted", "the attested digest does not match the envelope")
    key = trust.get(str(attestation.get("key_id")))
    if key is None:
        raise ApprovalRefused("approval_untrusted", "the signing key is not trusted")
    signature = _unb64(str(record.get("signature_b64")), "signature_b64")
    if not _ed25519_verify(key.public_key, signature, canonical_json(attestation)):
        raise ApprovalRefused("approval_untrusted", "bad signature")
    if attestation.get("actor") != key.actor:
        raise ApprovalRefused("approval_forged_author", f"attested actor is not the key's actor ({key.key_id})")
    if key.revoked_at is not None and _parse(key.revoked_at, "revoked_at") <= now:
        raise ApprovalRefused("grant_revoked", f"key {key.key_id} of {key.actor} revoked at {key.revoked_at}")
    if envelope.get("instance_id") != instance.get("instance_id"):
        raise ApprovalRefused("approval_untrusted", "record belongs to another instance")
    target = envelope.get("target")
    account_id = target.get("account_id") if isinstance(target, dict) else None
    if not _granted(key, instance, str(account_id)):
        raise ApprovalRefused("grant_missing", f"{key.actor} holds no grant for this instance and account")
    if attestation.get("kind") not in ("approve", "revoke"):
        raise ApprovalRefused("approval_untrusted", "unknown attestation kind")
    sequence = attestation.get("sequence")
    if not isinstance(sequence, int) or sequence < 1:
        raise ApprovalRefused("approval_untrusted", "attestation sequence must be a positive integer")
    return attestation


@dataclass(frozen=True)
class ImportSource:
    """The C3 journal's view of the operation, read under the paused publisher.

    `quiesced` is the operator's statement, from the C3 journal, that the local
    publisher for this binding is paused (`withdraw`/`pause`), so it cannot send
    while ownership moves. `remote_id` and `container_id` carry what the C3
    journal recorded for an operation that already reached Meta.
    """

    quiesced: bool
    state: str | None = None
    remote_id: str | None = None
    container_id: str | None = None


def _payload_from(envelope: dict[str, Any], caption: str, binding: SocialBinding, source_state: str) -> MetaPayload:
    platform = envelope.get("platform")
    operation = envelope.get("operation")
    if platform != binding.platform:
        raise ApprovalRefused("binding_mismatch", f"the approval is for {platform}, the binding is {binding.platform}")
    target = envelope.get("target") or {}
    if str(target.get("account_platform_id")) != binding.native_account_id:
        raise ApprovalRefused("binding_mismatch", "the approval targets another account than the binding")
    route = envelope.get("route")
    handoff = platform == "instagram_professional" and route == "human_handoff"
    if handoff:
        if _STATE_MAP.get(source_state) not in ("handoff", "unknown", "published", "cancelled", "failed"):
            raise ApprovalRefused(
                "route_handoff_only",
                "this Instagram approval is for the human Business Suite handoff; the ledger records an existing "
                "handoff but never publishes it through the API",
            )
    elif route not in ROUTES.get((str(platform), str(operation)), set()):
        raise ApprovalRefused("route_mismatch", f"route {route!r} is not dispatchable for {platform} {operation}")
    public = envelope.get("public_fields") or {}
    if public.get("interaction", "platform_default") != "platform_default":
        raise ApprovalRefused("interaction_control_unproven", "no pre-publication interaction control is proven")
    if public.get("visibility", "public") != "public":
        raise ApprovalRefused("visibility_unsupported", "only public visibility is dispatched")
    encoded = caption.encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != envelope.get("caption_sha256") or len(encoded) != envelope.get(
        "caption_bytes"
    ):
        raise ApprovalRefused("approval_mismatch", "the caption bytes do not reproduce the approved caption digest")
    parent = None
    if operation == "reply":
        parent = MetaParent(
            post_id=str(target.get("parent_post_id")),
            comment_id=str(target.get("comment_id")),
            comment_version=str(target.get("comment_version")),
        )
    slot = envelope.get("slot_utc")
    try:
        return MetaPayload(
            platform=platform,  # type: ignore[arg-type]
            action=operation,  # type: ignore[arg-type]
            format=str(envelope.get("format")),
            account=MetaAccount(account_id=str(target.get("account_id")), native_account_id=binding.native_account_id),  # type: ignore[arg-type]
            parent=parent,
            caption=caption,
            caption_sha256=str(envelope.get("caption_sha256")),
            media=tuple(envelope.get("media") or ()),
            alt_text=tuple(public.get("alt_text") or ()),
            links=tuple(public.get("links") or ()),
            ai_label=bool(public.get("ai_label")),
            slot_utc=_parse(slot, "slot_utc") if slot else None,
            envelope_sha256=canonical_digest(envelope),
        )
    except ValueError as exc:
        raise ApprovalRefused("approval_mismatch", f"the envelope is not a dispatchable Meta payload: {exc}") from exc


async def import_c3_approval(
    store: SocialStore,
    binding: SocialBinding,
    *,
    record: dict[str, Any],
    caption: str,
    trust: dict[str, TrustedKey],
    instance: dict[str, Any],
    source: ImportSource,
    later_records: list[dict[str, Any]] | None = None,
    actor: str,
    now: datetime | None = None,
) -> ImportOutcome:
    """Verify one C3 approval record and import it with its dispatch state, atomically.

    `later_records` are the other records of the same operation from the C3
    approvals folder: a verified revocation or a newer approval among them
    refuses this one (`approval_revoked`, `superseded`). Runs as the writer role
    (it inserts into `approvals`); the runtime role cannot.
    """
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    if binding.platform == "linkedin":
        raise ApprovalRefused("binding_mismatch", "C3 approvals are Meta approvals; this is a LinkedIn binding")
    if not source.quiesced:
        raise ApprovalRefused(
            "source_not_quiesced", "pause the C3 publisher for this binding before the ledger takes ownership"
        )
    if source.state == "claimed":
        raise ApprovalRefused("source_not_quiesced", "the C3 journal holds a live claim on this operation")
    if source.state is not None and source.state not in _STATE_MAP:
        raise ApprovalRefused("source_state_unknown", f"C3 journal state {source.state!r} has no ledger mapping")

    attestation = verify_record(record, trust, instance, moment)
    if attestation["kind"] != "approve":
        raise ApprovalRefused("approval_revoked", "a revocation record is not an approval")
    envelope: dict[str, Any] = record["envelope"]
    operation_key = "/".join(
        (
            str(envelope["instance_id"]),
            str(envelope["target"]["account_id"]),
            str(envelope["post_id"]),
            str(envelope["operation"]),
        )
    )
    if attestation.get("operation_key") != operation_key:
        raise ApprovalRefused("approval_untrusted", "the attested operation key does not match the envelope")
    for other in later_records or []:
        other_att = verify_record(other, trust, instance, moment)
        if other_att.get("operation_key") != operation_key or other_att["sequence"] <= attestation["sequence"]:
            continue
        if other_att["kind"] == "revoke":
            raise ApprovalRefused("approval_revoked", f"revoked by sequence {other_att['sequence']}")
        raise ApprovalRefused("superseded", f"superseded by sequence {other_att['sequence']}")

    state: SocialState = _STATE_MAP[source.state or "pending"]
    payload = _payload_from(envelope, caption, binding, source.state or "pending")
    approved_at = _parse(attestation.get("approved_at"), "approved_at")
    expires = _parse(envelope.get("expires_utc"), "expires_utc")
    slot = payload.slot_utc
    if payload.action == "publish_post":
        assert slot is not None  # noqa: S101 - MetaPayload enforces it
        scheduled_at, expires_at = slot, expires
    elif payload.action == "schedule_post" and state == "pending":
        assert slot is not None  # noqa: S101 - MetaPayload enforces it
        scheduled_at, expires_at = approved_at, min(expires, slot - FB_MIN_LEAD)
    else:
        scheduled_at, expires_at = approved_at, expires
    if expires_at <= scheduled_at:
        if state == "pending":
            raise ApprovalRefused("approval_expired", "the approval's dispatch window is already closed")
        expires_at = scheduled_at + timedelta(seconds=1)
    if state == "pending" and moment >= expires_at:
        raise ApprovalRefused("approval_expired", "the approval expired before it could be imported")
    if state in ("scheduled", "published") and not source.remote_id:
        raise ApprovalRefused("source_incomplete", f"a {state} operation needs the remote id the C3 journal recorded")

    payload_json = payload.model_dump(mode="json")
    digest = canonical_digest(payload_json)
    sequence = int(attestation["sequence"])
    publication_id = f"pub_{digest[:24]}"
    social = SocialRecord(
        publication_id=publication_id,
        key=SocialKey(
            subject_kind=binding.subject_kind,
            subject_id=binding.subject_id,
            campaign_id=str(envelope["instance_id"]),
            post_id=f"{envelope['post_id']}.{envelope['operation']}.{sequence}",
            account_id=binding.account_id,
        ),
        binding_id=binding.binding_id,
        platform=binding.platform,
        payload=payload_json,
        payload_digest=digest,
        operation=payload.action,
        format=payload.format,
        state=state,
        scheduled_at=scheduled_at,
        expires_at=expires_at,
        remote_id=source.remote_id,
        container_id=source.container_id,
        adapter=binding.adapter if state != "pending" else None,
        failure_code=f"imported:{source.state}" if state == "unknown" and source.state else None,
        intent_id=operation_key,
        origin={
            "kind": "c3-approval",
            "envelope_sha256": canonical_digest(envelope),
            "key_id": attestation.get("key_id"),
            "actor": attestation.get("actor"),
            "sequence": sequence,
            "source_state": source.state,
            "route": envelope.get("route"),
        },
    )
    approval = SocialApproval(
        approval_id=f"c3_{canonical_digest(envelope)[:32]}",
        publication_id=publication_id,
        payload_digest=digest,
        binding_id=binding.binding_id,
        approved_by=f"{attestation.get('actor')} ({attestation.get('key_id')})",
        approved_at=approved_at,
    )
    return await store.import_intent(social, approval, actor=actor, at=moment)
