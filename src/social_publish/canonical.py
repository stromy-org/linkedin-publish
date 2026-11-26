"""The one canonical encoding every digest in the publication stack uses.

Sorted keys, no insignificant whitespace, UTF-8, non-ASCII preserved. It is the
encoding the LinkedIn manifest digest has always used (`linkedin_publish.manifest`
re-exports these exact functions) and the one the operator approval envelope
(`social-ops-approval/1`) is signed over. Changing a byte here changes every
stored digest, so the golden vectors pin it from both sides.

A leaf module: it imports nothing from this package or from `linkedin_publish`,
so both namespaces can depend on it without an import cycle.
"""

from __future__ import annotations

import hashlib
import json

__all__ = ["canonical_digest", "canonical_json"]


def canonical_json(payload: object) -> bytes:
    """One canonical serialization, used for every digest in this system.

    Sorted keys, no insignificant whitespace, UTF-8, non-ASCII preserved.
    Preserving non-ASCII matters: escaping it would make two byte-identical
    posts with an emoji hash differently depending on the writer.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def canonical_digest(payload: object) -> str:
    """SHA-256 over `canonical_json(payload)`, lowercase hex."""
    return hashlib.sha256(canonical_json(payload)).hexdigest()
