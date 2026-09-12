"""Narrowing helpers for untyped JSON.

`response.json()` and `json.loads` both return `Any`, which quietly disables
type checking for everything downstream of them. These helpers convert an
unknown payload into `dict[str, object]` once, at the boundary, so every field
read after that is checked.

A non-object payload becomes an empty mapping rather than an exception: a
provider that returns a bare array or a string is a *missing field* problem, and
the caller already has to handle missing fields.
"""

from __future__ import annotations

import json
from typing import cast

__all__ = ["as_object", "loads_object"]


def as_object(raw: object) -> dict[str, object]:
    """Narrow an arbitrary decoded value to a string-keyed mapping."""
    if not isinstance(raw, dict):
        return {}
    items = cast("dict[object, object]", raw).items()
    return {str(key): value for key, value in items}


def loads_object(raw: bytes | str) -> dict[str, object]:
    """Parse JSON and narrow it, raising `ValueError` only on malformed input."""
    return as_object(cast(object, json.loads(raw)))
