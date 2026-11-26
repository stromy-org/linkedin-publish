"""Token health: the verdict, the <=14-day expiry warning, the refresh rules, the cache.

The warning tests pin the one property that matters most: the date comes from a
MEASUREMENT. A binding registered today with no measured expiry gets no
"expires in 60 days" invented for it, and a binding measured to expire in 13
days warns however recently the process restarted.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from linkedin_publish.auth import TokenObservation
from linkedin_publish.bindings import InMemoryBindingStore, reenable_problems, rotated
from linkedin_publish.health import (
    DAILY_REFRESH,
    InMemoryObservationCache,
    assess,
    expiry_warning,
    refresh_reason,
    sanitize,
)
from linkedin_publish.postgres import _as_binding
from linkedin_publish.store import StoreConflict
from tests.conftest import NOW, binding

pytestmark = pytest.mark.unit


def observed(**overrides: object) -> TokenObservation:
    values: dict[str, object] = {
        "observed_at": NOW,
        "credential_version": "v1",
        "token_active": True,
        "scopes_ok": True,
        "identity_verified": True,
        "observed_scopes": ("openid", "profile", "w_member_social"),
        "expires_at": NOW + timedelta(days=40),
        "app_id": "app-personal",
        "member_sub": "AbC123xyz",
    }
    values.update(overrides)
    return TokenObservation(**values)  # type: ignore[arg-type]


def bound(days_left: float | None, **overrides: object):  # noqa: ANN201
    base = binding()
    expiry = None if days_left is None else NOW + timedelta(days=days_left)
    return base.model_copy(update={"token_expires_at": expiry, **overrides})


# ------------------------------------------------------------- expiry warning


def test_warns_inside_fourteen_days_of_the_measured_expiry() -> None:
    warning = expiry_warning(bound(13.5), now=NOW)
    assert warning is not None
    assert warning.code == "token_expires_soon"
    assert warning.days_remaining == 13
    assert warning.window_days == 14
    assert warning.source == "binding"


def test_no_warning_outside_the_window() -> None:
    assert expiry_warning(bound(14.5), now=NOW) is None


def test_the_boundary_day_warns() -> None:
    assert expiry_warning(bound(14), now=NOW) is not None


def test_no_measured_expiry_invents_none() -> None:
    """Never `now + 60 days`: an unmeasured expiry is not a date, and not a warning."""
    assert expiry_warning(bound(None), now=NOW) is None
    health = assess(bound(None), None, now=NOW)
    assert health.status == "unknown"
    assert health.reason == "not_measured"
    assert health.expires_at is None


def test_a_newer_observation_of_the_current_version_wins_over_the_binding() -> None:
    warning = expiry_warning(bound(40), observed(expires_at=NOW + timedelta(days=3)), now=NOW)
    assert warning is not None
    assert warning.source == "cache"
    assert warning.days_remaining == 3


def test_an_observation_of_another_version_is_ignored_for_expiry() -> None:
    assert expiry_warning(bound(40), observed(credential_version="v0", expires_at=NOW), now=NOW) is None


def test_a_passed_expiry_is_expired_and_blocks() -> None:
    health = assess(bound(-0.5), observed(expires_at=NOW - timedelta(hours=12)), now=NOW)
    assert health.status == "blocked"
    assert health.reason == "token_expired"
    assert health.blocks_writes
    assert health.expiry_warning is not None
    assert health.expiry_warning.code == "token_expired"
    assert health.expiry_warning.days_remaining == -1


# ------------------------------------------------------------------ verdict


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"token_active": False, "reason": "introspection_inactive"}, "token_inactive"),
        ({"identity_verified": False, "reason": "introspection_app_mismatch"}, "wrong_app"),
        ({"identity_verified": False, "reason": "userinfo_member_mismatch"}, "wrong_identity"),
        ({"scopes_ok": False}, "scopes_missing"),
        ({"credential_version": "v0"}, "credential_version_mismatch"),
    ],
)
def test_known_bad_credentials_block_writes(overrides: dict[str, object], reason: str) -> None:
    health = assess(bound(40), observed(**overrides), now=NOW)
    assert health.status == "blocked"
    assert health.reason == reason
    assert health.blocks_writes


def test_a_failed_probe_is_unknown_never_missing_or_bad() -> None:
    failed = observed(
        token_active=None, scopes_ok=None, identity_verified=None, expires_at=None,
        reason="introspection_unreachable: ConnectError",
    )  # fmt: skip
    health = assess(bound(40), failed, now=NOW)
    assert health.status == "unknown"
    assert health.reason is not None and health.reason.startswith("probe_failed")
    assert "missing" not in health.reason
    assert not health.blocks_writes


def test_a_healthy_observation_is_healthy() -> None:
    health = assess(bound(40), observed(), now=NOW)
    assert health.status == "healthy"
    assert health.expiry_warning is None
    assert health.as_dict()["source"] == "cache"


# ---------------------------------------------------------------- refreshing


def test_refresh_is_owed_when_missing_daily_and_before_a_stale_due_batch() -> None:
    assert refresh_reason(None, now=NOW, due_work=False) == "missing"
    assert refresh_reason(observed(observed_at=NOW - DAILY_REFRESH), now=NOW, due_work=False) == "daily"
    assert refresh_reason(observed(observed_at=NOW - timedelta(hours=2)), now=NOW, due_work=False) is None
    assert refresh_reason(observed(observed_at=NOW - timedelta(minutes=20)), now=NOW, due_work=True) == (
        "due_batch_stale"
    )
    near = observed(observed_at=NOW - timedelta(minutes=1), expires_at=NOW + timedelta(hours=20))
    assert refresh_reason(near, now=NOW, due_work=True) == "near_expiry"
    assert refresh_reason(observed(observed_at=NOW - timedelta(minutes=1)), now=NOW, due_work=True) is None


# --------------------------------------------------------------------- cache


async def test_the_cache_is_keyed_by_credential_version_and_monotonic() -> None:
    cache = InMemoryObservationCache()
    await cache.put("bind-1", observed())
    assert await cache.get("bind-1", "v1") is not None
    assert await cache.get("bind-1", "v2") is None  # a rotated secret never reads the old probe

    await cache.put("bind-1", observed(observed_at=NOW - timedelta(hours=1), token_active=False))
    kept = await cache.get("bind-1", "v1")
    assert kept is not None and kept.token_active is True  # an older probe never replaces a newer one

    assert await cache.invalidate("bind-1") == 1
    assert await cache.get("bind-1", "v1") is None


def test_sanitize_caps_the_reason() -> None:
    assert len(sanitize(observed(reason="x" * 5000)).reason or "") == 200


# ------------------------------------------------------------------ rotation


def test_rotation_disables_clears_the_measurement_and_remembers_enablement() -> None:
    moved = rotated(bound(5), expected_version="v1", credential_ref="kv://new", credential_version="v2", at=NOW)
    assert moved.credential_version == "v2"
    assert moved.credential_ref == "kv://new"
    assert moved.publish_enabled is False
    assert moved.token_expires_at is None and moved.token_observed_at is None
    assert moved.observed_scopes == ()
    assert moved.rotated_from_version == "v1"
    assert moved.reenable_after_measure is True
    # Identity that approvals hang off does not move.
    assert (moved.app_id, moved.author_urn, moved.account_id) == ("app-personal", bound(5).author_urn, "acct-william")


def test_rotating_a_disabled_binding_does_not_arm_a_reenable() -> None:
    moved = rotated(
        bound(5, publish_enabled=False), expected_version="v1", credential_ref="r", credential_version="v2", at=NOW
    )
    assert moved.reenable_after_measure is False


@pytest.mark.parametrize(("expected", "new"), [("v0", "v2"), ("v1", "v1")])
def test_rotation_refuses_a_stale_view_or_the_same_version(expected: str, new: str) -> None:
    with pytest.raises(StoreConflict):
        rotated(bound(5), expected_version=expected, credential_ref="r", credential_version=new, at=NOW)


def test_reenable_needs_a_healthy_measurement_taken_after_the_rotation() -> None:
    moved = rotated(bound(5), expected_version="v1", credential_ref="r", credential_version="v2", at=NOW)
    assert reenable_problems(moved, None, now=NOW)  # nothing measured yet

    later = NOW + timedelta(minutes=5)
    measured = moved.model_copy(
        update={
            "token_expires_at": later + timedelta(days=59),
            "token_observed_at": later,
            "observed_scopes": ("openid", "profile", "w_member_social"),
        }
    )
    fresh = observed(credential_version="v2", observed_at=later, expires_at=later + timedelta(days=59))
    assert reenable_problems(measured, fresh, now=later) == []
    assert reenable_problems(measured, observed(credential_version="v2", observed_at=later, token_active=False),
                             now=later)  # fmt: skip
    assert reenable_problems(measured, observed(credential_version="v1", observed_at=later), now=later)


def test_reenable_never_enables_a_binding_that_was_never_enabled() -> None:
    never = bound(30, publish_enabled=False)
    problems = reenable_problems(never, observed(), now=NOW)
    assert any("canary" in problem for problem in problems)


async def test_in_memory_rotate_then_reenable_round_trip() -> None:
    store = InMemoryBindingStore()
    await store.register(bound(5))
    # Registration stores nothing enabled; simulate a commissioned binding.
    store._rows["bind-1"] = bound(5)  # noqa: SLF001 - fixture shortcut for an enabled binding
    moved = await store.rotate("bind-1", expected_version="v1", credential_ref="r", credential_version="v2", at=NOW)
    assert moved.publish_enabled is False
    with pytest.raises(StoreConflict):
        await store.reenable("bind-1", expected_version="v1")
    back = await store.reenable("bind-1", expected_version="v2")
    assert back.publish_enabled is True and back.reenable_after_measure is False


# ----------------------------------------------------- schema compatibility


def test_a_binding_row_from_a_ledger_before_0005_still_loads() -> None:
    """Runtime keeps working against an unmigrated ledger: the new columns default."""
    row = {
        "binding_id": "bind-1", "account_id": "a", "subject_kind": "entra_oid", "subject_id": "s",
        "app_id": "app", "author_urn": "urn:li:person:AbC", "allowed_org_urns": [], "adapter": "share_ugc",
        "declared_scopes": [], "observed_scopes": [], "credential_ref": "r", "credential_version": "v1",
        "token_expires_at": None, "token_observed_at": None, "capabilities": "[]", "publish_enabled": False,
    }  # fmt: skip
    loaded = _as_binding(row)
    assert loaded.rotated_at is None
    assert loaded.reenable_after_measure is False
