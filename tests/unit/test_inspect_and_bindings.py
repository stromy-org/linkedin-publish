"""`inspect_credentials` and the in-memory binding store.

The inspection combines introspection with identity, and the cases that matter
are the ones where one fact is fine and another is not: an active token from the
wrong app, an active token for the wrong person, an app with no OIDC product.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from linkedin_publish import Credentials
from linkedin_publish.auth import TokenObservation, inspect_credentials, member_sub_from_author
from linkedin_publish.bindings import InMemoryBindingStore, registration_conflicts
from linkedin_publish.models import AccountBinding, CapabilityStatus
from linkedin_publish.store import StoreConflict

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 30, 9, 0, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(days=59)
DECLARED = ("openid", "profile", "w_member_social")
CREDS = Credentials(access_token="tok", client_id="app-personal", client_secret="sec", credential_version="v1")
API = "https://api.linkedin.example"


def handler(*, introspection: dict[str, object] | None = None, userinfo: httpx.Response | None = None):  # noqa: ANN201
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("introspectToken"):
            payload: dict[str, object] = {
                "active": True,
                "status": "active",
                "client_id": "app-personal",
                "scope": "w_member_social,openid,profile",
                "expires_at": int(EXPIRY.timestamp()),
            }
            payload.update(introspection or {})
            return httpx.Response(200, json=payload)
        return userinfo or httpx.Response(200, json={"sub": "AbC123"})

    return respond, calls


async def inspect(respond, **kwargs: object) -> TokenObservation:  # noqa: ANN001
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        return await inspect_credentials(
            http, CREDS, declared_scopes=DECLARED, api_base=API, now=NOW, **kwargs  # type: ignore[arg-type]
        )


async def test_a_matching_token_is_healthy_with_measured_expiry() -> None:
    respond, _ = handler()
    observed = await inspect(respond, expected_app_id="app-personal", expected_member_sub="AbC123")
    assert observed.healthy
    assert observed.expires_at == EXPIRY
    assert observed.member_sub == "AbC123"


async def test_a_token_from_another_app_fails_identity_not_liveness() -> None:
    respond, calls = handler(introspection={"client_id": "app-cma"})
    observed = await inspect(respond, expected_app_id="app-personal", expected_member_sub="AbC123")
    assert observed.token_active is True
    assert observed.identity_verified is False
    assert observed.reason == "introspection_app_mismatch"
    assert not any(path.endswith("userinfo") for path in calls), "no identity call once the app is wrong"


async def test_another_member_is_a_hard_mismatch() -> None:
    respond, _ = handler(userinfo=httpx.Response(200, json={"sub": "SomeoneElse"}))
    observed = await inspect(respond, expected_app_id="app-personal", expected_member_sub="AbC123")
    assert observed.identity_verified is False
    assert not observed.healthy


async def test_a_probe_without_expectation_reports_the_sub_but_does_not_verify() -> None:
    respond, _ = handler()
    observed = await inspect(respond)
    assert observed.member_sub == "AbC123"
    assert observed.identity_verified is None
    assert observed.reason == "userinfo_no_expectation_configured"


async def test_skipping_identity_leaves_it_unproven_with_a_named_reason() -> None:
    respond, calls = handler()
    observed = await inspect(respond, check_identity=False)
    assert observed.identity_verified is None
    assert observed.reason == "identity_not_checked_no_oidc"
    assert not observed.healthy
    assert calls == ["/oauth/v2/introspectToken"]


async def test_an_unreachable_introspection_stops_before_identity() -> None:
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise httpx.ConnectError("down")

    observed = await inspect(respond, expected_member_sub="AbC123")
    assert observed.token_active is None
    assert len(calls) == 1


async def test_an_inactive_token_keeps_its_liveness_reason() -> None:
    respond, _ = handler(introspection={"active": False, "status": "revoked"})
    observed = await inspect(respond, expected_member_sub="AbC123")
    assert observed.known_bad
    assert observed.reason == "introspection_inactive"


def test_member_sub_comes_only_from_a_person_urn() -> None:
    assert member_sub_from_author("urn:li:person:AbC123") == "AbC123"
    assert member_sub_from_author("urn:li:organization:42") is None


# ------------------------------------------------------------------ bindings


def binding(**overrides: object) -> AccountBinding:
    fields: dict[str, object] = {
        "binding_id": "bind-1",
        "account_id": "william-personal",
        "subject_kind": "entra_oid",
        "subject_id": "oid-1",
        "app_id": "app-personal",
        "author_urn": "urn:li:person:AbC123",
        "adapter": "share_ugc",
        "declared_scopes": DECLARED,
        "credential_ref": "linkedin-member-token",
        "credential_version": "v1",
    }
    fields.update(overrides)
    return AccountBinding.model_validate(fields)


async def test_registration_is_created_disabled_even_if_asked_otherwise() -> None:
    store = InMemoryBindingStore()
    evidence = CapabilityStatus(capability="text", state="enabled", observed_at=NOW, evidence_publication_id="pub-x")
    assert await store.register(binding(publish_enabled=True, capabilities=(evidence,))) is True
    stored = await store.get("bind-1")
    assert stored is not None
    assert stored.publish_enabled is False
    assert stored.capabilities == ()


async def test_identical_reregistration_is_an_idempotent_noop() -> None:
    store = InMemoryBindingStore()
    await store.register(binding())
    assert await store.register(binding()) is False


async def test_a_different_member_under_the_same_id_is_a_conflict() -> None:
    store = InMemoryBindingStore()
    await store.register(binding())
    with pytest.raises(StoreConflict, match="author_urn"):
        await store.register(binding(author_urn="urn:li:person:Other"))


def test_conflicts_ignore_fields_that_evolve_after_registration() -> None:
    later = binding(publish_enabled=True, observed_scopes=DECLARED, token_expires_at=EXPIRY)
    assert registration_conflicts(binding(), later) == []


async def test_an_observation_records_measured_scopes_and_expiry() -> None:
    store = InMemoryBindingStore()
    await store.register(binding())
    observation = TokenObservation(
        observed_at=NOW, credential_version="v1", token_active=True, observed_scopes=DECLARED, expires_at=EXPIRY
    )
    updated = await store.record_observation("bind-1", observation)
    assert updated.token_expires_at == EXPIRY
    assert updated.observed_scopes == DECLARED
    assert updated.publish_enabled is False, "an observation never enables anything"


async def test_a_stale_credential_versions_probe_is_refused() -> None:
    store = InMemoryBindingStore()
    await store.register(binding(credential_version="v2"))
    observation = TokenObservation(observed_at=NOW, credential_version="v1", token_active=True)
    with pytest.raises(StoreConflict, match="stale"):
        await store.record_observation("bind-1", observation)
