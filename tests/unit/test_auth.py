"""Token health: three facts, measured and reported independently."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from linkedin_publish import Credentials
from linkedin_publish.auth import HealthCache, introspect_token, verify_identity

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(days=46)

CREDS = Credentials(
    access_token="test-token", client_id="app-personal", client_secret="test-secret", credential_version="v1"
)
DECLARED = ("w_member_social", "openid", "profile")


def client(handler) -> httpx.AsyncClient:  # noqa: ANN001
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def introspection(**overrides: object) -> httpx.Response:
    payload: dict[str, object] = {
        "active": True,
        "status": "active",
        "client_id": "app-personal",
        "scope": "w_member_social,openid,profile",
        "expires_at": int(EXPIRY.timestamp()),
    }
    payload.update(overrides)
    return httpx.Response(200, json=payload)


async def test_an_active_token_reports_scopes_and_real_expiry() -> None:
    async with client(lambda _r: introspection()) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.token_active is True
    assert observed.scopes_ok is True
    assert observed.expires_at == EXPIRY
    assert observed.app_id == "app-personal"


async def test_expiry_comes_from_the_response_not_from_now_plus_sixty_days() -> None:
    """A restart must not re-mint a 60-day expiry it never observed."""
    async with client(lambda _r: introspection()) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.expires_at is not None
    assert observed.expires_at != NOW + timedelta(days=60)
    assert observed.expires_within(timedelta(days=47), now=NOW) is True
    assert observed.expires_within(timedelta(days=45), now=NOW) is False


async def test_an_inactive_token_is_known_bad() -> None:
    async with client(lambda _r: introspection(active=False, status="revoked")) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.token_active is False
    assert observed.known_bad is True
    assert observed.healthy is False
    assert observed.reason == "introspection_inactive"


async def test_a_missing_scope_is_reported_separately_from_liveness() -> None:
    async with client(lambda _r: introspection(scope="openid profile")) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.token_active is True
    assert observed.scopes_ok is False
    assert observed.healthy is False


async def test_a_token_from_another_app_is_visible_in_the_observation() -> None:
    async with client(lambda _r: introspection(client_id="app-cma")) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.app_id == "app-cma"
    assert observed.app_id != CREDS.client_id


async def test_an_unreachable_probe_is_unknown_not_missing() -> None:
    """An outage and a revocation need different fixes, so they never merge."""

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("network down", request=request)

    async with client(boom) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.token_active is None
    assert observed.known_bad is False
    assert observed.healthy is False
    assert observed.reason is not None
    assert "unreachable" in observed.reason


async def test_a_non_200_introspection_is_unknown() -> None:
    async with client(lambda _r: httpx.Response(503)) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert observed.token_active is None
    assert observed.reason == "introspection_http_503"


async def test_the_token_travels_in_the_body_never_the_url() -> None:
    """argv, shell history and proxy logs never see it."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return introspection()

    async with client(handler) as http:
        await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
    assert CREDS.access_token not in str(seen[0].url)
    assert CREDS.access_token.encode() in seen[0].read()


async def test_identity_is_verified_against_the_expected_member() -> None:
    async with client(lambda _r: httpx.Response(200, json={"sub": "AbC123xyz"})) as http:
        verified, sub, reason = await verify_identity(
            http, CREDS, expected_member_sub="AbC123xyz", api_base="https://api.linkedin.test"
        )
    assert (verified, sub, reason) == (True, "AbC123xyz", None)


async def test_a_member_mismatch_is_a_hard_false() -> None:
    async with client(lambda _r: httpx.Response(200, json={"sub": "SomeoneElse"})) as http:
        verified, sub, reason = await verify_identity(
            http, CREDS, expected_member_sub="AbC123xyz", api_base="https://api.linkedin.test"
        )
    assert verified is False
    assert reason == "userinfo_member_mismatch"
    assert sub == "SomeoneElse"


async def test_a_missing_oidc_product_is_unknown_not_a_mismatch() -> None:
    """A 403 means the app lacks Sign In with LinkedIn — a different fix entirely."""
    async with client(lambda _r: httpx.Response(403, json={})) as http:
        verified, _sub, reason = await verify_identity(
            http, CREDS, expected_member_sub="AbC123xyz", api_base="https://api.linkedin.test"
        )
    assert verified is None
    assert reason == "userinfo_forbidden_403"


async def test_an_http_200_handshake_never_substitutes_for_the_probe() -> None:
    """The whole point: a reachable endpoint is not a live, scoped, bound token."""
    async with client(lambda _r: introspection(active=False, status="expired")) as http:
        observed = await introspect_token(http, CREDS, declared_scopes=DECLARED, now=NOW)
        verified, _sub, _reason = await verify_identity(
            http, CREDS, expected_member_sub="AbC123xyz", api_base="https://api.linkedin.test"
        )
    assert observed.healthy is False
    assert verified is not True


def test_the_health_cache_is_keyed_by_credential_version() -> None:
    """A rotated secret cannot be reported healthy on the previous one's probe."""
    from linkedin_publish.auth import TokenObservation

    cache = HealthCache()
    cache.put(
        "bind-1",
        TokenObservation(observed_at=NOW, credential_version="v1", token_active=True),
    )
    assert cache.get("bind-1", "v1") is not None
    assert cache.get("bind-1", "v2") is None

    cache.invalidate("bind-1")
    assert cache.get("bind-1", "v1") is None
