"""Nothing an upstream body says reaches a caller, a log, or an export."""

from __future__ import annotations

import httpx
import pytest

from linkedin_publish._http import map_write_failure, provider_code, request_id
from linkedin_publish.errors import sanitize_detail

pytestmark = pytest.mark.unit

SECRET_POST = "Our Q3 numbers are confidential: revenue 1.2M, three deals pending."


def test_the_submitted_post_is_never_echoed_into_a_failure() -> None:
    """LinkedIn quotes the rejected content back. We keep none of it."""
    response = httpx.Response(
        422,
        json={
            "serviceErrorCode": 100,
            "message": f"Content rejected: {SECRET_POST}",
            "status": 422,
        },
    )
    failure = map_write_failure(response, what="create")
    rendered = repr(failure.as_dict()) + str(failure) + repr(failure)
    assert SECRET_POST not in rendered
    assert "confidential" not in rendered
    assert failure.provider_code == "100"


def test_an_access_token_in_a_body_is_not_retained() -> None:
    response = httpx.Response(
        401, json={"code": "Bearer AQXyz-not-a-real-token", "message": "expired"}
    )
    failure = map_write_failure(response, what="create")
    assert failure.provider_code is None


def test_prose_is_dropped_rather_than_truncated() -> None:
    """Truncating would keep a prefix of whatever the provider echoed."""
    assert sanitize_detail("Content rejected: secret stuff", limit=64) is None
    assert sanitize_detail("REVOKED_ACCESS_TOKEN", limit=64) == "REVOKED_ACCESS_TOKEN"


def test_an_over_long_token_is_dropped() -> None:
    assert sanitize_detail("A" * 65, limit=64) is None


def test_request_id_is_preserved_for_support() -> None:
    response = httpx.Response(500, json={}, headers={"x-li-uuid": "abc-123-def"})
    assert request_id(response) == "abc-123-def"
    assert map_write_failure(response, what="create").request_id == "abc-123-def"


def test_a_non_json_body_yields_no_provider_code() -> None:
    assert provider_code(httpx.Response(502, content=b"<html>Bad Gateway</html>")) is None


def test_as_dict_carries_only_the_safe_fields() -> None:
    failure = map_write_failure(
        httpx.Response(429, json={"serviceErrorCode": 429}, headers={"retry-after": "60"}),
        what="create",
    )
    detail = failure.as_dict()
    assert set(detail) == {
        "code",
        "message",
        "http_status",
        "provider_code",
        "request_id",
        "retryable",
        "not_before",
    }
    assert detail["code"] == "quota_deferred"


def test_credentials_never_render_their_secrets() -> None:
    from linkedin_publish import Credentials

    creds = Credentials(
        access_token="AQ-super-secret-member-token",
        client_id="app-1",
        client_secret="shhh-client-secret",
        credential_version="v3",
    )
    rendered = f"{creds!r} {creds}"
    assert "AQ-super-secret-member-token" not in rendered
    assert "shhh-client-secret" not in rendered
    assert "app-1" in rendered
    assert "v3" in rendered
