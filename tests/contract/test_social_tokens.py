"""Meta token-expiry evidence: measured from `debug_token`, never assumed (F32).

Fixtures only — `debug_token` bodies shaped like Graph's reference
(https://developers.facebook.com/docs/graph-api/reference/debug_token/), served
by an `httpx.MockTransport`. No network, no real token.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from social_publish import (
    DispatchService,
    InMemorySocialBindingStore,
    InMemorySocialStore,
    MetaCredentials,
    MetaTokenEvidence,
    SocialBinding,
    inspect_meta_token,
    meta_token_status,
    parse_debug_token,
)
from social_publish.errors import StoreConflict
from tests.social_fakes import NOW, TOKEN, FakeGraph, adapter, approval_for, meta_binding, meta_payload, record_for

pytestmark = [pytest.mark.contract]

APP = "app-organic"
IN_50_DAYS = int((NOW + timedelta(days=50)).timestamp())


def body(**data: object) -> dict[str, object]:
    base: dict[str, object] = {
        "app_id": APP,
        "type": "SYSTEM_USER",
        "application": "Fixture organic app",
        "data_access_expires_at": 0,
        "expires_at": IN_50_DAYS,
        "is_valid": True,
        "issued_at": int((NOW - timedelta(days=10)).timestamp()),
        "scopes": ["pages_manage_posts", "pages_read_engagement", "instagram_content_publish"],
        "user_id": "1000000000000042",
    }
    base.update(data)
    return {"data": base}


def parse(doc: object, *, app: str | None = APP) -> MetaTokenEvidence:
    return parse_debug_token(doc, credential_version="v1", observed_at=NOW, expected_app_id=app)


# ------------------------------------------------------------------ parsing


def test_a_positive_expires_at_is_a_measured_expiry() -> None:
    evidence = parse(body())
    assert evidence.expires_at == datetime.fromtimestamp(IN_50_DAYS, UTC)
    assert not evidence.never_expires and evidence.recordable and evidence.reason is None
    assert evidence.scopes == ("instagram_content_publish", "pages_manage_posts", "pages_read_engagement")


def test_expires_at_zero_on_a_valid_token_of_the_app_is_explicit_no_expiry_evidence() -> None:
    evidence = parse(body(expires_at=0))
    assert evidence.never_expires and evidence.expires_at is None and evidence.recordable


@pytest.mark.parametrize(
    ("doc", "reason"),
    [
        (body(expires_at=0, is_valid=False), "token_invalid"),
        (body(expires_at=0, app_id="another-app"), "app_mismatch"),
        (body(expires_at=None), "expiry_not_reported"),
        (body(expires_at="0"), "expiry_not_reported"),
        ({"error": {"code": 190, "message": "Invalid OAuth access token."}}, "debug_token_error:190"),
        ({"data": []}, "debug_token_malformed"),
        ("not json", "debug_token_malformed"),
    ],
    ids=["invalid", "other-app", "missing", "string", "graph-error", "malformed", "not-a-mapping"],
)
def test_nothing_else_is_ever_read_as_no_expiry(doc: object, reason: str) -> None:
    evidence = parse(doc)
    assert evidence.reason == reason
    assert not evidence.never_expires and evidence.expires_at is None and not evidence.recordable


async def test_inspect_sends_the_token_only_as_input_token_and_header() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=body(expires_at=0))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        evidence = await inspect_meta_token(
            http, MetaCredentials(access_token=TOKEN, credential_version="v1"), expected_app_id=APP, now=NOW
        )

    assert evidence.never_expires
    (request,) = seen
    assert request.method == "GET" and request.url.path == "/v27.0/debug_token"
    assert request.url.params["input_token"] == TOKEN
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert TOKEN not in repr(evidence) and TOKEN not in str(evidence.as_dict())


async def test_a_transport_failure_is_unknown_and_names_no_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        evidence = await inspect_meta_token(
            http, MetaCredentials(access_token=TOKEN, credential_version="v1"), expected_app_id=APP, now=NOW
        )
    assert evidence.reason == "probe_transport:ConnectError" and not evidence.recordable
    assert TOKEN not in str(evidence.as_dict())


# ------------------------------------------------------------------ record


async def test_recording_stores_expiry_or_no_expiry_for_the_current_version_only() -> None:
    binding = meta_binding(publish_enabled=False)
    store = InMemorySocialBindingStore([binding])

    never = await store.record_token_evidence(binding.binding_id, parse(body(expires_at=0)))
    assert never.token_never_expires and never.token_expires_at is None and never.token_observed_at == NOW

    measured = await store.record_token_evidence(binding.binding_id, parse(body()))
    assert not measured.token_never_expires and measured.token_expires_at == datetime.fromtimestamp(IN_50_DAYS, UTC)

    with pytest.raises(StoreConflict, match="credential version"):
        await store.record_token_evidence(
            binding.binding_id,
            parse_debug_token(body(), credential_version="v2", observed_at=NOW, expected_app_id=APP),
        )
    with pytest.raises(StoreConflict, match="no expiry fact"):
        await store.record_token_evidence(binding.binding_id, parse(body(is_valid=False)))


def test_a_binding_cannot_both_never_expire_and_expire() -> None:
    with pytest.raises(ValueError, match="both never expire"):
        meta_binding().model_copy(update={}).model_validate(
            {
                **meta_binding().model_dump(),
                "token_never_expires": True,
                "token_expires_at": NOW,
                "token_observed_at": NOW,
            }
        )


# ------------------------------------------------------------------ status


def measured(**updates: object) -> SocialBinding:
    return SocialBinding.model_validate({**meta_binding().model_dump(), "token_observed_at": NOW, **updates})


def test_status_reports_no_expiry_measured_and_unknown() -> None:
    never = meta_token_status(measured(token_never_expires=True), now=NOW)
    assert never["status"] == "healthy" and never["expiry_warning"] is None
    assert "never_expires" in str(never["reason"])

    later = meta_token_status(measured(token_expires_at=NOW + timedelta(days=50)), now=NOW)
    assert later["status"] == "healthy" and later["expiry_warning"] is None

    unmeasured = meta_token_status(meta_binding(), now=NOW)
    assert unmeasured["status"] == "unknown" and unmeasured["expiry_warning"] is None


def test_the_shared_expiry_warning_covers_meta_bindings() -> None:
    soon = meta_token_status(measured(token_expires_at=NOW + timedelta(days=5, hours=1)), now=NOW)
    assert soon["status"] == "healthy"
    assert soon["expiry_warning"] == {
        "code": "token_expires_soon",
        "expires_at": (NOW + timedelta(days=5, hours=1)).isoformat(),
        "days_remaining": 5,
        "window_days": 14,
        "source": "binding",
    }
    gone = meta_token_status(measured(token_expires_at=NOW - timedelta(hours=1)), now=NOW)
    assert gone["status"] == "blocked"
    assert isinstance(gone["expiry_warning"], dict) and gone["expiry_warning"]["code"] == "token_expired"


async def test_an_expired_token_meta_binding_is_warned_on_the_tick_and_sends_nothing() -> None:
    """Negative control: the tick of an expired-token Meta binding carries the warning and writes nothing."""
    binding = measured(
        token_expires_at=NOW - timedelta(hours=1),
        capabilities=meta_binding(enabled=(("schedule_post", "text"),)).capabilities,
        publish_enabled=True,
    )
    record = record_for(binding, meta_payload())
    store = InMemorySocialStore([binding])
    await store.import_intent(record, approval_for(record), actor="op", at=NOW)
    graph = FakeGraph()

    tick = await DispatchService(store, {"meta_graph": adapter(graph)}).tick(binding, now=NOW, dry_run=False)

    report = tick.as_dict()
    assert report["token_health"] == "blocked"
    warning = report["token_expiry_warning"]
    assert isinstance(warning, dict) and warning["code"] == "token_expired"
    assert graph.writes() == []
    assert [f["code"] for f in tick.failures] == ["token_expired"] and tick.outcome == "partial"
    left = await store.get(record.publication_id)
    assert left is not None and left.state == "pending"  # a gate, not a failure: re-measured, it goes
