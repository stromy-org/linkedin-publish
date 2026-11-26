"""Commissioning a fresh binding: the C1 canary on a capability still `unknown`.

The defect these pin (found 2026-11-26 preparing the canary, v0.5.1): a fresh
binding's `text` is `unknown`, and `LinkedInClient.check` refused it before any
commissioning grant was consulted. So the one approved canary the grant exists
for was counted `skipped_capability` and could never go out; the library's own
grant test passed only because its fixture binding already had `text` enabled.

Plan C1 step 7: a grant for exactly one stored publication, digest, binding and
capability, issued to the trusted CLI commissioning actor, licenses that one
send; scheduler and MCP identities cannot use it; it is consumed at `sending`.

Most refusal tests here are *differential*: the same setup is refused with one
field wrong and sends exactly once with it right. A refusal alone would also
pass on v0.5.1, which refused every `unknown` capability; the second half is
what proves the refusal is caused by the field under test.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from linkedin_publish import CommissioningGrant, PostDraft, PublicationRecord
from linkedin_publish.errors import CapabilityUnavailable
from linkedin_publish.models import CapabilityStatus
from tests.conftest import NOW, PERSON, binding
from tests.contract.test_service import DIGEST, POST_URN, approval, record, seeded

pytestmark = pytest.mark.contract

ACTOR = "william"


def grant(**overrides: object) -> CommissioningGrant:
    fields: dict[str, object] = {
        "grant_id": "grant-1",
        "publication_id": "pub-1",
        "payload_digest": DIGEST,
        "binding_id": "bind-1",
        "capability": "text",
        "issued_by": ACTOR,
        "issued_at": NOW - timedelta(minutes=1),
        "expires_at": NOW + timedelta(minutes=29),
    }
    fields.update(overrides)
    return CommissioningGrant.model_validate(fields)


async def fresh(**kwargs: object):  # noqa: ANN201
    """A freshly registered binding: disabled, every capability `unknown`."""
    kwargs.setdefault("commissioning_actor", ACTOR)
    return await seeded(publish_enabled=False, capabilities=(), **kwargs)  # type: ignore[arg-type]


# ------------------------------------------------------------- the fix itself


async def test_a_grant_licenses_its_unknown_capability_for_exactly_one_send() -> None:
    store, client, log, service, bound = await fresh()
    assert bound.capability_state("text") == "unknown"
    await store.put_grant(grant())

    receipt = await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)

    assert receipt.post_urn == POST_URN
    assert log.count == 1
    stored = await store.get_grant("pub-1")
    assert stored is not None and stored.consumed_at == NOW
    published = await store.get_publication("pub-1")
    assert published is not None and published.state == "published"
    # Sending the canary enables nothing by itself: that is `account enable`.
    assert bound.publish_enabled is False
    assert bound.capability_state("text") == "unknown"
    await client.aclose()


async def test_the_commissioned_canary_is_not_skipped_by_the_tick_preflight() -> None:
    """`publish_due` used to count it `skipped_capability` before ever reaching the grant."""
    store, client, log, service, bound = await fresh()
    await store.put_grant(grant())

    result = await service.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)

    assert (result.skipped_capability, result.published, log.count) == (0, 1, 1)
    await client.aclose()


async def test_a_spent_grant_cannot_license_a_second_send() -> None:
    store, client, log, service, bound = await fresh(
        records=[record(), record(publication_id="pub-2", post_id="post-2")],
    )
    await store.put_grant(grant())
    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1

    # pub-1's grant is consumed; it does not travel to pub-2.
    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-2", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1
    await client.aclose()


async def test_a_consumed_grant_licenses_nothing() -> None:
    store, client, log, service, bound = await fresh()
    await store.put_grant(grant(consumed_at=NOW - timedelta(seconds=30)))
    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0

    await store.put_grant(grant(grant_id="grant-2"))
    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1
    await client.aclose()


# ------------------------------------------------------- every field must match


@pytest.mark.parametrize(
    ("wrong", "label"),
    [
        ({"publication_id": "pub-2"}, "another publication"),
        ({"payload_digest": "9" * 64}, "another digest"),
        ({"binding_id": "bind-2"}, "another binding"),
        ({"capability": "image"}, "another capability"),
        ({"issued_by": "someone-else"}, "another actor"),
        ({"expires_at": NOW - timedelta(seconds=1), "issued_at": NOW - timedelta(minutes=31)}, "expired"),
    ],
)
async def test_a_grant_that_differs_in_one_field_licenses_nothing(wrong: dict[str, object], label: str) -> None:
    extra = [record(publication_id="pub-2", post_id="post-2")] if label == "another publication" else []
    store, client, log, service, bound = await fresh(records=[record(), *extra])
    await store.put_grant(grant(**wrong))

    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0, label
    after = await store.get_publication("pub-1")
    assert after is not None and after.state == "pending", "a refused claim returns to pending"

    await store.put_grant(grant(grant_id="grant-right"))
    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1, f"the same setup with the right grant must send ({label})"
    await client.aclose()


# ------------------------------------------------------------ who may use one


async def test_the_scheduler_cannot_use_a_grant() -> None:
    """No commissioning actor: the scheduler and the MCP never construct one."""
    store, client, log, scheduler, bound = await fresh(commissioning_actor=None)
    await store.put_grant(grant())

    result = await scheduler.publish_due(bound, campaign_id="camp-1", now=NOW, dry_run=False)
    assert (result.skipped_capability, result.published, log.count) == (1, 0, 0)
    with pytest.raises(CapabilityUnavailable):
        await scheduler.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    unspent = await store.get_grant("pub-1")
    assert unspent is not None and unspent.consumed_at is None
    await client.aclose()

    # The same setup, sent by the commissioning actor's service.
    store2, client2, log2, operator, bound2 = await fresh(commissioning_actor=ACTOR)
    await store2.put_grant(grant())
    await operator.publish_one("pub-1", bound2, expected_digest=DIGEST, now=NOW)
    assert log2.count == 1
    await client2.aclose()


async def test_the_scheduler_cannot_use_a_grant_even_on_an_enabled_capability() -> None:
    """Before this release the scheduler consumed grants on a disabled binding."""
    store, client, log, scheduler, bound = await seeded(publish_enabled=False, commissioning_actor=None)
    assert bound.capability_state("text") == "enabled"
    await store.put_grant(grant())
    with pytest.raises(CapabilityUnavailable, match="publishing is disabled"):
        await scheduler.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 0
    await client.aclose()


# ------------------------------------------------- what a grant never overrides


async def test_a_grant_never_licenses_an_unavailable_capability() -> None:
    store, client, log, service, _ = await fresh()
    refused = _with_text("unavailable")
    await store.put_grant(grant())
    with pytest.raises(CapabilityUnavailable, match="unavailable"):
        await service.publish_one("pub-1", refused, expected_digest=DIGEST, now=NOW)
    assert log.count == 0

    unknown = _with_text(None)
    await service.publish_one("pub-1", unknown, expected_digest=DIGEST, now=NOW)
    assert log.count == 1
    await client.aclose()


async def test_a_grant_is_never_honoured_on_a_binding_held_after_rotation() -> None:
    store, client, log, service, bound = await fresh()
    held = bound.model_copy(update={"reenable_after_measure": True})
    await store.put_grant(grant())
    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-1", held, expected_digest=DIGEST, now=NOW)
    assert log.count == 0

    await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1
    await client.aclose()


async def test_a_text_grant_does_not_license_an_image_post() -> None:
    image = PublicationRecord(
        publication_id="pub-1",
        key=record().key,
        binding_id="bind-1",
        payload_digest=DIGEST,
        draft=PostDraft.model_validate(
            {"author_urn": PERSON, "commentary": "chart", "media": {"kind": "image", "asset": {"sha256": "a" * 64}}}
        ),
        scheduled_at=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(hours=23),
    )
    store, client, log, service, bound = await fresh(records=[image])
    await store.put_grant(grant(capability="text"))
    with pytest.raises(CapabilityUnavailable) as caught:
        await service.publish_one("pub-1", bound, expected_digest=DIGEST, now=NOW)
    assert caught.value.capability == "image"
    assert log.count == 0
    await client.aclose()


async def test_an_enabled_binding_still_commissions_a_new_capability_and_spends_the_grant() -> None:
    """Image/document need their own canary on an already-enabled text binding."""
    store, client, log, service, bound = await seeded(
        publish_enabled=True, capabilities=("text",), commissioning_actor=ACTOR
    )
    article = record(publication_id="pub-a", post_id="post-a").model_copy(
        update={
            "draft": PostDraft.model_validate(
                {"author_urn": PERSON, "commentary": "read", "media": {"kind": "article", "url": "https://x.test/a"}}
            )
        }
    )
    await store.upsert_publication(article)
    await store.put_approval(approval("pub-a"))
    with pytest.raises(CapabilityUnavailable):
        await service.publish_one("pub-a", bound, expected_digest=DIGEST, now=NOW)
    await store.put_grant(grant(publication_id="pub-a", capability="article"))
    await service.publish_one("pub-a", bound, expected_digest=DIGEST, now=NOW)
    assert log.count == 1
    spent = await store.get_grant("pub-a")
    assert spent is not None and spent.consumed_at is not None
    await client.aclose()


def _with_text(state: str | None):  # noqa: ANN202
    """The fresh binding with `text` recorded as `state`, or left unmeasured."""
    base = binding(publish_enabled=False, capabilities=())
    if state is None:
        return base
    return base.model_copy(
        update={"capabilities": (CapabilityStatus(capability="text", state=state, observed_at=NOW, reason="probe"),)}  # type: ignore[arg-type]
    )
