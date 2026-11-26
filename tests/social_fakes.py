"""Offline fakes for the social_publish tests: a recorded Graph API and helpers.

`FakeGraph` is an `httpx.MockTransport` handler holding just enough Meta state
to answer the operations the Meta adapter exercises — Page feed/photo
schedules, scheduled and published lists, node reads and deletes, Instagram
containers, the publishing limit, media_publish, the media list, and comment
reads and replies. Every request is recorded (method, path, body), so a test
can count writes, prove a read-only path wrote nothing, and check that no token
or signed URL leaked anywhere but the header and the body.

A fault is queued per operation. `lost` applies the write and then loses the
response (the dangerous case: Meta did it, we did not hear); `lost_before`
loses it without applying; an `httpx.Response` is returned as-is.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl

import httpx

from social_publish import (
    MetaCredentials,
    MetaGraphAdapter,
    MetaPayload,
    OperationCapability,
    SocialApproval,
    SocialBinding,
    SocialKey,
    SocialRecord,
    meta_payload_digest,
)
from social_publish.adapters.meta import comment_version

NOW = datetime(2026, 12, 3, 10, 0, tzinfo=UTC)
PAGE = "100000000000001"
IG = "170000000000001"
TOKEN = "test-page-token-not-a-real-credential"
SIGNED = "https://assets.example/signed/abc?sig=SECRET-SIGNATURE"
IMAGE = "sha256:" + "c" * 64
PATH_PREFIX = "/v27.0/"


@dataclass
class Call:
    method: str
    path: str
    body: dict[str, str]
    authorization: str | None

    @property
    def is_write(self) -> bool:
        return self.method in ("POST", "DELETE")


@dataclass
class FakeGraph:
    page: str = PAGE
    ig: str = IG
    calls: list[Call] = field(default_factory=list)
    scheduled: dict[str, dict[str, object]] = field(default_factory=dict)
    published: dict[str, dict[str, object]] = field(default_factory=dict)
    containers: dict[str, dict[str, object]] = field(default_factory=dict)
    status_script: dict[str, list[str]] = field(default_factory=dict)
    media: dict[str, dict[str, object]] = field(default_factory=dict)
    comments: dict[str, dict[str, object]] = field(default_factory=dict)
    replies: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    faults: dict[str, list[object]] = field(default_factory=dict)
    quota: tuple[int, int] = (0, 50)
    usage_header: str | None = None
    readback_message_override: str | None = None
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(900000000000001))

    # ------------------------------------------------------------ helpers

    def writes(self) -> list[Call]:
        return [call for call in self.calls if call.is_write]

    def ops(self) -> list[str]:
        return [f"{call.method} {call.path}" for call in self.calls]

    def new_id(self) -> str:
        return str(next(self._ids))

    def add_comment(self, comment_id: str, text: str, *, platform: str = "facebook_page") -> str:
        key = "message" if platform == "facebook_page" else "text"
        self.comments[comment_id] = {"id": comment_id, key: text}
        return comment_version(text)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

    def _fault(self, op: str) -> object | None:
        queue = self.faults.get(op)
        return queue.pop(0) if queue else None

    def _json(self, payload: object, status: int = 200) -> httpx.Response:
        headers = {"x-business-use-case-usage": self.usage_header} if self.usage_header else {}
        return httpx.Response(status, json=payload, headers=headers)

    @staticmethod
    def error(
        code: int, subcode: int = 0, *, status: int = 400, message: str = "echo: Belle victoire"
    ) -> httpx.Response:
        return httpx.Response(
            status,
            json={"error": {"code": code, "error_subcode": subcode, "message": message, "fbtrace_id": "TRACE123"}},
        )

    # ------------------------------------------------------------ handler

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        assert path.startswith(PATH_PREFIX), f"unpinned Graph version: {path}"
        rel = path[len(PATH_PREFIX) :]
        raw = request.read()
        body = dict(parse_qsl(raw.decode())) if raw else dict(request.url.params)
        self.calls.append(Call(request.method, rel, body, request.headers.get("authorization")))
        parts = rel.split("/")
        method = request.method

        if method == "POST" and parts == [self.page, "feed"]:
            return self._write("feed", lambda: self._create_schedule(body["message"], body["scheduled_publish_time"]))
        if method == "POST" and parts == [self.page, "photos"]:
            return self._write(
                "photos", lambda: self._create_schedule(body["caption"], body["scheduled_publish_time"], photo=True)
            )
        if method == "GET" and parts == [self.page, "scheduled_posts"]:
            items = [dict(item) for item in self.scheduled.values()]
            if self.readback_message_override is not None:
                for item in items:
                    item["message"] = self.readback_message_override
            return self._json({"data": items})
        if method == "GET" and parts == [self.page, "published_posts"]:
            return self._json({"data": list(self.published.values())})
        if method == "POST" and parts == [self.ig, "media"]:
            return self._write("media_create", lambda: self._create_container(body))
        if method == "GET" and parts == [self.ig, "media"]:
            return self._json({"data": list(self.media.values())})
        if method == "GET" and parts == [self.ig, "content_publishing_limit"]:
            used, total = self.quota
            return self._json({"data": [{"quota_usage": used, "config": {"quota_total": total}}]})
        if method == "POST" and parts == [self.ig, "media_publish"]:
            return self._write("media_publish", lambda: self._publish(body["creation_id"]))
        if len(parts) == 2 and parts[1] in ("comments", "replies"):
            if method == "POST":
                return self._write("reply", lambda: self._reply(parts[0], body["message"]))
            return self._json({"data": self.replies.get(parts[0], [])})
        if len(parts) == 1:
            node = parts[0]
            if method == "DELETE":
                return self._write("delete", lambda: self._delete(node))
            return self._read_node(node)
        return self.error(100, status=400)

    def _write(self, op: str, effect: object) -> httpx.Response:
        fault = self._fault(op)
        if isinstance(fault, httpx.Response):
            return fault
        if fault == "lost_before":
            raise httpx.ReadTimeout("lost before")
        result = effect()  # type: ignore[operator]
        if fault == "lost":
            raise httpx.ReadTimeout("lost after")
        return self._json(result)

    def _create_schedule(self, message: str, when: str, *, photo: bool = False) -> dict[str, object]:
        post_id = f"{self.page}_{self.new_id()}"
        self.scheduled[post_id] = {
            "id": post_id,
            "message": message,
            "scheduled_publish_time": int(when),
            "is_published": False,
        }
        return {"id": self.new_id(), "post_id": post_id} if photo else {"id": post_id}

    def go_live(self, post_id: str) -> None:
        """Meta publishes a scheduled post at its slot."""
        item = self.scheduled.pop(post_id)
        item["is_published"] = True
        self.published[post_id] = item

    def _create_container(self, body: dict[str, str]) -> dict[str, object]:
        container = self.new_id()
        self.containers[container] = {"id": container, "status_code": "IN_PROGRESS", "caption": body["caption"]}
        return {"id": container}

    def _publish(self, container: str) -> dict[str, object]:
        node = self.containers[container]
        media_id = self.new_id()
        node["status_code"] = "PUBLISHED"
        self.media[media_id] = {"id": media_id, "caption": node["caption"]}
        return {"id": media_id}

    def _reply(self, comment_id: str, message: str) -> dict[str, object]:
        reply_id = f"{comment_id}_{self.new_id()}"
        item: dict[str, object] = {"id": reply_id, "message": message, "text": message, "from": {"id": self.page}}
        self.replies.setdefault(comment_id, []).append(item)
        return {"id": reply_id}

    def _delete(self, node: str) -> dict[str, object]:
        self.scheduled.pop(node, None)
        return {"success": True}

    def _read_node(self, node: str) -> httpx.Response:
        if node in self.containers:
            script = self.status_script.get(node)
            if script:
                self.containers[node]["status_code"] = script.pop(0)
            return self._json({"id": node, "status_code": self.containers[node]["status_code"]})
        if node in self.scheduled:
            return self._json(self.scheduled[node])
        if node in self.published:
            return self._json(self.published[node])
        if node in self.comments:
            return self._json(self.comments[node])
        return self.error(100, 33)


class Tokens:
    def __init__(self, version: str = "v1") -> None:
        self.version = version

    async def get(self, binding: SocialBinding) -> MetaCredentials:
        return MetaCredentials(access_token=TOKEN, credential_version=self.version)


class Signer:
    def __init__(self) -> None:
        self.issued: list[tuple[str, str]] = []

    async def issue(self, subject_id: str, handle: str) -> str:
        self.issued.append((subject_id, handle))
        return SIGNED


async def _no_sleep(_seconds: float) -> None:
    return None


def adapter(graph: FakeGraph, **kwargs: object) -> MetaGraphAdapter:
    return MetaGraphAdapter(graph.client(), Tokens(), media=Signer(), sleep=_no_sleep, **kwargs)  # type: ignore[arg-type]


def meta_binding(
    platform: str = "facebook_page",
    *,
    enabled: tuple[tuple[str, str], ...] = (),
    publish_enabled: bool = True,
    binding_id: str | None = None,
) -> SocialBinding:
    native = PAGE if platform == "facebook_page" else IG
    return SocialBinding(
        binding_id=binding_id or f"bind-{platform}",
        account_id="fb-main" if platform == "facebook_page" else "ig-main",
        subject_kind="service",
        subject_id="fixture-brand",
        app_id="app-organic",
        platform=platform,  # type: ignore[arg-type]
        adapter="meta_graph",
        native_account_id=native,
        credential_ref="kv://meta-organic/fixture-brand",
        credential_version="v1",
        capabilities=tuple(
            OperationCapability(
                operation=op, format=fmt, state="enabled", observed_at=NOW, evidence_publication_id="house-receipt"
            )
            for op, fmt in enabled
        ),
        publish_enabled=publish_enabled,
    )


def meta_payload(
    platform: str = "facebook_page",
    *,
    action: str | None = None,
    fmt: str = "text",
    caption: str = "Fixture news: the season starts Saturday.",
    slot: datetime | None = None,
    media: tuple[str, ...] = (),
    parent: dict[str, str] | None = None,
    links: tuple[str, ...] = (),
) -> MetaPayload:
    action = action or ("schedule_post" if platform == "facebook_page" else "publish_post")
    return MetaPayload(
        platform=platform,  # type: ignore[arg-type]
        action=action,  # type: ignore[arg-type]
        format=fmt,
        account={
            "account_id": "fb-main" if platform == "facebook_page" else "ig-main",  # type: ignore[arg-type]
            "native_account_id": PAGE if platform == "facebook_page" else IG,
        },
        parent=parent,  # type: ignore[arg-type]
        caption=caption,
        caption_sha256=hashlib.sha256(caption.encode()).hexdigest(),
        media=media,
        links=links,
        slot_utc=None if action == "reply" else (slot or NOW + timedelta(hours=2)),
    )


def record_for(
    binding: SocialBinding,
    payload: MetaPayload,
    *,
    publication_id: str = "pub-1",
    scheduled_at: datetime | None = None,
    expires_at: datetime | None = None,
    state: str = "pending",
) -> SocialRecord:
    return SocialRecord(
        publication_id=publication_id,
        key=SocialKey(
            subject_kind=binding.subject_kind,
            subject_id=binding.subject_id,
            campaign_id="fixture-brand",
            post_id=publication_id,
            account_id=binding.account_id,
        ),
        binding_id=binding.binding_id,
        platform=binding.platform,
        payload=payload.model_dump(mode="json"),
        payload_digest=meta_payload_digest(payload),
        operation=payload.action,
        format=payload.format,
        state=state,  # type: ignore[arg-type]
        scheduled_at=scheduled_at or NOW - timedelta(minutes=1),
        expires_at=expires_at or NOW + timedelta(hours=3),
        intent_id=f"fixture-brand/{binding.account_id}/{publication_id}/{payload.action}",
    )


def approval_for(record: SocialRecord) -> SocialApproval:
    return SocialApproval(
        approval_id=f"appr-{record.publication_id}",
        publication_id=record.publication_id,
        payload_digest=record.payload_digest,
        binding_id=record.binding_id,
        approved_by="Fixture Operator (test)",
        approved_at=NOW - timedelta(hours=1),
    )


def leaked(graph: FakeGraph, *texts: str) -> list[str]:
    """Any secret that appears outside the Authorization header or the request body."""
    found = [f"path {call.path}" for call in graph.calls if TOKEN in call.path or SIGNED in call.path]
    found.extend(text[:80] for text in texts if TOKEN in text or SIGNED in text or "SECRET-SIGNATURE" in text)
    return found


def dumps(value: object) -> str:
    return json.dumps(value, default=str)
