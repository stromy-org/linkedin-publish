# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
#
# Graph responses are untrusted JSON; each field read here is type-checked at the
# point of use, and nothing from a response body is returned except ids and the
# exact fields compared against the approved payload.
"""Meta Graph adapter: Facebook Page scheduled posts, Instagram publishing, comment replies.

Pinned to Graph **v27.0**, the version the C3 operator publisher uses
(`social-ops` `references/meta-organic.md` § 1). Every request carries the
version in its path; the adapter never inherits another version.

What it implements, and nothing else (capability is declared per operation and
format, so anything outside this set is refused before transport):

| Platform  | Operation       | Format          | Graph                                                           |
|-----------|-----------------|-----------------|-----------------------------------------------------------------|
| Facebook  | `schedule_post` | `text`, `link`  | `POST /{page}/feed` with `published=false`, `scheduled_publish_time` |
| Facebook  | `schedule_post` | `feed_image`    | `POST /{page}/photos` (`url` = a short-lived signed URL)         |
| Facebook  | `reply`         | `comment_reply` | parent check, then `POST /{comment}/comments`                   |
| Instagram | `publish_post`  | `feed_image`    | container `POST /{ig}/media`, poll, `POST /{ig}/media_publish`   |
| Instagram | `reply`         | `comment_reply` | parent check, then `POST /{comment}/replies`                    |

**Facebook.** A created schedule is read back from `scheduled_posts` (every
page) and must show the created id, the exact message and the slot. A mismatch
is withdrawn (`DELETE` once, then read back absent) and reported `cancelled`; an
id that does not read back is `unknown` with the id kept. A lost create response
is reconciled by searching `scheduled_posts` and `published_posts` for the exact
message (and slot): exactly one match settles it; zero or several stay unknown.

**Instagram.** There is no scheduling parameter: the container is created when
the record is due. Its id is persisted on the claimed row *before* the status
poll, so a crash never loses it. A bounded poll distinguishes `IN_PROGRESS`,
`FINISHED`, `PUBLISHED`, `ERROR` and `EXPIRED`; only `FINISHED` may publish, and
only once (the core's `sending` row is the guard). A lost `media_publish`
response re-queries **the same container**: `PUBLISHED` plus exactly one media
with the approved caption settles it; `ERROR`/`EXPIRED` prove it was never
published; anything else stays `unknown`, across restarts, and is never re-sent.

**Rate limits.** Graph codes 4, 17, 32, 613, 80001, 80002 and 341 defer the
record (`estimated_time_to_regain_access` from `X-Business-Use-Case-Usage` sets
`not_before`); usage above 75 % on any field defers a write before it is sent.
Safe reads retry at most three times; a write is never retried.

**Secrets.** The token goes in the `Authorization` header only. A signed media
URL goes in the form body only. Neither is ever placed in an error, an event or
a return value; errors keep only the Graph code, subcode and `fbtrace_id`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx

from ..errors import Deferred, OutcomeUnknown, Refused
from ..model import MetaPayload, SocialBinding, SocialRecord
from .base import Persist, Resolution, SendResult, StageResult

__all__ = [
    "GRAPH_VERSION",
    "MetaCredentials",
    "MetaGraphAdapter",
    "MetaTokenProvider",
    "SignedMediaUrls",
    "comment_version",
]

GRAPH_VERSION = "v27.0"
GRAPH_BASE = "https://graph.facebook.com"

#: Facebook native schedule lead time (meta-organic § 4: 10 minutes to 30 days;
#: the wider 75-day bound stays unused until C1 observes it).
MIN_LEAD = timedelta(minutes=10)
MAX_LEAD = timedelta(days=30)
MAX_READ_ATTEMPTS = 3
USAGE_BACKOFF_PERCENT = 75.0
DEFAULT_DEFERRAL = timedelta(minutes=15)

DECLARED: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("facebook_page", "schedule_post", "text"),
        ("facebook_page", "schedule_post", "link"),
        ("facebook_page", "schedule_post", "feed_image"),
        ("facebook_page", "reply", "comment_reply"),
        ("instagram_professional", "publish_post", "feed_image"),
        ("instagram_professional", "reply", "comment_reply"),
    }
)

_RATE = {4, 17, 32, 613, 80001, 80002, 341}
_TOKEN = {190, 102}
_PERMISSION = {10, 283}
_RESTRICTION = {368}
_INVALID = {100}
_TRANSIENT = {1, 2}
_IG_TRANSIENT = {2207001, 2207032, 2207053}
_IG_QUOTA = {2207042}
_IG_MEDIA = {2207004, 2207005, 2207009, 2207010, 2207026, 2207028, 2207052, 2207003}
_IG_CONTAINER = {2207027, 2207008, 2207020}
_IG_RESTRICTION = {2207050, 2207051}

_SCHEDULED_FIELDS = "id,message,scheduled_publish_time,is_published,created_time"


def comment_version(text: str) -> str:
    """The version of a comment the approver saw: SHA-256 of its NFC text (C4's rule)."""
    return hashlib.sha256(unicodedata.normalize("NFC", text).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class MetaCredentials:
    """A Page (or BISU) token for exactly one binding, and the version it was issued as."""

    access_token: str
    credential_version: str

    def __repr__(self) -> str:
        return f"MetaCredentials(credential_version={self.credential_version!r})"


class MetaTokenProvider(Protocol):
    async def get(self, binding: SocialBinding) -> MetaCredentials: ...


class SignedMediaUrls(Protocol):
    """Issues a short-lived, read-only URL for one immutable approved object.

    The issuer, not this adapter, re-authorizes the subject and rights and
    verifies the object's hash before minting (plan Decision 15). The URL is a
    secret: the adapter only ever places it in a request body.
    """

    async def issue(self, subject_id: str, handle: str) -> str: ...


Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _Classified:
    kind: str  # "definite" | "deferred" | "transient"
    name: str
    final: bool = True


def _classify(code: int, subcode: int) -> _Classified:
    if subcode in _IG_QUOTA:
        return _Classified("deferred", "quota_exhausted")
    if subcode in _IG_TRANSIENT:
        return _Classified("transient", "transient")
    if subcode in _IG_MEDIA:
        return _Classified("definite", "media_rejected")
    if subcode in _IG_CONTAINER:
        return _Classified("definite", "container_not_ready")
    if subcode in _IG_RESTRICTION:
        return _Classified("definite", "platform_restriction")
    if code in _RATE:
        return _Classified("deferred", "rate_limited")
    if code in _TRANSIENT:
        return _Classified("transient", "transient")
    if code in _TOKEN:
        return _Classified("definite", "token_invalid")
    if code in _PERMISSION or 200 <= code <= 299:
        return _Classified("definite", "missing_permission")
    if code in _RESTRICTION:
        return _Classified("definite", "platform_restriction")
    if code in _INVALID:
        return _Classified("definite", "invalid_parameter")
    return _Classified("definite", "graph_error")


class _GraphError(Exception):
    def __init__(self, classified: _Classified, *, status: int, code: int, subcode: int, trace: object) -> None:
        super().__init__(classified.name)
        self.classified = classified
        self.status = status
        self.provider_code = f"{code}.{subcode}" if subcode else str(code)
        self.trace = trace


class _TransportLost(Exception):
    """No answer arrived (timeout, connection reset, 5xx without a Graph error)."""


def _usage_percent(headers: httpx.Headers) -> tuple[float, float | None]:
    """(max usage percent, max minutes to regain access) over the usage headers."""
    peak = 0.0
    regain: float | None = None
    for name in ("x-business-use-case-usage", "x-app-usage"):
        raw = headers.get(name)
        if not raw:
            continue
        try:
            doc = json.loads(raw)
        except ValueError:
            continue
        entries: list[object] = []
        if isinstance(doc, dict):
            for value in doc.values():
                if isinstance(value, list):
                    entries.extend(value)
                elif isinstance(value, (int, float)):
                    peak = max(peak, float(value))
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            for key in ("call_count", "total_cputime", "total_time"):
                value = entry.get(key)
                if isinstance(value, (int, float)):
                    peak = max(peak, float(value))
            minutes = entry.get("estimated_time_to_regain_access")
            if isinstance(minutes, (int, float)) and minutes > 0:
                regain = max(regain or 0.0, float(minutes))
    return peak, regain


class MetaGraphAdapter:
    """The `meta_graph` adapter. One instance serves any number of Meta bindings."""

    name = "meta_graph"
    platforms: frozenset[str] = frozenset({"facebook_page", "instagram_professional"})

    def __init__(
        self,
        http: httpx.AsyncClient,
        tokens: MetaTokenProvider,
        *,
        media: SignedMediaUrls | None = None,
        api_base: str = GRAPH_BASE,
        version: str = GRAPH_VERSION,
        poll_attempts: int = 5,
        poll_interval: float = 60.0,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if version != GRAPH_VERSION:
            raise ValueError(f"the Meta adapter is pinned to Graph {GRAPH_VERSION}; got {version!r}")
        self._http = http
        self._tokens = tokens
        self._media = media
        self._base = f"{api_base.rstrip('/')}/{version}"
        self._poll_attempts = poll_attempts
        self._poll_interval = poll_interval
        self._sleep = sleep
        self._usage = 0.0

    # ------------------------------------------------------------- contract

    def declared(self) -> frozenset[tuple[str, str]]:
        return frozenset((operation, fmt) for _platform, operation, fmt in DECLARED)

    def declared_for(self, platform: str) -> frozenset[tuple[str, str]]:
        return frozenset((operation, fmt) for p, operation, fmt in DECLARED if p == platform)

    def validate(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> None:
        payload = self._payload(binding, record)
        if (payload.platform, payload.action, payload.format) not in DECLARED:
            raise Refused(
                "format_unsupported",
                f"{payload.platform} {payload.action} {payload.format} is not implemented by the Meta adapter",
            )
        if payload.action == "schedule_post":
            assert payload.slot_utc is not None  # noqa: S101 - MetaPayload enforces it
            lead = payload.slot_utc - now
            if lead < MIN_LEAD or lead > MAX_LEAD:
                raise Refused(
                    "schedule_out_of_bounds",
                    f"a Facebook schedule must be {MIN_LEAD} to {MAX_LEAD} ahead; this slot is {lead} ahead",
                )
            if payload.format == "link" and len(payload.links) != 1:
                raise Refused("format_unsupported", "a link post carries exactly one link")
        if payload.format == "feed_image" and len(payload.media) != 1:
            raise Refused("format_unsupported", "a feed_image post carries exactly one image; never a different format")
        if payload.format in ("text", "link", "comment_reply") and payload.media:
            raise Refused("format_unsupported", f"a {payload.format} post carries no media")
        if payload.media and self._media is None:
            raise Refused("media_unavailable", "this post carries media and no signed-URL issuer is configured")

    async def stage(self, binding: SocialBinding, record: SocialRecord, now: datetime, persist: Persist) -> StageResult:
        payload = self._payload(binding, record)
        if payload.action != "publish_post":
            return StageResult("ready")
        ig = payload.account.native_account_id
        container = record.container_id
        if container is None:
            url = await self._signed(binding, payload.media[0])
            form: dict[str, str] = {"image_url": url, "caption": payload.caption}
            if payload.alt_text and payload.alt_text[0]:
                form["alt_text"] = payload.alt_text[0]
            if payload.ai_label:
                form["is_ai_generated"] = "true"
            try:
                body = await self._write(binding, "POST", f"{ig}/media", form)
            except OutcomeUnknown:
                # Creating a container publishes nothing; an orphan expires
                # unpublished after 24 hours. Release the claim and try again.
                return StageResult("wait", reason="container_create_unknown")
            except Refused as exc:
                if exc.code == "container_not_ready":
                    return StageResult("wait", reason=exc.code)
                return StageResult("failed", reason=exc.code)
            container = _id(body)
            if container is None:
                return StageResult("wait", reason="container_create_unknown")
            # Durable before anything else happens to it.
            await persist({"container_id": container})
        for attempt in range(self._poll_attempts):
            status = await self._container_status(binding, container)
            if status == "FINISHED":
                return StageResult("ready")
            if status == "ERROR":
                return StageResult("failed", reason="media_processing_failed")
            if status == "EXPIRED":
                return StageResult("restage", reason="container_expired")
            if status == "PUBLISHED":
                return StageResult("anomaly", reason="container_already_published")
            if attempt + 1 < self._poll_attempts:
                await self._sleep(self._poll_interval)
        return StageResult("wait", reason="container_not_ready")

    async def preflight(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> None:
        payload = self._payload(binding, record)
        if self._usage > USAGE_BACKOFF_PERCENT:
            raise Deferred(
                "rate_limited",
                f"Graph usage is above {USAGE_BACKOFF_PERCENT:.0f} %; the write waits",
                not_before=now + DEFAULT_DEFERRAL,
            )
        if payload.action == "reply":
            await self._check_parent(binding, payload)
        elif payload.action == "publish_post":
            await self._check_quota(binding, payload, now)

    async def send(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> SendResult:
        payload = self._payload(binding, record)
        if payload.action == "schedule_post":
            return await self._schedule(binding, payload, now)
        if payload.action == "publish_post":
            if record.container_id is None:
                raise Refused("container_not_ready", "no persisted container to publish")
            body = await self._write(
                binding,
                "POST",
                f"{payload.account.native_account_id}/media_publish",
                {"creation_id": record.container_id},
            )
            media_id = _id(body)
            if media_id is None:
                raise OutcomeUnknown("media_publish answered without a media id")
            return SendResult("published", remote_id=media_id, published_at=now)
        assert payload.parent is not None  # noqa: S101 - MetaPayload enforces it
        edge = "comments" if payload.platform == "facebook_page" else "replies"
        body = await self._write(binding, "POST", f"{payload.parent.comment_id}/{edge}", {"message": payload.caption})
        reply_id = _id(body)
        if reply_id is None:
            raise OutcomeUnknown("the reply answered without an id")
        return SendResult("published", remote_id=reply_id, published_at=now)

    async def read_back(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> Resolution:
        """A `scheduled` Facebook post: published at its slot yet, still scheduled, or gone."""
        payload = self._payload(binding, record)
        if payload.action != "schedule_post" or record.remote_id is None:
            return Resolution(None)
        try:
            node = await self._read(binding, record.remote_id, {"fields": _SCHEDULED_FIELDS})
        except Refused as exc:
            if exc.code == "invalid_parameter":
                return Resolution("unknown", remote_id=record.remote_id, reason="remote_missing")
            raise
        if node.get("is_published") is True:
            return Resolution("published", remote_id=record.remote_id)
        return Resolution(None)

    async def reconcile(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> Resolution:
        payload = self._payload(binding, record)
        if payload.action == "schedule_post":
            return await self._reconcile_schedule(binding, record, payload)
        if payload.action == "publish_post":
            return await self._reconcile_container(binding, record, payload)
        return await self._reconcile_reply(binding, payload)

    async def cancel(self, binding: SocialBinding, record: SocialRecord, now: datetime) -> bool:
        payload = self._payload(binding, record)
        if payload.action != "schedule_post" or record.remote_id is None:
            return False
        try:
            await self._write(binding, "DELETE", record.remote_id, {})
        except (Refused, Deferred, OutcomeUnknown):
            return False
        listed = await self._scheduled_posts(binding, payload.account.native_account_id)
        return all(item.get("id") != record.remote_id for item in listed)

    # -------------------------------------------------------- Facebook paths

    async def _schedule(self, binding: SocialBinding, payload: MetaPayload, now: datetime) -> SendResult:
        assert payload.slot_utc is not None  # noqa: S101 - validated
        page = payload.account.native_account_id
        when = str(int(payload.slot_utc.timestamp()))
        if payload.format == "feed_image":
            url = await self._signed(binding, payload.media[0])
            form = {"url": url, "caption": payload.caption, "published": "false", "scheduled_publish_time": when}
            if payload.alt_text and payload.alt_text[0]:
                form["alt_text_custom"] = payload.alt_text[0]
            body = await self._write(binding, "POST", f"{page}/photos", form)
            created = _id(body, prefer="post_id")
        else:
            form = {"message": payload.caption, "published": "false", "scheduled_publish_time": when}
            if payload.format == "link":
                form["link"] = payload.links[0]
            body = await self._write(binding, "POST", f"{page}/feed", form)
            created = _id(body)
        if created is None:
            raise OutcomeUnknown("the schedule answered without an id")
        try:
            listed = await self._scheduled_posts(binding, page)
        except (Refused, Deferred) as exc:
            raise OutcomeUnknown(f"created {created} but the read-back failed ({exc.code})", remote_id=created) from exc
        match = next((item for item in listed if item.get("id") == created), None)
        if match is None:
            raise OutcomeUnknown("the created schedule did not read back", remote_id=created)
        if match.get("message") != payload.caption or str(match.get("scheduled_publish_time")) != when:
            if await self.cancel(binding, _with_remote(binding, payload, created), now):
                return SendResult("cancelled", remote_id=created)
            raise OutcomeUnknown("the read-back did not match and the withdrawal did not verify", remote_id=created)
        return SendResult("scheduled", remote_id=created)

    async def _reconcile_schedule(
        self, binding: SocialBinding, record: SocialRecord, payload: MetaPayload
    ) -> Resolution:
        assert payload.slot_utc is not None  # noqa: S101 - validated
        page = payload.account.native_account_id
        if record.remote_id is not None:
            try:
                node = await self._read(binding, record.remote_id, {"fields": _SCHEDULED_FIELDS})
            except Refused:
                return Resolution(None, reason="created_object_unreadable")
            if node.get("message") != payload.caption:
                return Resolution(None, reason="created_object_differs")
            state = "published" if node.get("is_published") is True else "scheduled"
            return Resolution(state, remote_id=record.remote_id)
        when = str(int(payload.slot_utc.timestamp()))
        scheduled = [
            item
            for item in await self._scheduled_posts(binding, page)
            if item.get("message") == payload.caption and str(item.get("scheduled_publish_time")) == when
        ]
        published = [
            item
            for item in await self._paged(binding, f"{page}/published_posts", "id,message,created_time")
            if item.get("message") == payload.caption
        ]
        matches = [("scheduled", item) for item in scheduled] + [("published", item) for item in published]
        if len(matches) != 1:
            return Resolution(None, reason=f"{len(matches)} matches")
        state, item = matches[0]
        return Resolution(state, remote_id=str(item.get("id")))  # type: ignore[arg-type]

    async def _scheduled_posts(self, binding: SocialBinding, page: str) -> list[dict[str, object]]:
        return await self._paged(binding, f"{page}/scheduled_posts", _SCHEDULED_FIELDS)

    # ------------------------------------------------------- Instagram paths

    async def _container_status(self, binding: SocialBinding, container: str) -> str:
        node = await self._read(binding, container, {"fields": "status_code"})
        return str(node.get("status_code") or "")

    async def _check_quota(self, binding: SocialBinding, payload: MetaPayload, now: datetime) -> None:
        body = await self._read(
            binding,
            f"{payload.account.native_account_id}/content_publishing_limit",
            {"fields": "quota_usage,config"},
        )
        data = body.get("data")
        entry = data[0] if isinstance(data, list) and data else None
        usage = entry.get("quota_usage") if isinstance(entry, dict) else None
        config = entry.get("config") if isinstance(entry, dict) else None
        total = config.get("quota_total") if isinstance(config, dict) else None
        if not isinstance(usage, int) or not isinstance(total, int):
            raise Deferred(
                "quota_unreadable", "the publishing quota did not read back", not_before=now + DEFAULT_DEFERRAL
            )
        if usage >= total:
            raise Deferred(
                "quota_exhausted", f"publishing quota {usage}/{total} is used up", not_before=now + timedelta(hours=1)
            )

    async def _reconcile_container(
        self, binding: SocialBinding, record: SocialRecord, payload: MetaPayload
    ) -> Resolution:
        if record.container_id is None:
            return Resolution(None, reason="no_container")
        try:
            status = await self._container_status(binding, record.container_id)
        except (Refused, Deferred):
            return Resolution(None, reason="container_unreadable")
        if status in ("ERROR", "EXPIRED"):
            # A container that errored or expired was never published.
            return Resolution("failed", reason=f"container_{status.lower()}")
        if status != "PUBLISHED":
            return Resolution(None, reason=f"container_{status.lower() or 'blank'}")
        items = [
            item
            for item in await self._paged(binding, f"{payload.account.native_account_id}/media", "id,caption,timestamp")
            if item.get("caption") == payload.caption
        ]
        if len(items) != 1:
            return Resolution(None, reason="published_unlocated")
        return Resolution("published", remote_id=str(items[0].get("id")))

    # ---------------------------------------------------------- reply paths

    async def _check_parent(self, binding: SocialBinding, payload: MetaPayload) -> None:
        assert payload.parent is not None  # noqa: S101 - MetaPayload enforces it
        fields = "id,message,parent" if payload.platform == "facebook_page" else "id,text,parent_id"
        try:
            node = await self._read(binding, payload.parent.comment_id, {"fields": fields})
        except Refused as exc:
            if exc.code == "invalid_parameter":
                raise Refused("parent_missing", "the parent comment no longer reads back", final=True) from exc
            raise
        text = node.get("message") if payload.platform == "facebook_page" else node.get("text")
        if not isinstance(text, str) or node.get("id") != payload.parent.comment_id:
            raise Refused("parent_missing", "the parent comment no longer reads back", final=True)
        if comment_version(text) != payload.parent.comment_version:
            raise Refused("parent_changed", "the parent comment changed since approval; re-triage and re-approve")

    async def _reconcile_reply(self, binding: SocialBinding, payload: MetaPayload) -> Resolution:
        assert payload.parent is not None  # noqa: S101 - MetaPayload enforces it
        if payload.platform == "facebook_page":
            items = await self._paged(binding, f"{payload.parent.comment_id}/comments", "id,message,from")
            ours = [
                item
                for item in items
                if item.get("message") == payload.caption
                and isinstance(item.get("from"), dict)
                and item["from"].get("id") == payload.account.native_account_id  # type: ignore[union-attr]
            ]
        else:
            items = await self._paged(binding, f"{payload.parent.comment_id}/replies", "id,text")
            ours = [item for item in items if item.get("text") == payload.caption]
        if len(ours) != 1:
            return Resolution(None, reason=f"{len(ours)} matches")
        return Resolution("published", remote_id=str(ours[0].get("id")))

    # ------------------------------------------------------------- transport

    def _payload(self, binding: SocialBinding, record: SocialRecord) -> MetaPayload:
        payload = record.meta_payload()
        if payload.platform != binding.platform or payload.account.native_account_id != binding.native_account_id:
            raise Refused("binding_mismatch", "the payload targets another account than its binding", final=True)
        return payload

    async def _signed(self, binding: SocialBinding, handle: str) -> str:
        if self._media is None:
            raise Refused("media_unavailable", "no signed-URL issuer is configured")
        try:
            return await self._media.issue(binding.subject_id, handle)
        except (PermissionError, FileNotFoundError) as exc:
            raise Refused(
                "media_unavailable", f"the approved media could not be issued ({type(exc).__name__})"
            ) from exc

    async def _headers(self, binding: SocialBinding) -> dict[str, str]:
        credentials = await self._tokens.get(binding)
        if credentials.credential_version != binding.credential_version:
            raise Refused(
                "credential_version_mismatch",
                f"the token is version {credentials.credential_version!r}; the binding names "
                f"{binding.credential_version!r}",
            )
        return {"Authorization": f"Bearer {credentials.access_token}"}

    async def _call(self, binding: SocialBinding, method: str, path: str, params: dict[str, str]) -> dict[str, object]:
        headers = await self._headers(binding)
        url = f"{self._base}/{path.lstrip('/')}"
        try:
            if method == "GET":
                response = await self._http.request(method, url, params=params, headers=headers)
            else:
                response = await self._http.request(method, url, data=params, headers=headers)
        except httpx.HTTPError as exc:
            raise _TransportLost(type(exc).__name__) from exc
        peak, regain = _usage_percent(response.headers)
        self._usage = peak
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {}
        error = body.get("error") if isinstance(body, dict) else None
        if response.status_code >= 400 or isinstance(error, dict):
            if not isinstance(error, dict):
                if response.status_code >= 500:
                    raise _TransportLost(f"http {response.status_code}")
                error = {}
            code = _int(error.get("code"))
            subcode = _int(error.get("error_subcode"))
            classified = _classify(code, subcode)
            if response.status_code >= 500 and classified.name == "graph_error":
                raise _TransportLost(f"http {response.status_code}")
            failure = _GraphError(
                classified,
                status=response.status_code,
                code=code,
                subcode=subcode,
                trace=error.get("fbtrace_id"),
            )
            failure.regain = regain  # type: ignore[attr-defined]
            raise failure
        return body if isinstance(body, dict) else {}

    async def _read(self, binding: SocialBinding, path: str, params: dict[str, str]) -> dict[str, object]:
        last: Exception | None = None
        for attempt in range(MAX_READ_ATTEMPTS):
            try:
                return await self._call(binding, "GET", path, params)
            except _TransportLost as exc:
                last = exc
            except _GraphError as exc:
                if exc.classified.kind != "transient":
                    raise _refusal(exc) from exc
                last = exc
            if attempt + 1 < MAX_READ_ATTEMPTS:
                await self._sleep(2.0**attempt)
        raise Deferred("transient", f"a safe read failed {MAX_READ_ATTEMPTS} times") from last

    async def _paged(self, binding: SocialBinding, path: str, fields: str) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        params = {"fields": fields, "limit": "100"}
        for _page in range(50):
            body = await self._read(binding, path, params)
            data = body.get("data")
            if isinstance(data, list):
                out.extend(item for item in data if isinstance(item, dict))
            paging = body.get("paging")
            cursors = paging.get("cursors") if isinstance(paging, dict) else None
            after = cursors.get("after") if isinstance(cursors, dict) else None
            has_next = isinstance(paging, dict) and paging.get("next")
            if not after or not has_next:
                return out
            params = {"fields": fields, "limit": "100", "after": str(after)}
        raise Deferred("transient", "paging did not end within 50 pages")

    async def _write(self, binding: SocialBinding, method: str, path: str, form: dict[str, str]) -> dict[str, object]:
        try:
            return await self._call(binding, method, path, form)
        except _TransportLost as exc:
            raise OutcomeUnknown(f"no answer to the write ({exc})") from exc
        except _GraphError as exc:
            if exc.classified.kind == "transient":
                raise OutcomeUnknown(
                    "a transient Graph error after a write",
                    http_status=exc.status,
                    provider_code=exc.provider_code,
                    request_id=exc.trace,
                ) from exc
            raise _refusal(exc) from exc


def _refusal(exc: _GraphError) -> Refused | Deferred:
    if exc.classified.kind == "deferred":
        regain = getattr(exc, "regain", None)
        not_before = datetime.now(UTC) + (timedelta(minutes=regain) if regain else DEFAULT_DEFERRAL)
        return Deferred(
            exc.classified.name,
            "Graph asked to wait",
            not_before=not_before,
            http_status=exc.status,
            provider_code=exc.provider_code,
            request_id=exc.trace,
        )
    return Refused(
        exc.classified.name,
        "Graph refused the request",
        final=exc.classified.final,
        http_status=exc.status,
        provider_code=exc.provider_code,
        request_id=exc.trace,
    )


def _int(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _id(body: dict[str, object], *, prefer: str | None = None) -> str | None:
    for key in (prefer, "id"):
        if key and isinstance(body.get(key), (str, int)):
            return str(body[key])
    return None


def _with_remote(binding: SocialBinding, payload: MetaPayload, remote_id: str) -> SocialRecord:
    """A throwaway record for `cancel` during a send (the core persists the real one)."""
    return SocialRecord(
        publication_id="-",
        key={
            "subject_kind": binding.subject_kind,
            "subject_id": binding.subject_id,
            "campaign_id": "-",  # type: ignore[arg-type]
            "post_id": "-",
            "account_id": binding.account_id,
        },
        binding_id=binding.binding_id,
        platform=payload.platform,
        payload=payload.model_dump(mode="json"),
        payload_digest="-",
        operation=payload.action,
        format=payload.format,
        state="sending",
        scheduled_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(seconds=1),
        remote_id=remote_id,
    )
