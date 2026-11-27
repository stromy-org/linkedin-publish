# LinkedIn Publish (`social_publish`)

Official-API LinkedIn and Meta organic publishing for Stromy: small HTTP
clients, and a durable, approval-gated publication ledger on top of them.

Client-neutral by construction. Nothing here reads `client-data`, resolves a
`client_slug`, parses an Entra claim, or knows what a brand is. It takes resolved
data and opaque trusted subjects; brand knowledge lives in the L3 skills and
credential resolution in the hosted MCP.

**Status: engineering-complete for C0 + C2 of ORG-PLAN-285 and the library half
of ORG-PLAN-652 C5a — 492 tests green, including the durable tier against a real
Postgres; no capability is commissioned.** Every binding ships `publish_enabled=false` with every capability
`unknown`, so this library refuses to publish until a recorded canary receipt
says otherwise. It has never been run against LinkedIn.

## `social_publish`: the platform-neutral core (v0.8.0)

Since v0.8.0 the wheel carries a second, **canonical** import namespace,
`social_publish` (ORG-PLAN-652 C5a). The distribution is still
`linkedin-publish`, and every public `linkedin_publish` import, CLI command,
serialized LinkedIn payload and digest is unchanged — the v0.7.0 digests are
pinned in `tests/unit/test_social_compat.py`. The rename of the distribution is
a later, separate release (C5b).

| The core owns | An adapter owns |
|---|---|
| immutable intent and its digest, scoped approval, scheduling windows, claims, attempt state, receipts, reconciliation | native ids, credentials, format and visibility rules, media staging, rate-limit interpretation, provider read-back |

```python
from social_publish import DispatchService, MetaGraphAdapter, import_c3_approval
from social_publish.linkedin import LinkedInClient, PublicationService  # the same objects as linkedin_publish
```

- **Capability per account, operation and format.** A Meta binding enables, for
  example, `(schedule_post, text)` and nothing else. The core refuses any other
  pair before transport, and never sends a record as a "nearby" format.
- **No fallback adapter.** The adapter comes from the trusted binding. After an
  uncertain response the record is `unknown`, reconciled read-only by the same
  adapter, and never sent again — by that adapter or any other.
- **Meta adapter, pinned to Graph v27.0** (the version the operator publisher
  uses): Facebook Page scheduled posts (`text`, `link`, `feed_image`) with
  read-back from `scheduled_posts` and cancel-then-verify; Instagram
  `feed_image` through the container lifecycle (`IN_PROGRESS`, `FINISHED`,
  `PUBLISHED`, `ERROR`, `EXPIRED`), with the container id persisted before
  `media_publish` and a lost publish answer re-queried on the same container,
  `unknown` until resolved, across restarts; comment replies with a live parent
  check. Carousels, reels and stories are not implemented and are refused.
- **Trusted approval import.** `import_c3_approval` verifies a
  `social-ops-approval/1` record (Ed25519 signature by a trusted key, actor,
  revocation, grant, caption digest, account) and imports the original intent id
  with its dispatch state in one transaction. A remote schedule, a human handoff,
  or a sent or unknown attempt never becomes fresh work; importing the same
  approval twice returns the same row. The C3 publisher must be paused for the
  binding first (`source_not_quiesced` otherwise).
- **Rollback switch:** `PostgresSocialBindingStore.disable_platform(...)` turns
  Meta publishing off. Ticks still expire, recover leases, read back schedules
  and reconcile unknowns; they send nothing.

Install the `meta` extra (`cryptography`) to verify approvals.

## The one guarantee

Not exactly-once — that is not achievable across an external API and a database,
and is not claimed. What is guaranteed:

> **No automatic replay after an ambiguous send.**

A timeout, a crash, a 5xx or a missing receipt leaves the record `unknown`, and
nothing automated moves it from there. Not a retry, not the next tick, not a new
process, not a token rotation, not a re-import. A human reconciles it against the
actual post, with `linkedin-publish publication reconcile --publication <id>
--actor <you> (--post-urn <urn> | --failure-reason "<what you found>")`.

A `published` post that its owner later deletes on LinkedIn is recorded with
`linkedin-publish publication record-removed --publication <id> --actor <you>
--reason "<how you know>"` (migration 0008). Ledger only, never a provider call:
the row becomes the terminal `removed`, its receipt stays, who/when/why is a
`removed_externally` event, and the post no longer counts as `account enable
--evidence`.

## Install

```bash
uv sync                    # client only
uv sync --extra postgres   # + the durable ledger
uv sync --extra meta       # + approval verification for the Meta path
```

## Two layers

```python
from linkedin_publish import LinkedInClient, PublicationService

# Transport only. No database. Testable against fixtures, no credential needed.
async with LinkedInClient() as client:
    prepared = await client.prepare(binding, draft)      # zero requests

# Durable. Every hosted publishing path goes through this.
service = PublicationService(store, client, credentials)
result = await service.publish_due(binding, campaign_id="...", dry_run=True)
```

| Module | Holds |
|---|---|
| `models` | The public contracts: `PostDraft`, `PublishReceipt`, `AccountBinding` |
| `urns` | URN parsing and resource-type validation |
| `errors` | The typed failure taxonomy; the sanitization chokepoint |
| `auth` | Credentials, token introspection, OIDC identity, health cache |
| `media` | Asset resolution, byte sniffing, upload transport policy |
| `adapters/` | `share_ugc` and `rest_posts` — two complete, independent paths |
| `client` | Adapter selection, capability checks, quota reservation |
| `store` | The ledger: records, protocol, in-memory implementation |
| `service` | Claim → gates → durable `sending` → HTTP → receipt |
| `limits` | Request budgets and retry policy |
| `analytics` | Organization share statistics (CMA-gated, bounded windows; disabled by default) |
| `cma` | The one switch, `cma_organization_enabled`, for every CMA organization capability |
| `manifest` | Publish manifest v1 and the canonical digest |
| `postgres` | The durable store, budgets and migrations (`postgres` extra) |

## CMA organization capabilities: shipped disabled

Three capabilities act as a LinkedIn organization, so they need the Community
Management API product, which no app here has yet:

| Capability | Endpoints (versioned REST, `LinkedIn-Version` from `version.py`) | Scope and role |
|---|---|---|
| Organization publishing: a post authored as `urn:li:organization:*` (text, article, image) | `POST /rest/posts` (and `/v2/ugcPosts` on the Share product) | `w_organization_social`; ADMINISTRATOR, DIRECT_SPONSORED_CONTENT_POSTER or CONTENT_ADMIN |
| Organization documents | `POST /rest/documents?action=initializeUpload` (owner = the organization), `PUT` to the pre-signed URL, then `POST /rest/posts` with `content.media` | the same |
| Organization share statistics | `GET /rest/organizationalEntityShareStatistics?q=organizationalEntity` | `rw_organization_admin`; ADMINISTRATOR |

The adapters and their wire shapes exist and have fixture tests
(`tests/contract/test_cma_switch.py`). **Every one refuses by default** with
`CmaOrganizationDisabled` (code `capability_unavailable`, `switch:
cma_organization_enabled`), before any request and before any binding state is
read. There is one switch: `LinkedInClient(cma_organization_enabled=True)`,
which the client passes down to both adapters and to `fetch_share_statistics`
(each also takes the keyword and refuses on its own). The switch is necessary,
never sufficient. The binding must still list the organization in
`allowed_organization_urns` and carry commissioned evidence for the capability.
A person-authored post never consults the switch. Turn it on only in a
deployment whose app has the CMA product approved.

## Design rules worth knowing before editing

- **The adapter comes from the binding**, before validation or upload — never
  from the draft, never from a previous failure. A 403 on `rest_posts` does not
  cause a `share_ugc` attempt.
- **Capabilities are enabled by recorded commissioning evidence**, one shape at a
  time. A successful text post enables `text` and nothing else.
- **`sending` is committed before the request leaves.** The reverse ordering is
  what produces duplicate posts.
- **A SHA-256 handle is an identifier, not an authorization.** Every asset read
  passes the subject to the injected reader.
- **No upstream body reaches a caller.** Only a sanitized status, provider code
  and request id survive — LinkedIn echoes rejected content back.
- **Approval is over a digest.** Change a character, a binding, a minute or an
  asset and the approval no longer applies.

## Offline check

```bash
uv run linkedin-publish manifest validate tests/fixtures/publish-manifest.valid.json
```

Prints validity and the canonical digest. Reads no credential, opens no database,
makes no network call — and says `UNAPPROVED`, because validation is not
authorization to publish.

## Commissioning an account

The first two operator steps of ORG-PLAN-285 C1, in order. The credential is read
from the environment only — `LINKEDIN_ACCESS_TOKEN`, `LINKEDIN_CLIENT_ID`,
`LINKEDIN_CLIENT_SECRET`, optionally `LINKEDIN_CREDENTIAL_VERSION` — never from an
argument, so it never reaches shell history.

```bash
# 1. Probe the freshly minted token: active? which scopes? which app? whose account?
#    Prints the app-scoped author URN and the MEASURED expiry. Writes nothing.
uv run linkedin-publish account inspect

# 2. Register the binding under the writer role. Created DISABLED; enables nothing.
#    --credential-ref is where the secret lives (e.g. a Key Vault secret name), never the value.
uv run linkedin-publish account register \
  --account-id william-personal --subject-id <operator Entra object id> \
  --app-id <App A client id> --author-urn urn:li:person:<sub from step 1> \
  --credential-ref <secret name> --credential-version <secret version> --dsn "$WRITER_DSN"

# 3. Re-inspect against the binding and store the measured scopes + expiry on it.
uv run linkedin-publish account inspect --binding <id> --dsn "$WRITER_DSN" --record
```

`inspect` exits `0` healthy (or a clean probe), `1` known bad — inactive, wrong
app, wrong member, missing scope — and `2` unknown: the probe could not complete,
which is never read as "no token". A binding with a different member, app or
adapter under an existing id is refused: that is a new binding with fresh
approvals, not an edit. For the CMA app, which has no OIDC product, pass
`--no-identity`; its identity then stays unproven, which keeps it unactivatable.

### The canary: import, approve, commission, send, enable

A fresh binding is disabled with every capability `unknown`. The canary is the
one send allowed on it: a grant for exactly that publication, digest, binding
and capability, issued to your commissioning identity, licenses that single
send through the shared service, and is consumed at the durable `sending`
transition. The scheduler and the MCP can never use a grant. A text canary
enables text and nothing else (ORG-PLAN-285 C1 steps 7 and 8).

```bash
# 4. Store the post as an UNAPPROVED pending publication. The CLI twin of the
#    hosted `import_manifest` tool: same digest-derived publication id, same key.
#    Re-importing identical bytes is a no-op; a changed payload is refused.
uv run linkedin-publish manifest import canary.json --binding <id> --dsn "$WRITER_DSN"

# 5. Approve the exact stored bytes (shows them, asks; there is no auto-yes),
#    then write the one-shot grant for that single send.
uv run linkedin-publish approval record --publication <pub id> --digest <digest> \
  --binding <id> --actor <you> --dsn "$WRITER_DSN"
uv run linkedin-publish account commission --publication <pub id> --digest <digest> \
  --binding <id> --capability text --actor <you> --dsn "$WRITER_DSN"

# 6. Send that ONE publication, as the same --actor, with the measured credential
#    in the environment (LINKEDIN_CREDENTIAL_VERSION must be the binding's).
#    Shows the binding, capability and grant, then asks. Exit 0 published,
#    1 refused, 3 outcome unknown: look at LinkedIn, then `publication reconcile`.
#    The DSN's role is linkedin_publish_commissioner (migration 0006): it runs the
#    send transitions, spends the grant and the quota, and nothing else. The
#    writer role alone cannot send; the runtime role can no longer spend a grant.
uv run linkedin-publish publish due --binding <id> --publication <pub id> --actor <you> --dsn "$LEDGER_DSN"

# 7. Enable from the receipt: refused unless <pub id> is `published` on this
#    binding, of exactly this capability's shape, and the credential is measured
#    healthy. Shows before -> after, then asks. Sets the capability `enabled`
#    with the publication as evidence, and turns publishing on.
uv run linkedin-publish account enable --binding <id> --capability text --evidence <pub id> --dsn "$WRITER_DSN"
```

### The Meta canary (v0.9.0)

The same route for a Meta binding, under `meta` (the LinkedIn verbs above are
unchanged). A grant names one stored Meta publication, its digest, binding,
`operation:format` and actor; it licenses that one send only while the pair is
`unknown` — never an `unavailable` pair, a held binding or another format — and
only `meta publish-due` (a dispatcher built with the commissioning actor; the
scheduler never is) can spend it, at `sending`. `meta enable` enables exactly the
receipt's pair, and only from a send this ledger made: `published`, or for a
Facebook native schedule `scheduled` after its read-back. Every writing verb has
a pre-flight, a confirmation and `--dry-run`; every refusal is named.

```bash
# Token evidence from Graph debug_token: a measured expiry, or explicit
# no-expiry evidence (`expires_at: 0` on a valid token of the binding's app).
# META_ACCESS_TOKEN / META_CREDENTIAL_VERSION come from the environment only.
uv run linkedin-publish meta inspect --binding <id> --record --dsn "$WRITER_DSN"
uv run linkedin-publish meta status --binding <id> --dsn "$READER_DSN"
uv run linkedin-publish meta commission --publication <pub id> --digest <digest> --binding <id> \
    --operation schedule_post --format text --actor <operator> --dsn "$WRITER_DSN"
uv run linkedin-publish meta publish-due --binding <id> --publication <pub id> --actor <operator> \
    --dsn "$COMMISSIONER_DSN"
uv run linkedin-publish meta enable --binding <id> --operation schedule_post --format text \
    --evidence <pub id> --dsn "$WRITER_DSN"
```

The operator CLI holds no media-signing authority, so a media canary
(`feed_image`) is refused there by name (`media_unavailable`); it is sent by a
host that builds `DispatchService(..., commissioning_actor=...)` with its own
signed-URL issuer. A C3 operation is handed over with
`social_publish.import_c3_handover(store, binding, doc, ...)`, where `doc` is
`publish.py export-source`'s `social-ops-handover/1` document.

### Meta registration, rotation and rights (v0.10.0)

```bash
# Registers DISABLED with every capability unknown: configuration, not permission.
# Refuses a non-Meta platform, a non-numeric account id, a duplicate id or account.
# No token on the command line; --credential-ref is a Key Vault secret NAME.
uv run linkedin-publish meta register --binding <id> --account-id <logical> --platform facebook_page \
    --native-account-id <page id> --app-id <app id> --subject-id <subject> \
    --credential-ref <kv secret name> --credential-version <kv secret version> --dsn "$WRITER_DSN" [--dry-run]
# Moves to a new credential version, clears the measurement, turns publishing off and
# holds the binding (no grant is honoured). Then measure the NEW secret and re-enable.
uv run linkedin-publish meta rotate --binding <id> --credential-ref <name> --credential-version <new> \
    --dsn "$WRITER_DSN" [--dry-run]
META_CREDENTIAL_VERSION=<new> uv run linkedin-publish meta inspect --binding <id> --record --dsn "$WRITER_DSN"
uv run linkedin-publish meta reenable --binding <id> --dsn "$WRITER_DSN" [--dry-run]
```

No migration: the writer's existing UPDATE covers the rotation columns. A C3
envelope's `rights_evidence` is kept on the ledger row under
`origin["rights_evidence"]` (outside every digest). A media issuer calls
`social_publish.require_rights(record, now=..., platform=...)` before signing: it
refuses expired rights, an uncovered platform and a record with no retained
evidence.

### Renewal (token rotation)

A member token lives about 60 days. A re-minted token keeps its binding, its
approvals and its ledger; only the credential version moves. Needs migration
`0005_token_health` (`db migrate`).

```bash
# 1. Move the binding to the new secret version. DISABLES publishing in the same
#    write and clears the stored measurement and the health cache.
uv run linkedin-publish account rotate --binding <id> \
  --credential-ref <where the new secret lives> --credential-version <new version> --dsn "$WRITER_DSN"

# 2. With the NEW secret in the environment, measure and record it.
uv run linkedin-publish account inspect --binding <id> --dsn "$WRITER_DSN" --record

# 3. Restore publishing — refused unless the binding was enabled before the
#    rotation and step 2 measured the new version healthy after it.
uv run linkedin-publish account reenable --binding <id> --dsn "$WRITER_DSN"

# Any time, no credential and no request: stored expiry, verdict, <=14-day warning.
uv run linkedin-publish account status --binding <id> --dsn "$READER_DSN"
```

Published and `unknown` publications are never reset by a rotation; pending
approved ones go out only after the re-enable. `reenable` is not the canary's
first enablement and cannot become one.

**Token health** (`linkedin_publish.health`): `assess` turns the last observation
into `healthy`, `blocked` (expired, inactive/revoked, wrong app, wrong member,
missing scope, version mismatch — these block every write, including in
`PublicationService`) or `unknown` (a failed probe or no measurement — never read
as "no token"). `expiry_warning` fires inside 14 days of the **measured** expiry,
never `now + 60 days`. `refresh_reason` says when a probe is owed (missing,
daily, or before a due batch). Observations are cached per
`(binding_id, credential_version)` in `token_observations`.

## Tests

```bash
uv run pytest tests/unit tests/contract     # offline; no credential, no database
uv run pytest tests/integration             # needs a real Postgres (see below)
```

The integration tier exercises what an in-memory store cannot: two processes
racing a compare-and-set, a transaction rolling back a partial quota reservation,
and column-level `GRANT`s actually refusing a write. **CI runs it against a
Postgres service container**, and a skip there is a hard failure rather than a
pass — that is the point, since a silently-skipped tier is green and proves
nothing. Locally it skips without a DSN; to run it:

```bash
docker run --rm -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:16
export LINKEDIN_PUBLISH_TEST_DSN=postgres://postgres:pg@localhost:5432/postgres
uv run pytest tests/integration -v
```

## Database

Owned here, in its own `linkedin_publish` schema — not in the Stromy registry
shim or in `workflow-runtime-core`, whose tables answer "did this run happen",
which cannot survive an accepted POST followed by a crash.

```bash
uv run linkedin-publish db plan                  # what this build ships
uv run linkedin-publish db migrate --dsn ...     # under the migration identity
```

The ledger needs nothing but Postgres, so it runs on any server — Azure, AWS RDS,
a client's own. Only the login differs, and that is the one seam
(`linkedin_publish.pg_auth`): `--pg-auth password` (the default) uses the DSN as
written; `--pg-auth entra` (`LINKEDIN_PUBLISH_PG_AUTH=entra`, the `azure` extra)
gives each new connection a short-lived Microsoft Entra token for a server that
refuses passwords. Any other cloud login is a `password_provider` coroutine passed
to `pool_kwargs()`.

Two roles, and the split is the point: `linkedin_publish_runtime` moves
publications through delivery states and cannot create an approval, author a
binding, or rewrite an approved payload; `linkedin_publish_writer` registers
bindings, records token observations on them, mints approvals and
commissioning grants, and records the operator's outcome for an `unknown` send
(outcome columns only — migration 0003). `approval record`, `account commission`
and `publication reconcile` all read the publication from the ledger, show it,
and write only after an interactive confirmation.

Migration **0006** (v0.8.0) admits Meta bindings and rows without loosening the
LinkedIn rules: `account_bindings.platform` and `native_account_id` (a numeric
Page or Instagram user id, and no `author_urn` — no LinkedIn URN is ever
invented for Meta), `publications.platform` tied to its binding by a composite
foreign key, Meta locators in `remote_id` and `container_id` (a LinkedIn row
keeps `post_urn` and may hold neither), the Meta-only states `scheduled` and
`handoff`, and `intent_id` with at most one live row per intent. It adds
`linkedin_publish_commissioner`, the narrow role `publish due` connects as, and
revokes the runtime role's `consumed_at` grant on `commissioning_grants`. It is
additive for old readers (`REQUIRED_MIGRATION` stays `0002_roles`), and a
failure rolls the whole file back to 0005. Upgrades from the v0.4.2 and v0.7.0
shapes, a retry, old readers, Meta writers, role refusals and the rollback switch
are exercised in `tests/integration/test_social_migration.py`.

Migration **0007** (v0.9.0) adds `account_bindings.token_never_expires`, Meta's
explicit no-expiry evidence, with a CHECK (Meta bindings only, never beside a
measured expiry, never without its observation time), and grants the
commissioner UPDATE on exactly `remote_id` and `container_id` so a commissioned
Meta canary records its receipt. No other role changes. Upgrade from a populated
0006 ledger, a failed 0007 rolling back whole, a retry, both readers and the
canary under the real roles are in `tests/integration/test_meta_commissioning.py`.

## Releases

Consumed downstream via `[tool.uv.sources]` git+URL pins.

1. Bump `[project].version` in `pyproject.toml` on `main` — a normal reviewed PR. Relock (`uv lock`) in the same commit.
2. Actions -> **Release** -> *Run workflow* (or `gh workflow run release.yml`).

**You never type a tag.** The workflow derives it from `[project].version`, runs
every gate — default branch, main-ancestry, lint, types, tests, lockfile, build —
and creates the tag and the GitHub Release only if they all pass. Dispatching
without having bumped the version is refused before anything is built.

Consumer pins are not your job: stromy-org's `internal-lib-pins.yml` reconciles
them daily from every consumer's own `[tool.uv.sources]`.

## CI

Deliberately **self-contained** — it does not call the org's shared
`ci-python.yml` reusable workflow, because that lives in a private repo and this
one is public: the call does not resolve, and the run dies at 0s with no jobs and
only "this run likely failed because of a workflow file issue" to show for it.
See the comment at the top of `.github/workflows/ci.yml`. The cost is keeping it
in step with the shared workflow by hand; the benefit is that anyone who clones
this public repo can prove it correct from its own contents.

## Agent instructions

See `AGENTS.md` (canonical). `CLAUDE.md` and `.github/copilot-instructions.md`
are generated from it by `scripts/render-agent-md.py` — do not hand-edit them.
