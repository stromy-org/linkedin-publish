# LinkedIn Publish

Official-API LinkedIn publishing for Stromy: a small HTTP client, and a durable,
approval-gated publication ledger on top of it.

Client-neutral by construction. Nothing here reads `client-data`, resolves a
`client_slug`, parses an Entra claim, or knows what a brand is. It takes resolved
data and opaque trusted subjects; brand knowledge lives in the L3 skills and
credential resolution in the hosted MCP.

**Status: engineering-complete for C0 + C2 of ORG-PLAN-285 — 209 tests green,
including the durable tier against a real Postgres; no capability is
commissioned.** Every binding ships `publish_enabled=false` with every capability
`unknown`, so this library refuses to publish until a recorded canary receipt
says otherwise. It has never been run against LinkedIn.

## The one guarantee

Not exactly-once — that is not achievable across an external API and a database,
and is not claimed. What is guaranteed:

> **No automatic replay after an ambiguous send.**

A timeout, a crash, a 5xx or a missing receipt leaves the record `unknown`, and
nothing automated moves it from there. Not a retry, not the next tick, not a new
process, not a token rotation, not a re-import. A human reconciles it against the
actual post.

## Install

```bash
uv sync                    # client only
uv sync --extra postgres   # + the durable ledger
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
| `analytics` | Organization share statistics (CMA-gated, bounded windows) |
| `manifest` | Publish manifest v1 and the canonical digest |
| `postgres` | The durable store, budgets and migrations (`postgres` extra) |

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

Two roles, and the split is the point: `linkedin_publish_runtime` moves
publications through delivery states and cannot create an approval, author a
binding, or rewrite an approved payload; `linkedin_publish_writer` mints
approvals and commissioning grants.

## Releases

Consumed downstream via `[tool.uv.sources]` git+URL pins.

1. Bump `[project].version` in `pyproject.toml` on `main`.
2. `git tag vX.Y.Z && git push --tags`
3. CI publishes a GitHub Release; `notify-parent.yml` fires `submodule-bumped`
   into stromy-org.

Full pattern: `stromy-org/infra-docs/ai/internal-libs.md`.

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
