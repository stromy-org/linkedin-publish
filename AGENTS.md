# AGENTS.md

Self-contained instructions for Codex and other AI agents working on linkedin-publish.

> **AGENTS.md is the canonical instruction file** for this repo (cross-vendor standard).
> `CLAUDE.md` and `.github/copilot-instructions.md` are generated from this file by
> `scripts/render-agent-md.py`. Gemini CLI reads this file directly via
> `context.fileName: ["AGENTS.md"]` in `.gemini/settings.json`. **Do not hand-edit
> the generated files.**

## Project Overview

Official-API LinkedIn publishing for Stromy: a small HTTP client, and a durable,
approval-gated publication ledger on top of it. Built for ORG-PLAN-285 (the
LinkedIn presence & publishing rail).

**Client-neutral, by construction and by rule.** This library never reads
`client-data`, resolves a `client_slug`, parses an Entra claim, or imports
anything that knows what a brand is. It takes resolved data and opaque trusted
subjects. Brand knowledge belongs in the L3 skills, client context in the plugin
layer, and credential resolution in the hosted MCP (`linkedin-mcp`). A PR that
adds a `client_slug` argument to any signature here is in the wrong repo.

**Nothing is commissioned.** Every binding defaults to `publish_enabled=false`
with every capability `unknown`. The library has never been run against LinkedIn.
Capabilities are enabled from recorded canary receipts (plan C1), not from code.

## Commands

```bash
uv sync
uv sync --extra all              # All optional extras
uv run pytest -v
uv run ruff check src/
uv run pyright src/linkedin_publish/
uv run linkedin-publish --help
```


## Architecture

Two layers. The transport is usable alone — no database — which is what makes it
testable against wire fixtures with no credential. Every *hosted* publishing path
nonetheless goes through the service, because only the durable ledger can promise
a post is not sent twice.

```
models · urns · errors          the contracts and the failure taxonomy
auth · media · limits           credentials, bytes, budgets
adapters/{ugc,rest}             two complete, independent transports
client                          adapter selection, capability checks, quota
store · service                 the ledger and its state machine
postgres · migrations/          the durable implementation (`postgres` extra)
manifest                        publish manifest v1 + the canonical digest
```

Dependency direction is strictly downward in that list. `adapters/` never imports
`service`; `store` never imports `client`.

### Invariants — do not relax without re-reading ORG-PLAN-285

1. **The guarantee is "no automatic replay after an ambiguous send"**, not
   exactly-once. Exactly-once across an external API and a database is not
   achievable and must never be claimed in code, docs or a commit message.
2. **`sending` is committed to the database before the HTTP request leaves.** The
   reverse ordering produces duplicate posts. A crash after that point leaves
   `unknown`, and recovery marks it so — never `pending`.
3. **A dead `claimed` lease returns to `pending`; a dead `sending` lease becomes
   `unknown`.** These are not symmetric and must never be collapsed.
4. **The adapter is selected from the trusted binding**, before validation or
   upload. No runtime fallback on 401/403/404/5xx, ever.
5. **A capability is enabled by recorded commissioning evidence of its own
   shape.** A text canary enables `text` and nothing else.
6. **Approval is over a payload digest.** Any change to content, destination,
   schedule, visibility or asset invalidates it.
7. **No upstream body reaches a caller, a log or an export.** LinkedIn echoes
   rejected content; only a sanitized status, provider code and request id
   survive `errors.sanitize_detail`.
8. **A SHA-256 handle is an identifier, not an authorization.** Asset reads pass
   the subject to the injected reader, which decides.
9. **The runtime database role cannot create an approval or rewrite an approved
   payload.** That is enforced by column-level GRANTs in `migrations/0002`, and
   tested by connecting *as that role* — not by a Python guard.
10. **No database transaction spans a LinkedIn request.**

## Public API

Exported from `linkedin_publish/__init__.py`; see `__all__` there. The two entry
points are `LinkedInClient` (transport) and `PublicationService` (durable).

## Development Patterns

- ruff: line-length 120, rules `ASYNC, B, PERF, S, E, F, W, I`
- pyright: strict mode
- Optional dependencies guarded with `try/except` + `DependencyError` (see `exceptions.py`)
- All commits via `/conventional-commit` skill (machine-wide global skill install)

## Testing

```
tests/
  conftest.py
  unit/        # fast, isolated
  contract/    # API stability
  integration/ # end-to-end, may need env vars
```

Markers: `@pytest.mark.unit`, `@pytest.mark.contract`, `@pytest.mark.integration`.

`unit` and `contract` are fully offline: no credential, no database, no network.
The only thing ever faked is HTTP, via a recording `httpx.MockTransport` — so a
test can assert on the exact request that *would* have gone out, and count how
many did. Several tests are counting tests on purpose: "it refused" and "it
refused after posting" look identical from a return value.

`integration` needs a **real** Postgres and skips without
`LINKEDIN_PUBLISH_TEST_DSN`. **A skip is a NOT-RUN, not a pass** — it covers
multi-process compare-and-set, transactional rollback of partial quota
reservations, and role permissions, none of which an in-memory store can prove.

**Never write a test that performs a real LinkedIn request.** CI makes zero.


## Agent-md rendering

`AGENTS.md` is the only authored agent-instruction file. Regenerate the rest:

```bash
python3 scripts/render-agent-md.py            # CLAUDE.md + .github/copilot-instructions.md
python3 scripts/render-agent-md.py --check    # exit 1 if stale
```

**Never hand-edit** `CLAUDE.md` or `.github/copilot-instructions.md` — they carry a "GENERATED FILE" banner; edits are wiped on next render.

## Commit Standards

- Conventional Commits with gitmoji
- Every commit via the `conventional-commit` skill (machine-wide)
- Co-Authored-By trailer on AI-assisted commits

## Skill Workflow

- **Commits**: `/conventional-commit`
- **Library maintenance**: `/python-library-maintain` (in-satellite — bump version, tag release, refresh AGENTS, sync optional extras)
- **New skills (rare for libs)**: `/skill-creator`
