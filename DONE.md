# Done — linkedin-publish

Completed items, newest first. Cross-repo initiatives are recorded in
`stromy-org/DONE.md`.

## LIP-001 — Execute the Postgres integration tier

**Completed:** 2026-09-13 · **Workstream:** brand-content · **Plan:** ORG-PLAN-285

The durable tier was authored with the C2 implementation and had never executed —
no Postgres was reachable on the authoring machine, so all 20 tests skipped and
ORG-PLAN-285 acceptance criterion 2 could not be claimed.

CI now runs it against a Postgres 16 service container
(`.github/workflows/ci.yml`, `integration` job), and a skip there is a hard
failure rather than a pass: `tests/integration/conftest.py` fails instead of
skipping when `CI` is set and the DSN is missing, so the tier cannot lapse back
to NOT-RUN and still read green.

**All 20 tests pass** (run 34722764714): multi-process compare-and-set observes
exactly one winner, the refused reservation leaves the app counter unspent, and
all eight role tests connect *as* `linkedin_publish_runtime` and confirm Postgres
refuses approval creation, payload rewrite, binding re-pointing, grant issuance
and approval revocation.

**It found a real defect on its first execution.** asyncpg returns `jsonb` as a
string unless a type codec is registered, so every read from the Postgres store
handed Pydantic raw JSON text for `draft`; seven tests — every one that reads a
publication back — failed. 189 offline tests had passed against a store that
could not read its own writes. Fixed in `28d52e1`; the in-memory store
round-trips Python objects and could never have surfaced it.

- Acceptance criteria: all met.
- Commits: `39e8c78` (inline CI + run the tier), `28d52e1` (the jsonb fix).
