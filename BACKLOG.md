# Backlog — linkedin-publish

Operational backlog for this library. Cross-repo initiatives live in
`stromy-org/BACKLOG.md`; this repo's rail is tracked there as **ORG-285**
(plan: `PLAN_linkedin-presence-rail.md`, ORG-PLAN-285).

ID prefix: `LIP`.

## Queue

### LIP-001 — Execute the Postgres integration tier

**Status:** open · **Workstream:** brand-content · **Filed:** 2026-09-12

The durable tier (`tests/integration/`) was authored alongside C2 but has
**never executed** — no Postgres was reachable on the authoring machine, so all
20 tests skipped. ORG-PLAN-285 acceptance criterion 2 requires them run before
the durable guarantees may be claimed, and explicitly forbids counting a skip as
a pass. This is the single largest gap between "engineering-complete" and
"proven" in this repo.

Acceptance criteria:
- [ ] `LINKEDIN_PUBLISH_TEST_DSN` set against a disposable Postgres 16; all 20
      integration tests pass, none skipped.
- [ ] The multi-process compare-and-set test observes exactly one winner.
- [ ] The role tests connect **as `linkedin_publish_runtime`** and Postgres
      refuses approval creation, payload rewrite, binding authorship and grant
      issuance.
- [ ] The rollback test proves a refused reservation did not spend the app
      counter.
- [x] CI runs this tier on a service container, so it cannot silently lapse back
      to NOT-RUN. **Done 2026-09-13** — `.github/workflows/ci.yml` `integration`
      job, plus `tests/integration/conftest.py` failing rather than skipping when
      `CI` is set and the DSN is missing.

**Progress**
- 2026-09-13 — CI now runs the tier, so the remaining criteria are verified by
  the workflow rather than by hand. The same commit replaced the template's call
  to the org's private shared reusable workflow with an inlined one: a public
  repo cannot resolve a private workflow, and the first push failed at 0s with no
  jobs (run 34720884095).

Pointers: `tests/integration/conftest.py`, `migrations/0002_roles.sql`.

### LIP-002 — Recheck `LINKEDIN_VERSION` before any commissioning

**Status:** open · **Workstream:** brand-content · **Filed:** 2026-09-12

`version.LINKEDIN_VERSION` is `202608`, recorded from the plan at authoring time
as a *candidate*. LinkedIn retires versioned-REST versions on its own schedule
and this library deliberately invents no sunset date. An unsupported version is
a failure mode that only appears against the live API, which nothing in the
offline suite can catch.

Acceptance criteria:
- [ ] The supported-version list is read from the Developer Portal at C1
      commissioning and the constant updated in `version.py` (one place).
- [ ] The observed value is recorded in the commissioning record alongside the
      canary receipt.

## Radar

### LIP-003 — Media byte limits are conservative guesses

**Status:** watch · **Revisit-when:** the first image or document capability is
commissioned · `revisit: c1-media-commissioning`

`media.IMAGE_MAX_BYTES` (8 MiB) and `DOCUMENT_MAX_BYTES` (100 MiB) are pilot
caps, not measured provider limits; the provider's enforcement is the real
bound. They are deliberately below any published figure, so the failure mode is
a refusal we control rather than a rejection we do not. Revisit with real
evidence when media capabilities are commissioned — not before, since a guess
replaced by another guess is not progress.

### LIP-004 — Refresh-token support is not implemented and not promised

**Status:** parked · **Revisit-when:** actual refresh-token evidence exists for
the app · `revisit: refresh-token-eligibility`

ORG-PLAN-285 C4 is explicit: the baseline is manual renewal, and refresh
eligibility must not be assumed from CMA approval or a nominal 60-day lifetime.
If implemented later it needs single-flight refresh, atomic secret rotation and
independent refresh-token-expiry reminders. Parked deliberately, not forgotten.
