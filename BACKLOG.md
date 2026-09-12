# Backlog — linkedin-publish

Operational backlog for this library. Cross-repo initiatives live in
`stromy-org/BACKLOG.md`; this repo's rail is tracked there as **ORG-285**
(plan: `PLAN_linkedin-presence-rail.md`, ORG-PLAN-285).

ID prefix: `LIP`.

## Queue

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
