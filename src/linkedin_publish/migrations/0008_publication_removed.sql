-- linkedin_publish 0008 — a published post may be recorded as removed outside the rail.
--
-- Fleet Social T29 (ORG-PLAN-285/C2). An account owner can delete a published
-- post on LinkedIn by hand; until now the ledger kept saying `published`, and
-- that receipt could still be offered as `account enable --evidence`. The
-- operator disposition `publication record-removed` moves one `published` row
-- to the terminal `removed`, keeping its receipt (`post_urn`, `permalink`,
-- `published_at`) untouched and recording who, when and why as a
-- `removed_externally` event. It never calls the provider.
--
-- The only schema change is the state CHECK: 0006's platform-aware CHECK gains
-- `removed` for every platform. It is replaced in place (the same drop-and-add
-- 0006 used); no column, index, role or grant changes. The writer already holds
-- UPDATE (state, ...) on `publications` from 0003, which is all the disposition
-- needs, and the runtime minimum (REQUIRED_MIGRATION) does not move: no
-- runtime path reads or writes `removed`. A build older than this one that
-- reads a `removed` row would reject the unknown state, so apply 0008 and
-- upgrade readers before the first disposition is recorded.
--
-- Idempotent: every statement can re-run after a partial failure (the whole file
-- runs in one transaction under the migration lock, so a failure leaves 0007).

SET LOCAL search_path TO linkedin_publish;

ALTER TABLE publications DROP CONSTRAINT IF EXISTS publications_state_by_platform;
ALTER TABLE publications ADD CONSTRAINT publications_state_by_platform CHECK (
    state IN ('pending', 'claimed', 'sending', 'published', 'failed', 'unknown', 'expired', 'cancelled', 'removed')
    OR (platform <> 'linkedin' AND state IN ('scheduled', 'handoff'))
);
