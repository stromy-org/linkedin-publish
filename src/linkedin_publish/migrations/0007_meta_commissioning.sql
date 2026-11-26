-- linkedin_publish 0007 — Meta token evidence; the commissioner records Meta receipts.
--
-- ORG-PLAN-652 C5a (Fleet Social F32). Two gaps left by 0006:
--
--   1. A Meta organic token is issued with a 60-day expiry or with none, so a
--      NULL `token_expires_at` cannot tell "never measured" from "measured as
--      never expiring". `token_never_expires` makes the second an explicit fact,
--      written only by `meta inspect --record` from a valid `debug_token`
--      reading that reported `expires_at: 0`. The CHECK keeps it honest: only on
--      a Meta binding, never beside a measured expiry, never without the time it
--      was observed. LinkedIn bindings cannot carry it.
--   2. The commissioner role (0006) can run every send transition but cannot
--      write the two Meta locators, so a commissioned Meta canary would lose its
--      Instagram container id and its Graph object id at the database. It gains
--      UPDATE on exactly `remote_id` and `container_id` — the same delivery
--      columns the runtime has had since 0006, and nothing else.
--
-- No role is widened beyond that: the runtime is untouched, and the writer
-- already holds table-level UPDATE on `account_bindings` (0002), which covers
-- the new column. Old readers keep working: the column has a default, and every
-- reader selects named columns or tolerates an unknown one. The runtime minimum
-- (REQUIRED_MIGRATION) does not move.
--
-- Idempotent: every statement can re-run after a partial failure (the whole file
-- runs in one transaction under the migration lock, so a failure leaves 0006).

SET LOCAL search_path TO linkedin_publish;

ALTER TABLE account_bindings
    ADD COLUMN IF NOT EXISTS token_never_expires boolean NOT NULL DEFAULT false;

ALTER TABLE account_bindings DROP CONSTRAINT IF EXISTS account_bindings_token_never_expires;
ALTER TABLE account_bindings ADD CONSTRAINT account_bindings_token_never_expires CHECK (
    NOT token_never_expires
    OR (platform <> 'linkedin' AND token_expires_at IS NULL AND token_observed_at IS NOT NULL)
);

GRANT UPDATE (remote_id, container_id) ON publications TO linkedin_publish_commissioner;
