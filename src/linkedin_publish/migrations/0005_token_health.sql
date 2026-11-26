-- linkedin_publish 0005 — token rotation and the token observation cache.
--
-- Two gaps found while commissioning the first binding (2026-10-11):
--
--   1. Renewal cost a new binding. `credential_ref` / `credential_version` are
--      identity fields, so a re-minted token could not be recorded on the
--      existing binding. `account rotate` now moves a binding to a new version,
--      disables it, and remembers whether it was enabled (so `account reenable`
--      can restore exactly that, and nothing more, after a healthy re-measure).
--   2. A tick saw only a dead token, never an approaching expiry, and every
--      ACA execution is a fresh process, so an in-memory health cache measured
--      nothing between ticks. `token_observations` keeps the last sanitized
--      probe per (binding, credential version): dates, booleans, scope names,
--      the app id and the app-scoped member id. Never a token or a secret.
--
-- Grants: the runtime may read and refresh observations (the scheduled health
-- path probes daily), but still cannot touch `account_bindings`. The writer —
-- the operator CLI — may also delete them, which is how rotation invalidates
-- the cache.
--
-- Additive only, and idempotent. The runtime minimum (REQUIRED_MIGRATION) does
-- not move: a build that carries this file keeps running, cache reported
-- unavailable, against a ledger the operator has not migrated yet.

SET LOCAL search_path TO linkedin_publish;

ALTER TABLE account_bindings
    ADD COLUMN IF NOT EXISTS rotated_at             timestamptz,
    ADD COLUMN IF NOT EXISTS rotated_from_version   text,
    ADD COLUMN IF NOT EXISTS reenable_after_measure boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS token_observations (
    binding_id         text        NOT NULL REFERENCES account_bindings (binding_id),
    credential_version text        NOT NULL,
    observed_at        timestamptz NOT NULL,
    token_active       boolean,
    scopes_ok          boolean,
    identity_verified  boolean,
    observed_scopes    text[]      NOT NULL DEFAULT '{}',
    expires_at         timestamptz,
    app_id             text,
    member_sub         text,
    -- A short provider reason code, capped by the writer. Never a response body.
    reason             text CHECK (reason IS NULL OR length(reason) <= 200),
    PRIMARY KEY (binding_id, credential_version)
);

GRANT SELECT, INSERT, UPDATE ON token_observations TO linkedin_publish_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON token_observations TO linkedin_publish_writer;
