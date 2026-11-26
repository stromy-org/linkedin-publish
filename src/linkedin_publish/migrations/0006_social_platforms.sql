-- linkedin_publish 0006 — admit Meta bindings and rows; a commissioning-send role.
--
-- ORG-PLAN-652 C5a. At 0001 the binding constraint names two LinkedIn adapters
-- and requires `author_urn`, so a platform column alone cannot admit a Meta
-- binding. This migration expands the binding and locator constraints with
-- platform-specific rules instead of loosening them:
--
--   * `account_bindings.platform` (default 'linkedin') and `native_account_id`.
--     A LinkedIn binding keeps its adapter set and its LinkedIn author URN and
--     has no native id; a Meta binding uses the `meta_graph` adapter, a numeric
--     Page or Instagram user id, and NO author URN — none is ever invented.
--   * `publications.platform`, tied to its binding's platform by a composite
--     foreign key, so a row cannot claim another platform than its binding.
--   * Meta locators live in `remote_id` (the Graph object id) and `container_id`
--     (an Instagram media container, persisted before media_publish). A LinkedIn
--     row keeps `post_urn` and may hold neither; a Meta row never holds a
--     `post_urn`.
--   * Two states for remote work, Meta rows only: `scheduled` (Meta holds a
--     native schedule) and `handoff` (a human scheduled it in Business Suite).
--   * `intent_id` (the originating C3 operation key) and `origin` (the imported
--     approval's provenance). At most one LIVE row per intent — not cancelled,
--     expired or failed — so an approval imported twice has a single owner.
--
-- Preserved: every natural key, every stored digest, commissioning grants and
-- their one-open index, the `sending`/`unknown` fencing, and every role grant
-- below. Old readers keep working: they select named columns, and every new
-- column has a default or is nullable. The runtime minimum (REQUIRED_MIGRATION)
-- does not move.
--
-- F26 (Fleet Social): `linkedin_publish_commissioner`, the narrow identity the
-- operator CLI's `publish due` connects as. It may run the send transitions,
-- consume a grant and spend quota — and nothing else: it cannot approve, issue
-- a grant, author a binding or rewrite a payload. The runtime role LOSES its
-- UPDATE on `commissioning_grants.consumed_at`, so "the scheduler cannot use a
-- grant" is a database fact, not only a constructor argument. No role is widened.
--
-- Idempotent: every statement can re-run after a partial failure (the whole file
-- runs in one transaction under the migration lock, so a failure leaves 0005).

SET LOCAL search_path TO linkedin_publish;

-- ------------------------------------------------------------- bindings ---

ALTER TABLE account_bindings
    ADD COLUMN IF NOT EXISTS platform          text NOT NULL DEFAULT 'linkedin',
    ADD COLUMN IF NOT EXISTS native_account_id text;

ALTER TABLE account_bindings ALTER COLUMN author_urn DROP NOT NULL;

-- Drop 0001's column CHECK on `adapter` whatever name the server gave it.
DO $$
DECLARE
    c record;
BEGIN
    FOR c IN
        SELECT conname
          FROM pg_constraint
         WHERE conrelid = 'linkedin_publish.account_bindings'::regclass
           AND contype = 'c'
           AND conname <> 'account_bindings_platform_identity'
           AND pg_get_constraintdef(oid) LIKE '%share_ugc%'
    LOOP
        EXECUTE format('ALTER TABLE linkedin_publish.account_bindings DROP CONSTRAINT %I', c.conname);
    END LOOP;
END
$$;

ALTER TABLE account_bindings DROP CONSTRAINT IF EXISTS account_bindings_platform_identity;
ALTER TABLE account_bindings ADD CONSTRAINT account_bindings_platform_identity CHECK (
    (platform = 'linkedin'
        AND adapter IN ('share_ugc', 'rest_posts')
        AND author_urn IS NOT NULL
        AND author_urn LIKE 'urn:li:%'
        AND native_account_id IS NULL)
    OR
    (platform IN ('facebook_page', 'instagram_professional')
        AND adapter = 'meta_graph'
        AND author_urn IS NULL
        AND native_account_id IS NOT NULL
        AND native_account_id ~ '^[0-9]{1,32}$')
);

-- The target of the publications -> bindings platform tie. Added once: the
-- foreign key below depends on it, so a re-run must not drop it.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'linkedin_publish.account_bindings'::regclass
           AND conname = 'account_bindings_id_platform'
    ) THEN
        ALTER TABLE linkedin_publish.account_bindings
            ADD CONSTRAINT account_bindings_id_platform UNIQUE (binding_id, platform);
    END IF;
END
$$;

-- --------------------------------------------------------- publications ---

ALTER TABLE publications
    ADD COLUMN IF NOT EXISTS platform     text NOT NULL DEFAULT 'linkedin',
    ADD COLUMN IF NOT EXISTS remote_id    text,
    ADD COLUMN IF NOT EXISTS container_id text,
    ADD COLUMN IF NOT EXISTS intent_id    text,
    ADD COLUMN IF NOT EXISTS origin       jsonb;

ALTER TABLE publications DROP CONSTRAINT IF EXISTS publications_binding_platform;
ALTER TABLE publications ADD CONSTRAINT publications_binding_platform
    FOREIGN KEY (binding_id, platform) REFERENCES account_bindings (binding_id, platform);

-- Replace 0001's state CHECK (named by the server) with a platform-aware one.
DO $$
DECLARE
    c record;
BEGIN
    FOR c IN
        SELECT conname
          FROM pg_constraint
         WHERE conrelid = 'linkedin_publish.publications'::regclass
           AND contype = 'c'
           AND conname NOT IN ('publications_state_by_platform', 'publications_window', 'publications_locator')
           AND pg_get_constraintdef(oid) LIKE '%claimed%'
    LOOP
        EXECUTE format('ALTER TABLE linkedin_publish.publications DROP CONSTRAINT %I', c.conname);
    END LOOP;
END
$$;

ALTER TABLE publications DROP CONSTRAINT IF EXISTS publications_state_by_platform;
ALTER TABLE publications ADD CONSTRAINT publications_state_by_platform CHECK (
    state IN ('pending', 'claimed', 'sending', 'published', 'failed', 'unknown', 'expired', 'cancelled')
    OR (platform <> 'linkedin' AND state IN ('scheduled', 'handoff'))
);

ALTER TABLE publications DROP CONSTRAINT IF EXISTS publications_locator;
ALTER TABLE publications ADD CONSTRAINT publications_locator CHECK (
    (platform = 'linkedin' AND remote_id IS NULL AND container_id IS NULL)
    OR (platform <> 'linkedin' AND post_urn IS NULL)
);

-- One approval version imported twice is one row; one intent has one live row.
CREATE UNIQUE INDEX IF NOT EXISTS publications_intent_digest
    ON publications (intent_id, payload_digest)
    WHERE intent_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS publications_intent_live
    ON publications (intent_id)
    WHERE intent_id IS NOT NULL AND state NOT IN ('cancelled', 'expired', 'failed');

-- Remote schedules are read back on every tick.
CREATE INDEX IF NOT EXISTS publications_remote_idx
    ON publications (binding_id, state)
    WHERE state IN ('scheduled', 'handoff');

-- ---------------------------------------------------------------- grants ---

-- Runtime: the two new locator columns are delivery state, like `post_urn`.
GRANT UPDATE (remote_id, container_id) ON publications TO linkedin_publish_runtime;
-- Writer: reconciling an unknown Meta send records the object it found.
GRANT UPDATE (remote_id) ON publications TO linkedin_publish_writer;

-- F26: the runtime may no longer consume a commissioning grant.
REVOKE UPDATE (consumed_at) ON commissioning_grants FROM linkedin_publish_runtime;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'linkedin_publish_commissioner') THEN
        CREATE ROLE linkedin_publish_commissioner NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA linkedin_publish TO linkedin_publish_commissioner;
GRANT SELECT ON schema_migrations, account_bindings, manifests, publications, approvals,
                commissioning_grants, token_observations TO linkedin_publish_commissioner;
GRANT UPDATE (
    state,
    not_before,
    attempt_token,
    lease_deadline,
    attempts,
    post_urn,
    permalink,
    adapter,
    published_at,
    failure_code,
    failure_detail,
    updated_at
) ON publications TO linkedin_publish_commissioner;
GRANT UPDATE (consumed_at) ON commissioning_grants TO linkedin_publish_commissioner;
GRANT SELECT, INSERT ON publication_events TO linkedin_publish_commissioner;
GRANT USAGE, SELECT ON SEQUENCE publication_events_event_id_seq TO linkedin_publish_commissioner;
GRANT SELECT, INSERT, UPDATE ON media_uploads, request_budgets TO linkedin_publish_commissioner;
