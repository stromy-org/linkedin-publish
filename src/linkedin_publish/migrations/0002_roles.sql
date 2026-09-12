-- linkedin_publish 0002 — least-privilege roles.
--
-- Two identities, and the split is the point:
--
--   linkedin_publish_runtime  the scheduled job and the MCP. Moves publications
--                             through delivery states. Cannot create an approval,
--                             author a binding, or edit an approved payload.
--   linkedin_publish_writer   the operator CLI's registration/approval identity.
--                             Mints approvals and commissioning grants.
--
-- Neither is a superuser and neither owns the schema, so a compromised runtime
-- cannot grant itself the other's rights. Column-level grants are what make
-- "the runtime cannot change approved bytes" a database fact rather than a
-- convention some future code path forgets.
--
-- Roles are created by the deployment's Postgres bootstrap (Entra-backed); this
-- migration only grants. It is idempotent and safe to re-run.

SET LOCAL search_path TO linkedin_publish;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'linkedin_publish_runtime') THEN
        CREATE ROLE linkedin_publish_runtime NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'linkedin_publish_writer') THEN
        CREATE ROLE linkedin_publish_writer NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA linkedin_publish TO linkedin_publish_runtime, linkedin_publish_writer;

-- ---------------------------------------------------------------- runtime ---
GRANT SELECT ON account_bindings, manifests, approvals, commissioning_grants TO linkedin_publish_runtime;
GRANT SELECT, INSERT ON publication_events TO linkedin_publish_runtime;
GRANT USAGE, SELECT ON SEQUENCE publication_events_event_id_seq TO linkedin_publish_runtime;
GRANT SELECT, INSERT, UPDATE ON media_uploads, request_budgets TO linkedin_publish_runtime;

GRANT SELECT ON publications TO linkedin_publish_runtime;
-- Delivery-state columns only. `payload_digest`, `draft`, `binding_id` and the
-- natural-key columns are absent on purpose: the runtime cannot rewrite what was
-- approved, or point an approved payload at another account.
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
) ON publications TO linkedin_publish_runtime;

-- A grant is consumed by the runtime at the sending transition — but only
-- consumed. It cannot issue one, so a scheduler can never manufacture the
-- licence that lets a canary out while publishing is disabled.
GRANT UPDATE (consumed_at) ON commissioning_grants TO linkedin_publish_runtime;

-- ----------------------------------------------------------------- writer ---
GRANT SELECT, INSERT, UPDATE ON account_bindings TO linkedin_publish_writer;
GRANT SELECT, INSERT ON manifests, publications, approvals, commissioning_grants TO linkedin_publish_writer;
GRANT SELECT, INSERT ON publication_events TO linkedin_publish_writer;
GRANT USAGE, SELECT ON SEQUENCE publication_events_event_id_seq TO linkedin_publish_writer;
-- Revocation is the writer's, not the runtime's.
GRANT UPDATE (revoked_at, revoked_by) ON approvals TO linkedin_publish_writer;
