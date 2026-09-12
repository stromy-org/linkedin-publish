-- linkedin_publish 0001 — the publication ledger.
--
-- Owned here, never in the Stromy runtime registry shim or in workflow-runtime-core:
-- those tables answer "did this run happen", which cannot survive an accepted POST
-- followed by a crash. This schema answers "was this exact post sent, once".
--
-- Roles are split deliberately. The runtime may move a publication through its
-- delivery states; it may NOT create an approval, author a binding, or edit an
-- approved payload. That separation is exercised by the integration tests, not
-- just asserted in Python.

CREATE SCHEMA IF NOT EXISTS linkedin_publish;
SET LOCAL search_path TO linkedin_publish;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version      text PRIMARY KEY,
    checksum     text        NOT NULL,
    applied_at   timestamptz NOT NULL DEFAULT now(),
    applied_by   text        NOT NULL DEFAULT current_user
);

-- Trusted server-side configuration. Written only by the registration writer.
CREATE TABLE IF NOT EXISTS account_bindings (
    binding_id          text PRIMARY KEY,
    account_id          text        NOT NULL,
    subject_kind        text        NOT NULL,
    subject_id          text        NOT NULL,
    app_id              text        NOT NULL,
    author_urn          text        NOT NULL,
    allowed_org_urns    text[]      NOT NULL DEFAULT '{}',
    adapter             text        NOT NULL CHECK (adapter IN ('share_ugc', 'rest_posts')),
    declared_scopes     text[]      NOT NULL DEFAULT '{}',
    observed_scopes     text[]      NOT NULL DEFAULT '{}',
    -- A reference and a version. Never a credential value.
    credential_ref      text        NOT NULL,
    credential_version  text        NOT NULL,
    token_expires_at    timestamptz,
    token_observed_at   timestamptz,
    capabilities        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    publish_enabled     boolean     NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS account_bindings_subject_idx
    ON account_bindings (subject_kind, subject_id);

-- The exact normalized bytes that were imported, and their digest.
CREATE TABLE IF NOT EXISTS manifests (
    manifest_id   text PRIMARY KEY,
    campaign_id   text        NOT NULL,
    binding_id    text        NOT NULL REFERENCES account_bindings (binding_id),
    digest        text        NOT NULL,
    content       bytea       NOT NULL,
    imported_at   timestamptz NOT NULL DEFAULT now(),
    imported_by   text        NOT NULL,
    UNIQUE (campaign_id, digest)
);

CREATE TABLE IF NOT EXISTS publications (
    publication_id   text PRIMARY KEY,
    subject_kind     text        NOT NULL,
    subject_id       text        NOT NULL,
    campaign_id      text        NOT NULL,
    post_id          text        NOT NULL,
    account_id       text        NOT NULL,
    binding_id       text        NOT NULL REFERENCES account_bindings (binding_id),
    -- Immutable once written. A revised payload is a new publication_id.
    payload_digest   text        NOT NULL,
    draft            jsonb       NOT NULL,
    state            text        NOT NULL DEFAULT 'pending'
                     CHECK (state IN ('pending','claimed','sending','published',
                                      'failed','unknown','expired','cancelled')),
    scheduled_at     timestamptz NOT NULL,
    expires_at       timestamptz NOT NULL,
    not_before       timestamptz,
    attempt_token    text,
    lease_deadline   timestamptz,
    attempts         integer     NOT NULL DEFAULT 0,
    post_urn         text,
    permalink        text,
    adapter          text,
    published_at     timestamptz,
    failure_code     text,
    failure_detail   text,
    replaces         text REFERENCES publications (publication_id),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- The natural key. The same key with a different digest is a conflict that
    -- the application refuses; it is never a second post.
    CONSTRAINT publications_natural_key
        UNIQUE (subject_kind, subject_id, campaign_id, post_id, account_id),
    CONSTRAINT publications_window CHECK (expires_at > scheduled_at)
);

CREATE INDEX IF NOT EXISTS publications_due_idx
    ON publications (binding_id, state, scheduled_at)
    WHERE state = 'pending';

-- Identity tombstones outlive run-output pruning: deleting an old run folder
-- must never make an old approval publishable again.
CREATE INDEX IF NOT EXISTS publications_open_idx
    ON publications (state)
    WHERE state IN ('sending', 'unknown');

CREATE TABLE IF NOT EXISTS approvals (
    approval_id     text PRIMARY KEY,
    publication_id  text        NOT NULL REFERENCES publications (publication_id),
    payload_digest  text        NOT NULL,
    binding_id      text        NOT NULL REFERENCES account_bindings (binding_id),
    -- Server-stamped. Not a caller-supplied field, and not a manifest boolean.
    approved_by     text        NOT NULL,
    approved_at     timestamptz NOT NULL DEFAULT now(),
    revoked_at      timestamptz,
    revoked_by      text,
    UNIQUE (publication_id, payload_digest)
);

-- A one-shot licence for the commissioning canary, while publishing is off.
CREATE TABLE IF NOT EXISTS commissioning_grants (
    grant_id        text PRIMARY KEY,
    publication_id  text        NOT NULL REFERENCES publications (publication_id),
    payload_digest  text        NOT NULL,
    binding_id      text        NOT NULL REFERENCES account_bindings (binding_id),
    capability      text        NOT NULL,
    issued_by       text        NOT NULL,
    issued_at       timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    consumed_at     timestamptz,
    CONSTRAINT commissioning_grants_window CHECK (expires_at > issued_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS commissioning_grants_one_open
    ON commissioning_grants (publication_id)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS publication_events (
    event_id        bigserial PRIMARY KEY,
    publication_id  text        NOT NULL REFERENCES publications (publication_id),
    at              timestamptz NOT NULL DEFAULT now(),
    kind            text        NOT NULL,
    actor           text        NOT NULL,
    detail          jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS publication_events_pub_idx
    ON publication_events (publication_id, at);

-- An upload belongs to one binding, app, author and adapter. It is never
-- reusable across any of them: the URN spaces differ and so does the ownership.
CREATE TABLE IF NOT EXISTS media_uploads (
    upload_id    text PRIMARY KEY,
    binding_id   text        NOT NULL REFERENCES account_bindings (binding_id),
    app_id       text        NOT NULL,
    author_urn   text        NOT NULL,
    adapter      text        NOT NULL,
    sha256       text        NOT NULL,
    media_urn    text        NOT NULL,
    media_type   text        NOT NULL,
    size_bytes   bigint      NOT NULL,
    status       text        NOT NULL DEFAULT 'ready',
    expires_at   timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (binding_id, app_id, author_urn, adapter, sha256)
);

-- Shared by every replica and by both the workflow and the MCP. A per-process
-- counter would grant each replica a full quota.
CREATE TABLE IF NOT EXISTS request_budgets (
    scope       text        NOT NULL,
    identity    text        NOT NULL,
    endpoint    text        NOT NULL DEFAULT '',
    day         date        NOT NULL,
    used        integer     NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, identity, endpoint, day)
);
