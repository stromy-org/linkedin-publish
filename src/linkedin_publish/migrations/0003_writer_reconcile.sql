-- linkedin_publish 0003 — the operator may record the outcome of an unknown send.
--
-- `publication reconcile` runs as the writer (operator) identity. 0002 gave the
-- writer no UPDATE on publications at all, so the one way out of `unknown` —
-- the operator's evidence of what LinkedIn actually shows — could not be
-- written by anyone but the runtime, which must never decide it.
--
-- Outcome columns only. `payload_digest`, `draft`, `binding_id`, the natural
-- key and every lease/attempt column stay out of reach: reconciling records a
-- result, it cannot change what was approved or re-arm a send.
--
-- Grants only; idempotent and safe to re-run.

SET LOCAL search_path TO linkedin_publish;

GRANT UPDATE (
    state,
    post_urn,
    permalink,
    published_at,
    failure_code,
    failure_detail,
    updated_at
) ON publications TO linkedin_publish_writer;
