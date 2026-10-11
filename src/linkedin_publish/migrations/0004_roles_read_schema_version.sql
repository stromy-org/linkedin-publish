-- linkedin_publish 0004 — both roles may read the schema version.
--
-- Every runtime and writer connection starts with `check_compatible`, which
-- reads max(version) from schema_migrations so a build never runs against a
-- schema older than it needs. 0002 granted neither role SELECT on that table,
-- so the check failed with "permission denied for table schema_migrations"
-- under any identity that was not the migrator. The tests ran as the owner and
-- never saw it; the first hosted dry tick (2026-10-11) did.
--
-- Read-only, and only this table. Grants only; idempotent and safe to re-run.

SET LOCAL search_path TO linkedin_publish;

GRANT SELECT ON schema_migrations TO linkedin_publish_runtime, linkedin_publish_writer;
