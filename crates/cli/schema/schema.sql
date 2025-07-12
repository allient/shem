CREATE SCHEMA IF NOT EXISTS public AUTHORIZATION pg_database_owner;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public VERSION '1.1';

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';

