-- Local-only runtime login for the wallet service (docker-compose and
-- testcontainers). It runs once, as the superuser, on an empty data directory,
-- before any migration. The service connects as wallet_service; migrations
-- run as the owner (POSTGRES_USER). Privileges belong to the NOLOGIN group
-- wallet_app, which the initial migration creates if needed and grants.
-- Use a secret-managed password outside local environments.
DO $$
BEGIN
    CREATE ROLE wallet_app NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END;
$$;

CREATE ROLE wallet_service LOGIN PASSWORD 'wallet_service' NOSUPERUSER NOCREATEDB NOCREATEROLE IN ROLE wallet_app;
