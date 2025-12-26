-- 0493_enable_pg_graphql.sql
-- Idempotently enable Supabase GraphQL extension
-- Requires Postgres 15+ on Supabase
create extension if not exists pg_graphql;