-- 0240_products.sql (squashed)
-- Products table: keep existing columns; add name/category if missing
CREATE TABLE IF NOT EXISTS public.products (
  id         bigserial PRIMARY KEY,
  slug       citext UNIQUE,
  kind       text CHECK (kind IN ('software','hardware')),
  name       text,
  category   text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
-- Backfill name from slug if empty
UPDATE public.products SET name = COALESCE(name, slug::text) WHERE name IS NULL;

-- Search indexes (trigram). Supabase keeps pg_trgm under schema `ext`.
-- IMPORTANT: When running via tools that wrap in a transaction, avoid CONCURRENTLY.
CREATE INDEX IF NOT EXISTS products_slug_trgm_idx ON public.products USING gin ((slug::text) ext.gin_trgm_ops);
CREATE INDEX IF NOT EXISTS products_name_trgm_idx ON public.products USING gin ((name) ext.gin_trgm_ops);
