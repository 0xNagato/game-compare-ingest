-- 0513_create_platforms_and_hardware_minimal.sql
-- Minimal idempotent creation of platforms, hardware and game_consoles tables
-- This is intended only to satisfy environments missing these core tables so follow-up
-- migrations (platform_hardware map/backfills/views) can run. It intentionally mirrors
-- the subset of columns used by other migrations and is safe to run when the full
-- consolidated schema (0001) is not available.
CREATE TABLE
    IF NOT EXISTS public.platforms (
        id bigserial PRIMARY KEY,
        code text UNIQUE,
        name text NOT NULL UNIQUE,
        family text
    );

CREATE INDEX IF NOT EXISTS platforms_name_trgm_idx ON public.platforms USING gin ((name) gin_trgm_ops);

CREATE TABLE
    IF NOT EXISTS public.hardware (
        product_id bigint PRIMARY KEY REFERENCES public.products (id) ON DELETE CASCADE
    );

CREATE TABLE
    IF NOT EXISTS public.game_consoles (
        id bigserial PRIMARY KEY,
        product_id bigint NOT NULL REFERENCES public.hardware (product_id) ON DELETE CASCADE,
        model text NOT NULL,
        variant text,
        slug citext UNIQUE,
        release_date date,
        metadata jsonb,
        created_at timestamptz NOT NULL DEFAULT now (),
        updated_at timestamptz NOT NULL DEFAULT now ()
    );

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_consoles_product_model_variant ON public.game_consoles (product_id, model, (coalesce(variant, '')));

CREATE INDEX IF NOT EXISTS consoles_model_trgm_idx ON public.game_consoles USING gin ((model) gin_trgm_ops);