-- 0320_platforms.sql (squashed)
-- Platforms master table with search index
CREATE TABLE IF NOT EXISTS public.platforms (
  id   bigserial PRIMARY KEY,
  name text NOT NULL UNIQUE,
  slug citext UNIQUE
);

-- Trigram search index on platform name
CREATE INDEX IF NOT EXISTS platforms_name_trgm_idx ON public.platforms USING gin ((name) ext.gin_trgm_ops);
