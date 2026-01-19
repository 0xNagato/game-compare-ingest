-- 0483_media_kind_title.sql
-- Introduce media_kind ('image'|'video') and human-readable title for media records
-- Idempotent and safe on large tables.

DO $$ BEGIN
  CREATE TYPE media_kind AS ENUM ('image','video');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- provider_media_links: add kind column (image|video); keep existing media_type (text) as label/deprecated
DO $$ BEGIN
  ALTER TABLE IF EXISTS public.provider_media_links
    ADD COLUMN IF NOT EXISTS kind media_kind;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Backfill provider_media_links.kind based on existing media_type/title/url if null
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_media_links')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_media_links' AND column_name='kind') THEN
    UPDATE public.provider_media_links p
       SET kind = CASE
         WHEN p.kind IS NOT NULL THEN p.kind
         WHEN lower(coalesce(p.media_type,'')) IN ('video') THEN 'video'::media_kind
         WHEN lower(coalesce(p.media_type,'')) IN ('trailer','gameplay','teaser','clip') THEN 'video'::media_kind
         WHEN lower(coalesce(p.media_type,'')) IN ('image','screenshot','artwork','cover','hero','logo','icon','poster') THEN 'image'::media_kind
         WHEN p.url ~* '\\.(mp4|webm|m3u8)$' THEN 'video'::media_kind
         ELSE 'image'::media_kind
       END
     WHERE p.kind IS NULL;
  END IF;
END $$;

-- Ensure not null + default going forward
DO $$ BEGIN
  ALTER TABLE IF EXISTS public.provider_media_links
    ALTER COLUMN kind SET DEFAULT 'image'::media_kind;
  ALTER TABLE IF EXISTS public.provider_media_links
    ALTER COLUMN kind SET NOT NULL;
EXCEPTION WHEN undefined_object THEN NULL; END $$;

-- game_media: add kind and title
DO $$ BEGIN
  ALTER TABLE IF EXISTS public.game_media
    ADD COLUMN IF NOT EXISTS kind media_kind;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE IF EXISTS public.game_media
    ADD COLUMN IF NOT EXISTS title text;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Backfill game_media.kind and title
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='game_media' AND column_name='kind')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='game_media' AND column_name='title') THEN
    UPDATE public.game_media gm
       SET kind = CASE
         WHEN gm.kind IS NOT NULL THEN gm.kind
         WHEN gm.media_type IN ('trailer','gameplay') THEN 'video'::media_kind
         ELSE 'image'::media_kind
       END,
           title = COALESCE(gm.title, gm.media_type::text)
     WHERE gm.kind IS NULL OR gm.title IS NULL;
  END IF;
END $$;

-- Enforce default + not null for kind on game_media
DO $$ BEGIN
  ALTER TABLE IF EXISTS public.game_media
    ALTER COLUMN kind SET DEFAULT 'image'::media_kind;
  ALTER TABLE IF EXISTS public.game_media
    ALTER COLUMN kind SET NOT NULL;
EXCEPTION WHEN undefined_object THEN NULL; END $$;

-- Helpful indexes
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_media_links')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_media_links' AND column_name='kind') THEN
    CREATE INDEX IF NOT EXISTS provider_media_links_kind_idx ON public.provider_media_links(kind);
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='game_media' AND column_name='kind') THEN
    CREATE INDEX IF NOT EXISTS game_media_kind_idx ON public.game_media(kind);
  END IF;
END $$;
