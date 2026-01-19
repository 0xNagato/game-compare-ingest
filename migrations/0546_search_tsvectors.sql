-- Add generated tsvector columns + GIN indexes for search across connected tables
-- Idempotent migration

DO $$
DECLARE
  has_platform boolean;
  has_synopsis boolean;
  products_update_sql text;
  products_trigger_sql text;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='products' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.products ADD COLUMN search_tsv tsvector;
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='products' AND column_name='platform'
  ) INTO has_platform;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='products' AND column_name='synopsis'
  ) INTO has_synopsis;

  products_update_sql := 'UPDATE public.products SET search_tsv = to_tsvector(''simple'', concat_ws('' '', coalesce(name,'''')';
  IF has_platform THEN
    products_update_sql := products_update_sql || ', coalesce(platform,'''')';
  END IF;
  products_update_sql := products_update_sql || ', coalesce(slug,'''')';
  products_update_sql := products_update_sql || ', coalesce(category,'''')';
  IF has_synopsis THEN
    products_update_sql := products_update_sql || ', coalesce(synopsis,'''')';
  END IF;
  products_update_sql := products_update_sql || ')) WHERE search_tsv IS NULL';
  EXECUTE products_update_sql;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='products_search_tsv_trigger'
      AND tgrelid='public.products'::regclass
  ) THEN
    products_trigger_sql := 'CREATE TRIGGER products_search_tsv_trigger BEFORE INSERT OR UPDATE ON public.products FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger( search_tsv, ''pg_catalog.simple'', name';
    IF has_platform THEN
      products_trigger_sql := products_trigger_sql || ', platform';
    END IF;
    products_trigger_sql := products_trigger_sql || ', slug, category';
    IF has_synopsis THEN
      products_trigger_sql := products_trigger_sql || ', synopsis';
    END IF;
    products_trigger_sql := products_trigger_sql || ' )';
    EXECUTE products_trigger_sql;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS products_search_tsv_idx ON public.products USING gin (search_tsv);

-- Video game titles
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.video_game_titles ADD COLUMN search_tsv tsvector;
  END IF;
  UPDATE public.video_game_titles
  SET search_tsv = to_tsvector(
    'simple',
    concat_ws(' ', coalesce(title,''), coalesce(normalized_title,''))
  )
  WHERE search_tsv IS NULL;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='video_game_titles_search_tsv_trigger'
      AND tgrelid='public.video_game_titles'::regclass
  ) THEN
    CREATE TRIGGER video_game_titles_search_tsv_trigger
    BEFORE INSERT OR UPDATE ON public.video_game_titles
    FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(
      search_tsv, 'pg_catalog.simple', title, normalized_title
    );
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS vgt_search_tsv_idx ON public.video_game_titles USING gin (search_tsv);

DO $$
DECLARE
  vg_columns text[] := ARRAY['title', 'normalized_title', 'display_title', 'slug', 'developer', 'genre', 'genres', 'synopsis'];
  vg_update_parts text[] := ARRAY[]::text[];
  vg_trigger_parts text[] := ARRAY[]::text[];
  col text;
  column_exists boolean;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.video_games ADD COLUMN search_tsv tsvector;
  END IF;

  FOREACH col IN ARRAY vg_columns LOOP
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='video_games' AND column_name = col
    ) INTO column_exists;

    IF column_exists THEN
      IF col = 'genres' THEN
        vg_update_parts := array_append(vg_update_parts, format('coalesce(array_to_string(%I, '' ''), '''')', col));
        -- Skip genres for the trigger because tsvector_update_trigger expects scalar columns.
      ELSE
        vg_update_parts := array_append(vg_update_parts, format('coalesce(%I, '''')', col));
        vg_trigger_parts := array_append(vg_trigger_parts, format('%I', col));
      END IF;
    END IF;
  END LOOP;

  IF array_length(vg_update_parts, 1) IS NULL THEN
    vg_update_parts := ARRAY['''''']::text[];
  END IF;

  EXECUTE format(
    'UPDATE public.video_games SET search_tsv = to_tsvector(''simple'', concat_ws('' '', %s)) WHERE search_tsv IS NULL',
    array_to_string(vg_update_parts, ', ')
  );

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='video_games_search_tsv_trigger'
      AND tgrelid='public.video_games'::regclass
  ) THEN
    IF array_length(vg_trigger_parts, 1) IS NULL THEN
      RAISE NOTICE 'Skipping video_games_search_tsv_trigger creation because no scalar columns were found.';
    ELSE
      EXECUTE format(
        'CREATE TRIGGER video_games_search_tsv_trigger BEFORE INSERT OR UPDATE ON public.video_games FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger( search_tsv, ''pg_catalog.simple'', %s )',
        array_to_string(vg_trigger_parts, ', ')
      );
    END IF;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS vg_search_tsv_idx ON public.video_games USING gin (search_tsv);

-- Platforms
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='platforms' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.platforms ADD COLUMN search_tsv tsvector;
  END IF;
  UPDATE public.platforms
  SET search_tsv = to_tsvector(
    'simple',
    concat_ws(' ', coalesce(name,''), coalesce(code,''), coalesce(family,''))
  )
  WHERE search_tsv IS NULL;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='platforms_search_tsv_trigger'
      AND tgrelid='public.platforms'::regclass
  ) THEN
    CREATE TRIGGER platforms_search_tsv_trigger
    BEFORE INSERT OR UPDATE ON public.platforms
    FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(
      search_tsv, 'pg_catalog.simple', name, code, family
    );
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS platforms_search_tsv_idx ON public.platforms USING gin (search_tsv);

-- Retailers
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='retailers' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.retailers ADD COLUMN search_tsv tsvector;
  END IF;
  UPDATE public.retailers
  SET search_tsv = to_tsvector(
    'simple',
    concat_ws(' ', coalesce(name,''), coalesce(slug::text,''))
  )
  WHERE search_tsv IS NULL;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='retailers_search_tsv_trigger'
      AND tgrelid='public.retailers'::regclass
  ) THEN
    CREATE TRIGGER retailers_search_tsv_trigger
    BEFORE INSERT OR UPDATE ON public.retailers
    FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(
      search_tsv, 'pg_catalog.simple', name, slug
    );
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS retailers_search_tsv_idx ON public.retailers USING gin (search_tsv);

-- Providers
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='providers' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.providers ADD COLUMN search_tsv tsvector;
  END IF;
  UPDATE public.providers
  SET search_tsv = to_tsvector(
    'simple',
    concat_ws(' ', coalesce(name,''), coalesce(slug::text,''), coalesce(kind,''))
  )
  WHERE search_tsv IS NULL;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='providers_search_tsv_trigger'
      AND tgrelid='public.providers'::regclass
  ) THEN
    CREATE TRIGGER providers_search_tsv_trigger
    BEFORE INSERT OR UPDATE ON public.providers
    FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(
      search_tsv, 'pg_catalog.simple', name, slug, kind
    );
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS providers_search_tsv_idx ON public.providers USING gin (search_tsv);
