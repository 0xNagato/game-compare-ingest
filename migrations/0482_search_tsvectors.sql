-- Add generated tsvector columns + GIN indexes for search across connected tables
-- Idempotent migration

-- Products
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='products' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.products
      ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(name,'') || ' ' || coalesce(category,''))
      ) STORED;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS products_search_tsv_idx ON public.products USING gin (search_tsv);

-- Video game titles
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.video_game_titles
      ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(normalized_title,''))
      ) STORED;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS vgt_search_tsv_idx ON public.video_game_titles USING gin (search_tsv);

DO $$ DECLARE expr text; have_dev boolean; have_genres boolean; have_synopsis boolean; have_display boolean; BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='search_tsv'
  ) THEN
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='video_games' AND column_name='developer'
    ) INTO have_dev;
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='video_games' AND column_name='genres'
    ) INTO have_genres;
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='video_games' AND column_name='synopsis'
    ) INTO have_synopsis;
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='video_games' AND column_name='display_title'
    ) INTO have_display;

    expr := 'to_tsvector(''simple'', ';
    IF have_display THEN
      expr := expr || 'coalesce(display_title,'''')';
    ELSE
      expr := expr || '''''';
    END IF;
    IF have_dev THEN
      expr := expr || ' || '' '' || coalesce(developer,'''')';
    END IF;
    IF have_genres THEN
      expr := expr || ' || '' '' || coalesce(array_to_string(genres,'' ''),'''')';
    END IF;
    IF have_synopsis THEN
      expr := expr || ' || '' '' || coalesce(synopsis,'''')';
    END IF;
    expr := expr || ')';

    EXECUTE format(
      'ALTER TABLE public.video_games ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (%s) STORED;',
      expr
    );
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS vg_search_tsv_idx ON public.video_games USING gin (search_tsv);

-- Platforms
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='platforms' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.platforms
      ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(name,'') || ' ' || coalesce(code,'') || ' ' || coalesce(family,''))
      ) STORED;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS platforms_search_tsv_idx ON public.platforms USING gin (search_tsv);

-- Retailers
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='retailers' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.retailers
      ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(name,'') || ' ' || coalesce(slug::text,''))
      ) STORED;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS retailers_search_tsv_idx ON public.retailers USING gin (search_tsv);

-- Providers
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='providers' AND column_name='search_tsv'
  ) THEN
    ALTER TABLE public.providers
      ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(name,'') || ' ' || coalesce(slug::text,'') || ' ' || coalesce(kind,''))
      ) STORED;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS providers_search_tsv_idx ON public.providers USING gin (search_tsv);
