
-- Purpose:
--   Materialize the set of child video_games as a JSONB array for fast "one-row" title queries
--   and keep legacy single FK (video_game_id) aligned when present.
--   Also capture all linked video_game_sources ids for quick price/media lookups.
--   Enforce provider-level uniqueness and prepare for aliasing / fuzzy search.
--
-- Notes:
--   - video_games already has title_id, so video_game_ids is denormalization for convenience where present.
--   - Triggers keep video_game_titles.video_game_ids in sync with video_games when title_id exists.
--   - All DDL is idempotent and safe to re-run.

-- ----------------------------
-- 1) video_game_titles.video_game_ids / source_ids (materialized child ids)
-- ----------------------------
-- Ensure pg_trgm exists for later indexes
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema='public' AND table_name='video_game_titles'
	) THEN
		ALTER TABLE public.video_game_titles
			ADD COLUMN IF NOT EXISTS video_game_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

		ALTER TABLE public.video_game_titles
			ADD COLUMN IF NOT EXISTS source_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

		ALTER TABLE public.video_game_titles
			ADD COLUMN IF NOT EXISTS aliases jsonb NOT NULL DEFAULT '[]'::jsonb;

		IF NOT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname='video_game_titles_video_game_ids_is_array'
				AND conrelid='public.video_game_titles'::regclass
		) THEN
			ALTER TABLE public.video_game_titles
				ADD CONSTRAINT video_game_titles_video_game_ids_is_array
				CHECK (jsonb_typeof(video_game_ids) = 'array');
		END IF;

		IF NOT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname='video_game_titles_source_ids_is_array'
				AND conrelid='public.video_game_titles'::regclass
		) THEN
			ALTER TABLE public.video_game_titles
				ADD CONSTRAINT video_game_titles_source_ids_is_array
				CHECK (jsonb_typeof(source_ids) = 'array');
		END IF;

		IF NOT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname='video_game_titles_aliases_is_array'
				AND conrelid='public.video_game_titles'::regclass
		) THEN
			ALTER TABLE public.video_game_titles
				ADD CONSTRAINT video_game_titles_aliases_is_array
				CHECK (jsonb_typeof(aliases) = 'array');
		END IF;
	END IF;
END $$;

-- Helper: rebuild the arrays for a single title id.
-- Also keeps legacy video_game_titles.video_game_id (single) aligned when present.
CREATE OR REPLACE FUNCTION public.video_game_titles_rebuild_video_game_ids(p_title_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
	merged jsonb;
	sources jsonb;
	has_video_game_id boolean;
	video_games_has_title_id boolean;
	has_updated_at boolean;
	has_aliases boolean;
BEGIN
	IF p_title_id IS NULL THEN
		RETURN;
	END IF;

	IF NOT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema='public' AND table_name='video_game_titles'
	) THEN
		RETURN;
	END IF;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema='public' AND table_name='video_games'
	) INTO video_games_has_title_id;

	-- Determine column support
	IF video_games_has_title_id THEN
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
		) INTO video_games_has_title_id;
	END IF;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='video_game_id'
	) INTO has_video_game_id;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='updated_at'
	) INTO has_updated_at;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='aliases'
	) INTO has_aliases;

	IF video_games_has_title_id THEN
		SELECT COALESCE(
			jsonb_agg(DISTINCT vg.id ORDER BY vg.id),
			'[]'::jsonb
		)
		INTO merged
		FROM public.video_games vg
		WHERE vg.title_id = p_title_id;

		-- Collect all linked video_game_source ids via video_games → video_game_sources
		IF EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema='public' AND table_name='video_game_sources'
		) THEN
			SELECT COALESCE(
				jsonb_agg(DISTINCT vgs.id ORDER BY vgs.id),
				'[]'::jsonb
			)
			INTO sources
			FROM public.video_games vg
			JOIN public.video_game_sources vgs ON vgs.video_game_id = vg.id
			WHERE vg.title_id = p_title_id;
		ELSE
			sources := '[]'::jsonb;
		END IF;
	ELSIF has_video_game_id THEN
		-- Legacy schema without video_games.title_id: fall back to the single FK on titles
		SELECT CASE
			WHEN vgt.video_game_id IS NULL THEN '[]'::jsonb
			ELSE jsonb_build_array(vgt.video_game_id)
		END
		INTO merged
		FROM public.video_game_titles vgt
		WHERE vgt.id = p_title_id;
		sources := '[]'::jsonb;
	ELSE
		merged := '[]'::jsonb;
		sources := '[]'::jsonb;
	END IF;

	UPDATE public.video_game_titles
	SET video_game_ids = merged,
			source_ids = COALESCE(sources, '[]'::jsonb)
			|| '[]'::jsonb
	WHERE id = p_title_id;

	IF has_updated_at THEN
		UPDATE public.video_game_titles
		SET updated_at = now()
		WHERE id = p_title_id;
	END IF;

	-- initialize aliases to empty array if present and null
	IF has_aliases THEN
		UPDATE public.video_game_titles
		SET aliases = '[]'::jsonb
		WHERE id = p_title_id AND aliases IS NULL;
	END IF;

	IF has_video_game_id THEN
		-- Pick the first element as the legacy single FK when available.
		UPDATE public.video_game_titles
		SET video_game_id = CASE
			WHEN jsonb_array_length(merged) > 0 THEN (merged ->> 0)::bigint
			ELSE NULL
		END
		WHERE id = p_title_id;
	END IF;
END;
$$;

-- Trigger: whenever video_games changes, rebuild its parent title's video_game_ids.
CREATE OR REPLACE FUNCTION public.video_games_sync_title_video_game_ids()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	IF TG_OP = 'INSERT' THEN
		PERFORM public.video_game_titles_rebuild_video_game_ids(NEW.title_id);
		RETURN NEW;
	ELSIF TG_OP = 'DELETE' THEN
		PERFORM public.video_game_titles_rebuild_video_game_ids(OLD.title_id);
		RETURN OLD;
	ELSE
		IF OLD.title_id IS DISTINCT FROM NEW.title_id THEN
			PERFORM public.video_game_titles_rebuild_video_game_ids(OLD.title_id);
		END IF;
		PERFORM public.video_game_titles_rebuild_video_game_ids(NEW.title_id);
		RETURN NEW;
	END IF;
END;
$$;

DO $$
DECLARE
	has_title_id boolean;
BEGIN
	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
	) INTO has_title_id;

	IF has_title_id THEN
		IF NOT EXISTS (
			SELECT 1 FROM pg_trigger
			WHERE tgname = 'video_games_sync_title_video_game_ids_trg'
		) THEN
			CREATE TRIGGER video_games_sync_title_video_game_ids_trg
			AFTER INSERT OR UPDATE OR DELETE ON public.video_games
			FOR EACH ROW
			EXECUTE FUNCTION public.video_games_sync_title_video_game_ids();
		END IF;
	END IF;
END $$;

-- Trigger: whenever video_game_sources changes, rebuild arrays for the linked title.
CREATE OR REPLACE FUNCTION public.video_game_sources_sync_title_arrays()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
	old_title_id bigint;
	new_title_id bigint;
BEGIN
	-- Resolve title_ids from video_game_id (if present)
	IF TG_OP IN ('UPDATE','DELETE') AND OLD.video_game_id IS NOT NULL THEN
		SELECT title_id INTO old_title_id FROM public.video_games WHERE id = OLD.video_game_id;
	END IF;
	IF TG_OP IN ('UPDATE','INSERT') AND NEW.video_game_id IS NOT NULL THEN
		SELECT title_id INTO new_title_id FROM public.video_games WHERE id = NEW.video_game_id;
	END IF;

	IF old_title_id IS NOT NULL THEN
		PERFORM public.video_game_titles_rebuild_video_game_ids(old_title_id);
	END IF;
	IF new_title_id IS NOT NULL AND new_title_id IS DISTINCT FROM old_title_id THEN
		PERFORM public.video_game_titles_rebuild_video_game_ids(new_title_id);
	END IF;
	RETURN COALESCE(NEW, OLD);
END;
$$;

DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema='public' AND table_name='video_game_sources'
	) THEN
		IF NOT EXISTS (
			SELECT 1 FROM pg_trigger
			WHERE tgname = 'video_game_sources_sync_title_arrays_trg'
		) THEN
			CREATE TRIGGER video_game_sources_sync_title_arrays_trg
			AFTER INSERT OR UPDATE OR DELETE ON public.video_game_sources
			FOR EACH ROW
			EXECUTE FUNCTION public.video_game_sources_sync_title_arrays();
		END IF;
	END IF;
END $$;

-- Unique index to prevent duplicate titles per provider payload/source item
DO $$
DECLARE
	has_table boolean;
	has_vg_source_id boolean;
	has_provider_item_id boolean;
	has_vg_source_item_id boolean;
	idx_sql text;
BEGIN
	SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema='public' AND table_name='video_game_titles'
	) INTO has_table;

	IF NOT has_table THEN
		RETURN;
	END IF;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='video_game_source_id'
	) INTO has_vg_source_id;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='provider_item_id'
	) INTO has_provider_item_id;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='video_game_source_item_id'
	) INTO has_vg_source_item_id;

	IF NOT EXISTS (
		SELECT 1 FROM pg_indexes
		WHERE schemaname='public'
			AND indexname='video_game_titles_source_item_uq'
	) THEN
		IF has_vg_source_id AND has_vg_source_item_id THEN
			idx_sql := 'CREATE UNIQUE INDEX video_game_titles_source_item_uq
				ON public.video_game_titles (video_game_source_id, video_game_source_item_id)
				WHERE video_game_source_item_id IS NOT NULL';
		ELSIF has_vg_source_id AND has_provider_item_id THEN
			idx_sql := 'CREATE UNIQUE INDEX video_game_titles_source_item_uq
				ON public.video_game_titles (video_game_source_id, provider_item_id)
				WHERE provider_item_id IS NOT NULL';
		ELSE
			RAISE NOTICE 'Skipping creation of video_game_titles_source_item_uq; required columns missing (video_game_source_id + provider/provider_item columns).';
		END IF;

		IF idx_sql IS NOT NULL THEN
			EXECUTE idx_sql;
		END IF;
	END IF;
END $$;

-- Text search accelerators on title/normalized_title using pg_trgm
DO $$
DECLARE
	has_slug boolean;
	has_name boolean;
	has_title boolean;
	has_normalized_title boolean;
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema='public' AND table_name='video_game_titles'
	) THEN
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='slug'
		) INTO has_slug;
		
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='name'
		) INTO has_name;
		
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='title'
		) INTO has_title;
		
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='normalized_title'
		) INTO has_normalized_title;
		
		-- Index slug if it exists and isn't already indexed
		IF has_slug THEN
			IF NOT EXISTS (
				SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='video_game_titles_slug_trgm_idx'
			) THEN
				EXECUTE 'CREATE INDEX video_game_titles_slug_trgm_idx ON public.video_game_titles USING gin (slug gin_trgm_ops)';
			END IF;
		END IF;
		
		-- Index name if it exists and isn't already indexed
		IF has_name THEN
			IF NOT EXISTS (
				SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='video_game_titles_name_trgm_idx'
			) THEN
				EXECUTE 'CREATE INDEX video_game_titles_name_trgm_idx ON public.video_game_titles USING gin (name gin_trgm_ops)';
			END IF;
		END IF;
		
		-- Index title if it exists and isn't already indexed
		IF has_title THEN
			IF NOT EXISTS (
				SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='video_game_titles_title_trgm_idx'
			) THEN
				EXECUTE 'CREATE INDEX video_game_titles_title_trgm_idx ON public.video_game_titles USING gin (title gin_trgm_ops)';
			END IF;
		END IF;
		
		-- Index normalized_title if it exists and isn't already indexed
		IF has_normalized_title THEN
			IF NOT EXISTS (
				SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='video_game_titles_normalized_title_trgm_idx'
			) THEN
				EXECUTE 'CREATE INDEX video_game_titles_normalized_title_trgm_idx ON public.video_game_titles USING gin (normalized_title gin_trgm_ops)';
			END IF;
		END IF;
	END IF;
END $$;

-- Backfill materialized arrays for existing titles.
DO $$
DECLARE
	has_title_id boolean;
	has_video_game_id boolean;
	has_aliases boolean;
BEGIN
	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
	) INTO has_title_id;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='video_game_id'
	) INTO has_video_game_id;

	SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='aliases'
	) INTO has_aliases;

IF EXISTS (
	SELECT 1 FROM information_schema.tables
	WHERE table_schema='public' AND table_name='video_game_titles'
) THEN
	IF has_title_id THEN
		UPDATE public.video_game_titles vgt
		SET video_game_ids = COALESCE(s.ids, '[]'::jsonb)
		FROM (
			SELECT vg.title_id,
					 COALESCE(jsonb_agg(DISTINCT vg.id ORDER BY vg.id), '[]'::jsonb) AS ids
			FROM public.video_games vg
			GROUP BY vg.title_id
		) s
		WHERE vgt.id = s.title_id;

		-- Backfill source_ids via video_games → video_game_sources
		IF EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema='public' AND table_name='video_game_sources'
		) THEN
			UPDATE public.video_game_titles vgt
			SET source_ids = COALESCE(s.ids, '[]'::jsonb)
			FROM (
				SELECT vg.title_id,
						 COALESCE(jsonb_agg(DISTINCT vgs.id ORDER BY vgs.id), '[]'::jsonb) AS ids
				FROM public.video_games vg
				JOIN public.video_game_sources vgs ON vgs.video_game_id = vg.id
				GROUP BY vg.title_id
			) s
			WHERE vgt.id = s.title_id;
		ELSE
			UPDATE public.video_game_titles vgt SET source_ids = '[]'::jsonb;
		END IF;
	ELSIF has_video_game_id THEN
		UPDATE public.video_game_titles vgt
		SET video_game_ids = CASE
			WHEN vgt.video_game_id IS NULL THEN '[]'::jsonb
			ELSE jsonb_build_array(vgt.video_game_id)
		END,
			source_ids = '[]'::jsonb;
	END IF;

	IF has_aliases THEN
		UPDATE public.video_game_titles
		SET aliases = '[]'::jsonb
		WHERE aliases IS NULL;
	END IF;
END IF;
END $$;
