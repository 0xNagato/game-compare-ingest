-- 0482_platforms_canonicalization.sql
-- Canonicalization & dedupe support for platforms table.
-- Idempotent: safe to re-run.

-- 1. Ensure canonical_code column exists.
ALTER TABLE public.platforms ADD COLUMN IF NOT EXISTS canonical_code text;

-- 2. Backfill canonical_code for any NULL rows using deterministic normalization.
UPDATE public.platforms
SET canonical_code = lower(regexp_replace(coalesce(code, name),'[^a-z0-9]','','g'))
WHERE canonical_code IS NULL;

-- 3. (Optional) Remove obviously invalid placeholder rows (e.g. 'generic').
-- Guard with WHERE name ILIKE 'generic' to avoid accidental data loss.
DELETE FROM public.platforms WHERE name ILIKE 'generic';

-- 4. Merge duplicate canonical_code groups by choosing the smallest id as canonical.
-- We do this in plpgsql block so it can run idempotently: after first run duplicates removed.
DO $$
DECLARE
  rec RECORD;
  canonical_id BIGINT;
  dup_ids BIGINT[];
  vid BIGINT;
BEGIN
  FOR rec IN (
    SELECT canonical_code, array_agg(id ORDER BY id) AS ids, count(*) AS ct
    FROM public.platforms
    WHERE canonical_code IS NOT NULL
    GROUP BY canonical_code
    HAVING count(*) > 1
  ) LOOP
    canonical_id := rec.ids[1]; -- choose smallest id
    dup_ids := (SELECT array_agg(id) FROM unnest(rec.ids) id WHERE id <> canonical_id);
    IF dup_ids IS NULL THEN CONTINUE; END IF;
    -- Repoint video_games.platform_id
    UPDATE public.video_games SET platform_id = canonical_id WHERE platform_id = ANY(dup_ids);
    -- Delete duplicates
    DELETE FROM public.platforms WHERE id = ANY(dup_ids);
  END LOOP;
END $$;

-- 5. Create UNIQUE index on canonical_code to prevent future duplicates (only when no conflicts remain).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='platforms_canonical_code_uq'
  ) THEN
    BEGIN
      CREATE UNIQUE INDEX platforms_canonical_code_uq ON public.platforms (canonical_code);
    EXCEPTION WHEN duplicate_object THEN NULL; END;
  END IF;
END $$;

-- 6. Optional: Analyze table for planner stats post-dedupe.
ANALYZE public.platforms;
