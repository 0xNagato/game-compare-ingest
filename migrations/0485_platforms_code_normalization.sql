-- 0485_platforms_code_normalization.sql
-- Goal: Normalize specific platform codes, merge duplicates, remove placeholder 'generic'.
-- Changes:
--   * Rename / merge 'ps4' -> 'playstation-4' and 'ps5' -> 'playstation-5'.
--   * Merge 'xbox-series-x' into 'xbox-series'.
--   * Map 'generic' references to 'pc' (if present) then delete 'generic'.
--   * Create audit table recording merges.
-- Idempotent: Safe to re-run; guards ensure operations skip if already applied.

-- 0. Audit table (if not exists)
CREATE TABLE IF NOT EXISTS public.platform_merge_audit (
  audit_id      bigserial PRIMARY KEY,
  old_id        bigint NOT NULL,
  new_id        bigint NOT NULL,
  old_code      text NOT NULL,
  new_code      text NOT NULL,
  merged_rows   bigint,
  merged_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE(old_id)
);

-- Helper inline function-like DO blocks for each normalization case.

-- 1. ps4 → playstation-4 (rename if target absent; else merge)
DO $$ DECLARE src_id bigint; dest_id bigint; rows_changed bigint; BEGIN
  SELECT id INTO src_id FROM public.platforms WHERE code='ps4';
  -- Determine destination: existing target row or future rename id.
  SELECT id INTO dest_id FROM public.platforms WHERE code='playstation-4';
  IF src_id IS NULL THEN RETURN; END IF; -- nothing to do
  IF dest_id IS NULL THEN
    -- Simple rename path
    UPDATE public.platforms SET code='playstation-4', name='PlayStation 4', canonical_code='playstation4' WHERE id=src_id;
  ELSE
    -- Merge path: repoint FKs then delete source
    UPDATE public.video_games SET platform_id=dest_id WHERE platform_id=src_id;
    GET DIAGNOSTICS rows_changed = ROW_COUNT;
    INSERT INTO public.platform_merge_audit(old_id,new_id,old_code,new_code,merged_rows)
      VALUES (src_id,dest_id,'ps4','playstation-4',rows_changed)
      ON CONFLICT (old_id) DO NOTHING;
    DELETE FROM public.platforms WHERE id=src_id;
  END IF;
END $$;

-- 2. ps5 → playstation-5
DO $$ DECLARE src_id bigint; dest_id bigint; rows_changed bigint; BEGIN
  SELECT id INTO src_id FROM public.platforms WHERE code='ps5';
  SELECT id INTO dest_id FROM public.platforms WHERE code='playstation-5';
  IF src_id IS NULL THEN RETURN; END IF;
  IF dest_id IS NULL THEN
    UPDATE public.platforms SET code='playstation-5', name='PlayStation 5', canonical_code='playstation5' WHERE id=src_id;
  ELSE
    UPDATE public.video_games SET platform_id=dest_id WHERE platform_id=src_id;
    GET DIAGNOSTICS rows_changed = ROW_COUNT;
    INSERT INTO public.platform_merge_audit(old_id,new_id,old_code,new_code,merged_rows)
      VALUES (src_id,dest_id,'ps5','playstation-5',rows_changed)
      ON CONFLICT (old_id) DO NOTHING;
    DELETE FROM public.platforms WHERE id=src_id;
  END IF;
END $$;

-- 3. Merge xbox-series-x into xbox-series
DO $$ DECLARE src_id bigint; dest_id bigint; rows_changed bigint; BEGIN
  SELECT id INTO src_id FROM public.platforms WHERE code='xbox-series-x';
  SELECT id INTO dest_id FROM public.platforms WHERE code='xbox-series';
  IF src_id IS NULL OR dest_id IS NULL THEN RETURN; END IF; -- need both
  -- Only proceed if source still exists (not previously deleted)
  UPDATE public.video_games SET platform_id=dest_id WHERE platform_id=src_id;
  GET DIAGNOSTICS rows_changed = ROW_COUNT;
  INSERT INTO public.platform_merge_audit(old_id,new_id,old_code,new_code,merged_rows)
    VALUES (src_id,dest_id,'xbox-series-x','xbox-series',rows_changed)
    ON CONFLICT (old_id) DO NOTHING;
  DELETE FROM public.platforms WHERE id=src_id;
END $$;

-- 4. Remove 'generic' mapping references to 'pc'
DO $$ DECLARE gen_id bigint; pc_id bigint; rows_changed bigint; BEGIN
  SELECT id INTO gen_id FROM public.platforms WHERE code='generic';
  IF gen_id IS NULL THEN RETURN; END IF;
  SELECT id INTO pc_id FROM public.platforms WHERE code='pc';
  IF pc_id IS NULL THEN RETURN; END IF; -- cannot remap without pc
  UPDATE public.video_games SET platform_id=pc_id WHERE platform_id=gen_id;
  GET DIAGNOSTICS rows_changed = ROW_COUNT;
  INSERT INTO public.platform_merge_audit(old_id,new_id,old_code,new_code,merged_rows)
    VALUES (gen_id,pc_id,'generic','pc',rows_changed)
    ON CONFLICT (old_id) DO NOTHING;
  DELETE FROM public.platforms WHERE id=gen_id;
END $$;

-- 5. Ensure canonical_code matches updated code/name patterns after renames.
UPDATE public.platforms p SET canonical_code = lower(regexp_replace(coalesce(code,name),'[^a-z0-9]','','g'))
WHERE p.canonical_code IS NULL OR p.canonical_code <> lower(regexp_replace(coalesce(code,name),'[^a-z0-9]','','g'));

-- 6. Analyze for planner stats.
ANALYZE public.platforms;
ANALYZE public.video_games;