-- 0492_dedupe_video_game_titles.sql
-- Purpose: Detect and remove duplicate rows in public.video_game_titles.
-- Strategy:
--  - Compute a dedupe key per row: key := COALESCE(NULLIF(normalized_title, ''), lower(title)).
--  - Scope duplicates by product: pid0 := COALESCE(product_id, 0) to treat NULL as 0.
--  - For each (pid0, key) with >1 rows, keep the smallest id, re-point dependents, delete the rest.
--  - Add partial UNIQUE indexes to prevent re-introduction of duplicates going forward.
-- Prefer IF NOT EXISTS to avoid DO...EXECUTE (safer for query-splitting runners)
CREATE UNIQUE INDEX IF NOT EXISTS uniq_vgt_pid_normtitle ON public.video_game_titles (COALESCE(product_id, 0), normalized_title)
WHERE
  normalized_title IS NOT NULL
  AND normalized_title <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uniq_vgt_pid_lowertitle_when_no_norm ON public.video_game_titles (COALESCE(product_id, 0), lower(title))
WHERE
  (
    normalized_title IS NULL
    OR normalized_title = ''
  )
  AND title IS NOT NULL
  AND title <> '';

BEGIN;

-- Build a temporary mapping table of duplicate title ids → canonical keep_id
CREATE TEMP TABLE IF NOT EXISTS tmp_vgt_dups (keep_id bigint, dup_id bigint) ON COMMIT
DROP;

TRUNCATE TABLE tmp_vgt_dups;

INSERT INTO
  tmp_vgt_dups (keep_id, dup_id)
WITH
  keys AS (
    SELECT
      id,
      COALESCE(product_id, 0) AS pid0,
      COALESCE(NULLIF(normalized_title, ''), lower(title)) AS dkey
    FROM
      public.video_game_titles
  ),
  dups AS (
    SELECT
      pid0,
      dkey,
      MIN(id) AS keep_id,
      ARRAY_AGG (
        id
        ORDER BY
          id
      ) AS all_ids
    FROM
      keys
    WHERE
      dkey IS NOT NULL
      AND dkey <> ''
    GROUP BY
      pid0,
      dkey
    HAVING
      COUNT(*) > 1
  ),
  groups AS (
    SELECT
      keep_id,
      ARRAY (
        SELECT
          x
        FROM
          unnest (all_ids) AS x
        WHERE
          x <> keep_id
      ) AS dup_ids
    FROM
      dups
  )
SELECT
  keep_id,
  unnest (dup_ids) AS dup_id
FROM
  groups;

-- Move video_games.title_id to keep_id
UPDATE public.video_games vg
SET
  title_id = d.keep_id
FROM
  tmp_vgt_dups d
WHERE
  vg.title_id = d.dup_id;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'sellables'
  ) THEN
    EXECUTE '
      UPDATE public.sellables AS s
      SET software_title_id = d.keep_id
      FROM tmp_vgt_dups AS d
      WHERE s.software_title_id = d.dup_id
    ';
  ELSE
    RAISE NOTICE 'sellables table missing; skipping software_title_id dedupe';
  END IF;
END$$;

-- Delete duplicate title rows (keep smallest id)
DELETE FROM public.video_game_titles t USING tmp_vgt_dups d
WHERE
  t.id = d.dup_id;

COMMIT;

-- B) Guards are created above with IF NOT EXISTS; nothing further required.