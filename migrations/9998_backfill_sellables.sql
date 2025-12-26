-- Backfill: ensure software rows and sellables for all video_game_titles
-- Idempotent and safe to re-run.

-- Handle historical column renames: some environments renamed software.product_id -> video_game_id.
DO $$
DECLARE
	software_col text;
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='software' AND column_name='product_id'
	) THEN
		software_col := 'product_id';
	ELSIF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='software' AND column_name='video_game_id'
	) THEN
		software_col := 'video_game_id';
	ELSE
		RAISE NOTICE 'software table missing product/product_id columns; skipping backfill';
		RETURN;
	END IF;

	EXECUTE format($fmt$
		INSERT INTO public.software (%1$I)
		SELECT DISTINCT vgt.video_game_id
		FROM public.video_game_titles vgt
		JOIN public.video_games vg ON vg.title_id = vgt.id
		JOIN public.products p ON p.id = vgt.video_game_id
		LEFT JOIN public.software s ON s.%1$I = vgt.video_game_id
		WHERE vgt.video_game_id IS NOT NULL
			AND s.%1$I IS NULL
		ON CONFLICT DO NOTHING;
	$fmt$, software_col);
END $$;

-- 2) Create a software sellable for any title that lacks one.
INSERT INTO public.sellables (kind, software_title_id)
SELECT 'software'::sellable_kind, vgt.id
FROM public.video_game_titles vgt
JOIN public.video_games vg ON vg.title_id = vgt.id
LEFT JOIN public.sellables s ON s.software_title_id = vgt.id
WHERE s.id IS NULL;
