-- Backfill: ensure a hardware sellable exists for every game_consoles row
-- Idempotent: join filters prevent duplicates.

INSERT INTO public.sellables (kind, console_id)
SELECT 'hardware'::sellable_kind, gc.id
FROM public.game_consoles gc
LEFT JOIN public.sellables s ON s.console_id = gc.id
WHERE s.id IS NULL;
