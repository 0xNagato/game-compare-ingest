-- Add missing provider_id column so provider_media_links rows can be counted per provider.
-- This is required because ensure_provider_media_links_with_meta implicitly ties each row
-- back to the originating provider through provider_items.

ALTER TABLE IF EXISTS public.provider_media_links
  ADD COLUMN IF NOT EXISTS provider_id bigint;

DO $$
DECLARE
  col_missing boolean;
BEGIN
  SELECT NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'provider_media_links'
      AND column_name = 'provider_id'
  ) INTO col_missing;

  -- If the column already existed, nothing else to do.
  IF col_missing THEN
    UPDATE public.provider_media_links pml
    SET provider_id = pi.provider_id
    FROM public.provider_items pi
    WHERE pml.provider_item_id = pi.id
      AND pml.provider_id IS DISTINCT FROM pi.provider_id;
  ELSE
    -- Column exists but may have nulls from legacy rows; fill them.
    UPDATE public.provider_media_links pml
    SET provider_id = pi.provider_id
    FROM public.provider_items pi
    WHERE pml.provider_id IS NULL
      AND pml.provider_item_id = pi.id;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_schema='public'
      AND table_name='provider_media_links'
      AND constraint_name='provider_media_links_provider_fk'
  ) THEN
    ALTER TABLE IF EXISTS public.provider_media_links
      ADD CONSTRAINT provider_media_links_provider_fk
        FOREIGN KEY (provider_id)
        REFERENCES public.providers(id)
        ON DELETE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS provider_media_links_provider_idx
  ON public.provider_media_links(provider_id);