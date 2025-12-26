-- 0496_product_child_counts_and_platform_console_link.sql
-- Enforce richer product bookkeeping (software/hardware child counts) and ensure
-- platforms map 1:1 to consoles.

-- 1) Ensure hardware table uses product_id naming for clarity.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='hardware' AND column_name='platform_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='hardware' AND column_name='product_id'
  ) THEN
    ALTER TABLE public.hardware RENAME COLUMN platform_id TO product_id;
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='software' AND column_name='video_game_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='software' AND column_name='product_id'
  ) THEN
    ALTER TABLE public.software RENAME COLUMN video_game_id TO product_id;
  END IF;
END$$;

-- 2) Align game_consoles column names (product_id references hardware, new platform_id links to platforms).
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='game_consoles' AND column_name='platform_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='game_consoles' AND column_name='product_id'
  ) THEN
    ALTER TABLE public.game_consoles RENAME COLUMN platform_id TO product_id;
  END IF;
END$$;

ALTER TABLE public.game_consoles
  ADD COLUMN IF NOT EXISTS platform_id bigint;

-- No legacy data exists today, but enforce NOT NULL so future inserts must link to a platform.
ALTER TABLE public.game_consoles
  ALTER COLUMN platform_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='game_consoles_platform_id_key'
  ) THEN
    ALTER TABLE public.game_consoles
      ADD CONSTRAINT game_consoles_platform_id_key UNIQUE (platform_id);
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='game_consoles_platform_id_fkey'
  ) THEN
    ALTER TABLE public.game_consoles
      ADD CONSTRAINT game_consoles_platform_id_fkey
      FOREIGN KEY (platform_id)
      REFERENCES public.platforms(id)
      ON DELETE CASCADE;
  END IF;
END$$;

ALTER TABLE public.game_consoles
  DROP CONSTRAINT IF EXISTS game_consoles_product_id_fkey;
ALTER TABLE public.game_consoles
  ADD CONSTRAINT game_consoles_product_id_fkey
  FOREIGN KEY (product_id)
  REFERENCES public.hardware(product_id)
  ON DELETE CASCADE;

-- 3) Add child-count bookkeeping columns on products.
ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS software_children_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS hardware_children_count integer NOT NULL DEFAULT 0;

UPDATE public.products SET software_children_count = 0, hardware_children_count = 0;

UPDATE public.products p
SET software_children_count = sub.cnt
FROM (
  SELECT product_id, COUNT(*) AS cnt
  FROM public.software
  GROUP BY product_id
) sub
WHERE p.id = sub.product_id;

UPDATE public.products p
SET hardware_children_count = sub.cnt
FROM (
  SELECT product_id, COUNT(*) AS cnt
  FROM public.hardware
  GROUP BY product_id
) sub
WHERE p.id = sub.product_id;

UPDATE public.products
SET category = 'software'
WHERE software_children_count > 0;

UPDATE public.products
SET category = 'hardware'
WHERE hardware_children_count > 0;

ALTER TABLE public.products
  ALTER COLUMN category SET DEFAULT 'software';
UPDATE public.products SET category = 'software' WHERE category IS NULL;
ALTER TABLE public.products
  ALTER COLUMN category SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_category_kind_chk'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_category_kind_chk
      CHECK (category IN ('software','hardware'));
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_child_counts_nonnegative'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_child_counts_nonnegative
      CHECK (software_children_count >= 0 AND hardware_children_count >= 0);
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_child_exclusive_chk'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_child_exclusive_chk
      CHECK (software_children_count = 0 OR hardware_children_count = 0);
  END IF;
END$$;

-- 4) Helpers to keep counts in sync.
CREATE OR REPLACE FUNCTION public.refresh_product_child_counts(p_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  sw_count integer;
  hw_count integer;
  desired_category text;
BEGIN
  IF p_id IS NULL THEN
    RETURN;
  END IF;

  SELECT COUNT(*) INTO sw_count FROM public.software WHERE product_id = p_id;
  SELECT COUNT(*) INTO hw_count FROM public.hardware WHERE product_id = p_id;

  desired_category := CASE
    WHEN sw_count > 0 THEN 'software'
    WHEN hw_count > 0 THEN 'hardware'
    ELSE NULL
  END;

  UPDATE public.products
  SET software_children_count = sw_count,
      hardware_children_count = hw_count,
      category = COALESCE(desired_category, category),
      updated_at = now()
  WHERE id = p_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_products_software_counts()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM public.refresh_product_child_counts(NEW.product_id);
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM public.refresh_product_child_counts(OLD.product_id);
    RETURN OLD;
  ELSE
    IF NEW.product_id IS DISTINCT FROM OLD.product_id THEN
      PERFORM public.refresh_product_child_counts(OLD.product_id);
    END IF;
    PERFORM public.refresh_product_child_counts(NEW.product_id);
    RETURN NEW;
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_products_software_counts ON public.software;
CREATE TRIGGER trg_products_software_counts
AFTER INSERT OR UPDATE OR DELETE ON public.software
FOR EACH ROW EXECUTE FUNCTION public.tg_products_software_counts();

CREATE OR REPLACE FUNCTION public.tg_products_hardware_counts()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM public.refresh_product_child_counts(NEW.product_id);
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM public.refresh_product_child_counts(OLD.product_id);
    RETURN OLD;
  ELSE
    IF NEW.product_id IS DISTINCT FROM OLD.product_id THEN
      PERFORM public.refresh_product_child_counts(OLD.product_id);
    END IF;
    PERFORM public.refresh_product_child_counts(NEW.product_id);
    RETURN NEW;
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_products_hardware_counts ON public.hardware;
CREATE TRIGGER trg_products_hardware_counts
AFTER INSERT OR UPDATE OR DELETE ON public.hardware
FOR EACH ROW EXECUTE FUNCTION public.tg_products_hardware_counts();
