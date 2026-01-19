-- 0511_platform_hardware_view.sql
-- Create a convenient view that exposes platform -> hardware mapping
-- Only create if platform_hardware_map table exists
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='platform_hardware_map') THEN
    CREATE OR REPLACE VIEW public.platform_hardware_view AS
    SELECT
        p.id                    as platform_id,
        p.code                  as platform_code,
        p.name                  as platform_name,
        phm.hardware_product_id as hardware_product_id,
        phm.created_at          as mapped_at
    FROM
        public.platforms p
        LEFT JOIN public.platform_hardware_map phm ON phm.platform_id=p.id;

    -- Grant select to public (optional; left permissive for read-only APIs)
    EXECUTE 'GRANT SELECT ON public.platform_hardware_view TO public';
  END IF;
END $$;