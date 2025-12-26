-- 0011_fix_appq_purge_search_path.sql
-- Harden appq.purge against mutable search_path.
-- Source (introspected):
--   CREATE OR REPLACE FUNCTION appq.purge(queue_name text)
--   RETURNS bigint
--   LANGUAGE sql
--   AS $$ SELECT pgmq.purge_queue(queue_name => queue_name); $$

-- We preserve SECURITY INVOKER (prior behavior) and add a fixed search_path.
-- We also switch to positional parameter $1 to avoid any name resolution surprises.

DO $$ BEGIN
  PERFORM 1 FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'appq' AND p.proname = 'purge' AND pg_get_function_identity_arguments(p.oid) = 'queue_name text';
  -- Always replace to ensure search_path is enforced
  EXECUTE $$
  CREATE OR REPLACE FUNCTION appq.purge(queue_name text)
  RETURNS bigint
  LANGUAGE sql
  SECURITY INVOKER
  SET search_path = appq, pg_catalog
AS $fn$
  SELECT pgmq.purge_queue(queue_name => $1);
$fn$;
  $$;
EXCEPTION WHEN others THEN
  RAISE NOTICE 'appq.purge replacement attempted; %', SQLERRM;
END $$;
