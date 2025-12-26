-- 0012_remove_prices_insert_trigger.sql
-- Purpose: Resolve partition DDL conflict during bulk price inserts.
-- The BEFORE INSERT trigger attempted to create partitions while the parent
-- table was already referenced by the multi-row INSERT, causing:
--   cannot CREATE TABLE .. PARTITION OF "prices" because it is being used by active queries in this session
-- Strategy: Drop the trigger and its function. Partition provisioning now relies on:
--   1. Pre-created horizon (0002_partitions.sql)
--   2. Explicit calls to ensure_price_partition() BEFORE insert (see bulk_insert_prices).
-- This avoids DDL inside the same statement execution path.

SET search_path TO public;

-- Drop trigger safely if it exists
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    WHERE c.relname = 'prices' AND t.tgname = 'prices_partition') THEN
      EXECUTE 'DROP TRIGGER prices_partition ON public.prices';
  END IF;
END $$;

-- Drop the old trigger function (no longer needed)
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE p.proname = 'prices_partition_insert_trigger' AND n.nspname = 'public') THEN
      EXECUTE 'DROP FUNCTION public.prices_partition_insert_trigger()';
  END IF;
END $$;

-- Verification notice: SELECT tgname FROM pg_trigger JOIN pg_class ON tgrelid=pg_class.oid WHERE relname='prices' AND NOT tgisinternal;
