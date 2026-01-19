-- 0477_concurrent_ingest_migration.sql
-- Concurrent ingestion helpers: lock columns, partial indexes, and claim/finalize functions.
-- Idempotent and compatible with runners that split CREATE INDEX statements.

-- 1) Add lock/support columns (idempotent)
ALTER TABLE IF EXISTS public.provider_items
  ADD COLUMN IF NOT EXISTS locked_by text,
  ADD COLUMN IF NOT EXISTS locked_at timestamptz;

ALTER TABLE IF EXISTS public.provider_offers
  ADD COLUMN IF NOT EXISTS locked_by text,
  ADD COLUMN IF NOT EXISTS locked_at timestamptz;

-- Ensure updated_at exists where helper functions expect it
ALTER TABLE IF EXISTS public.providers
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

ALTER TABLE IF EXISTS public.provider_items
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

ALTER TABLE IF EXISTS public.retailer_providers
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

-- 2) Partial indexes to make SKIP LOCKED scans efficient (use CONCURRENTLY; runner handles CIC out of txn)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='last_seen_at')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_by') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_items_needs_process
      ON public.provider_items (provider_id, id)
      WHERE (attributes IS NULL OR last_seen_at IS NULL) AND locked_by IS NULL;
  END IF;
END $$;

-- Note: provider_offers does not have provider_id; use provider_item_id to aid lookups
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_offers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_offers' AND column_name='is_active')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_offers' AND column_name='locked_by') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_offers_needs_process
      ON public.provider_offers (provider_item_id, id)
      WHERE (is_active IS NULL) AND locked_by IS NULL;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_at') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_items_locked_at
      ON public.provider_items (locked_at)
      WHERE locked_at IS NOT NULL;
  END IF;
END $$;

-- 3) Helper: claim a batch (two-phase pattern)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='last_seen_at')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_by')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_at') THEN
    CREATE OR REPLACE FUNCTION public.claim_provider_items_batch(
      p_worker_id text,
      p_batch_size int
    ) RETURNS TABLE(id bigint, provider_id bigint) LANGUAGE plpgsql SECURITY DEFINER AS $FUNC$
    BEGIN
      RETURN QUERY
      WITH cte AS (
        SELECT id, provider_id
        FROM public.provider_items
        WHERE (attributes IS NULL OR last_seen_at IS NULL)
          AND locked_by IS NULL
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
      )
      UPDATE public.provider_items pi
      SET locked_by = p_worker_id, locked_at = now()
      FROM cte
      WHERE pi.id = cte.id
      RETURNING pi.id, pi.provider_id;
    END;
    $FUNC$;

    REVOKE EXECUTE ON FUNCTION public.claim_provider_items_batch(text, int) FROM PUBLIC;
  END IF;
END $$;

-- 4) Helper: finalize after successful processing
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='last_seen_at')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_by')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_at')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='updated_at') THEN
    CREATE OR REPLACE FUNCTION public.finalize_provider_items(
      p_ids bigint[],
      p_attributes jsonb
    ) RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $FUNC$
    BEGIN
      UPDATE public.provider_items
      SET attributes = COALESCE(p_attributes, attributes),
          last_seen_at = now(),
          locked_by = NULL,
          locked_at = NULL,
          updated_at = now()
      WHERE id = ANY(p_ids);
    END;
    $FUNC$;

    REVOKE EXECUTE ON FUNCTION public.finalize_provider_items(bigint[], jsonb) FROM PUBLIC;
  END IF;
END $$;

-- 5) Atomic single-statement claim+update (when work is quick/idempotent)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='last_seen_at')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_by')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='updated_at') THEN
    CREATE OR REPLACE FUNCTION public.atomic_process_provider_items(
      p_batch_size int
    ) RETURNS TABLE(id bigint, provider_id bigint) LANGUAGE sql AS $FUNC$
      WITH cte AS (
        SELECT id, provider_id
        FROM public.provider_items
        WHERE (attributes IS NULL OR last_seen_at IS NULL)
          AND locked_by IS NULL
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
      )
      UPDATE public.provider_items pi
      SET attributes = coalesce(pi.attributes, '{}'::jsonb),
          last_seen_at = now(),
          updated_at = now()
      FROM cte
      WHERE pi.id = cte.id
      RETURNING pi.id, pi.provider_id;
    $FUNC$;
  END IF;
END $$;

-- 6) Recovery: reset stale locks older than a threshold
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_by')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_at') THEN
    CREATE OR REPLACE FUNCTION public.recover_stale_provider_item_locks(
      p_threshold_interval interval DEFAULT '30 minutes'
    ) RETURNS int LANGUAGE plpgsql SECURITY DEFINER AS $FUNC$
    DECLARE
      v_count int;
    BEGIN
      UPDATE public.provider_items
      SET locked_by = NULL, locked_at = NULL
      WHERE locked_at IS NOT NULL AND locked_at < now() - p_threshold_interval;
      GET DIAGNOSTICS v_count = ROW_COUNT;
      RETURN v_count;
    END;
    $FUNC$;

    REVOKE EXECUTE ON FUNCTION public.recover_stale_provider_item_locks(interval) FROM PUBLIC;
  END IF;
END $$;

-- 7) Example backfills as reusable helpers
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='is_active')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='auth_metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='updated_at') THEN
    CREATE OR REPLACE FUNCTION public.backfill_providers_batch(p_batch_size int DEFAULT 1000)
    RETURNS TABLE(id bigint) LANGUAGE sql AS $FUNC$
    WITH cte AS (
      SELECT id
      FROM public.providers
      WHERE is_active IS NULL OR auth_metadata IS NULL OR metadata IS NULL
      FOR UPDATE SKIP LOCKED
      LIMIT p_batch_size
    )
    UPDATE public.providers p
    SET is_active = coalesce(p.is_active, true),
        auth_metadata = coalesce(p.auth_metadata, '{}'::jsonb),
        metadata = coalesce(p.metadata, '{}'::jsonb),
        updated_at = now()
    FROM cte
    WHERE p.id = cte.id
    RETURNING p.id;
    $FUNC$;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='credentials')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='settings')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='jurisdiction_scope')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='is_enabled')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='priority')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='updated_at') THEN
    CREATE OR REPLACE FUNCTION public.backfill_retailer_providers_batch(p_batch_size int DEFAULT 500)
    RETURNS TABLE(id bigint) LANGUAGE sql AS $FUNC$
    WITH cte AS (
      SELECT id
      FROM public.retailer_providers
      WHERE credentials IS NULL OR settings IS NULL OR metadata IS NULL OR jurisdiction_scope IS NULL OR is_enabled IS NULL OR priority IS NULL
      FOR UPDATE SKIP LOCKED
      LIMIT p_batch_size
    )
    UPDATE public.retailer_providers rp
    SET credentials = coalesce(rp.credentials, '{}'::jsonb),
        settings = coalesce(rp.settings, '{}'::jsonb),
        metadata = coalesce(rp.metadata, '{}'::jsonb),
        jurisdiction_scope = coalesce(rp.jurisdiction_scope, '{}'::text[]),
        is_enabled = coalesce(rp.is_enabled, true),
        priority = coalesce(rp.priority, 100),
        updated_at = now()
    FROM cte
    WHERE rp.id = cte.id
    RETURNING rp.id;
    $FUNC$;
  END IF;
END $$;

-- 8) Provider-scoped claim function: claims up to p_batch_size rows.
-- If p_provider_id IS NOT NULL, only claims rows for that provider_id.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='last_seen_at')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_by')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='locked_at') THEN
    CREATE OR REPLACE FUNCTION public.claim_provider_items_batch_scoped(
      p_worker_id text,
      p_batch_size int,
      p_provider_id bigint DEFAULT NULL
    ) RETURNS TABLE(id bigint, provider_id bigint) LANGUAGE plpgsql SECURITY DEFINER AS $FUNC$
    BEGIN
      RETURN QUERY
      WITH cte AS (
        SELECT id, provider_id
        FROM public.provider_items
        WHERE (attributes IS NULL OR last_seen_at IS NULL)
          AND locked_by IS NULL
          AND (p_provider_id IS NULL OR provider_id = p_provider_id)
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
      )
      UPDATE public.provider_items pi
      SET locked_by = p_worker_id, locked_at = now()
      FROM cte
      WHERE pi.id = cte.id
      RETURNING pi.id, pi.provider_id;
    END;
    $FUNC$;

    REVOKE EXECUTE ON FUNCTION public.claim_provider_items_batch_scoped(text,int,bigint) FROM PUBLIC;
  END IF;
END $$;
