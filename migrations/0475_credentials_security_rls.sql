-- 0475_credentials_security_rls.sql
-- Purpose: Security hardening for provider credentials, optional encryption, safe view, RLS policies, and batched backfills.
-- Notes:
-- - Designed for Postgres 15+ (Supabase compatible). Uses pgcrypto for digest/pgp_sym_encrypt.
-- - Idempotent via IF NOT EXISTS and guarded DO blocks.

-- ====== 0. Ensure pgcrypto available (for digest/pgp_sym_encrypt) ======
DO $$ BEGIN
  BEGIN
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pgcrypto extension unavailable (%), continuing without encryption helpers', SQLERRM;
  END;
END $$;

-- ====== 1. Batched backfills (idempotent) ======
-- Adjust batch_size per environment before running in production; safe defaults used here.

-- 1.1 providers: backfill is_active, auth_metadata, metadata
-- NOTE: These columns may not exist yet if added in later migrations. Skip if columns don't exist.
DO $$
DECLARE
  batch_size int := 50000;
  min_id bigint;
BEGIN
  -- Only run if all three columns exist
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='providers' AND column_name='is_active'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='providers' AND column_name='auth_metadata'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='providers' AND column_name='metadata'
  ) THEN
    LOOP
      SELECT min(id) INTO min_id
      FROM public.providers
      WHERE is_active IS NULL OR auth_metadata IS NULL OR metadata IS NULL;
      EXIT WHEN min_id IS NULL;

      UPDATE public.providers
        SET is_active   = coalesce(is_active, true),
            auth_metadata = coalesce(auth_metadata, '{}'::jsonb),
            metadata    = coalesce(metadata, '{}'::jsonb)
      WHERE id >= min_id AND id < min_id + batch_size
        AND (is_active IS NULL OR auth_metadata IS NULL OR metadata IS NULL);

      PERFORM pg_sleep(0.05);
    END LOOP;
  END IF;
END $$;

-- 1.2 retailer_providers: backfill credentials, settings, metadata, jurisdiction_scope, is_enabled, priority
-- NOTE: Table and columns may not exist yet. Skip if columns don't exist.
DO $$
DECLARE
  batch_size int := 20000;
  min_id bigint;
BEGIN
  -- Check if table exists and all required columns exist
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='credentials')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='settings')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='jurisdiction_scope')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='is_enabled')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='priority') THEN
    LOOP
      SELECT min(id) INTO min_id
      FROM public.retailer_providers
      WHERE credentials IS NULL OR settings IS NULL OR metadata IS NULL OR jurisdiction_scope IS NULL OR is_enabled IS NULL OR priority IS NULL;
      EXIT WHEN min_id IS NULL;

      UPDATE public.retailer_providers
        SET credentials        = coalesce(credentials, '{}'::jsonb),
            settings           = coalesce(settings, '{}'::jsonb),
            metadata           = coalesce(metadata, '{}'::jsonb),
            jurisdiction_scope = coalesce(jurisdiction_scope, ARRAY[]::text[]),
            is_enabled         = coalesce(is_enabled, true),
            priority           = coalesce(priority, 100)
      WHERE id >= min_id AND id < min_id + batch_size
        AND (credentials IS NULL OR settings IS NULL OR metadata IS NULL OR jurisdiction_scope IS NULL OR is_enabled IS NULL OR priority IS NULL);

      PERFORM pg_sleep(0.05);
    END LOOP;
  END IF;
END $$;

-- 1.3 provider_items: backfill attributes
-- NOTE: Table and column may not exist yet. Skip if column doesn't exist.
DO $$
DECLARE
  batch_size int := 50000;
  min_id bigint;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes') THEN
    LOOP
      SELECT min(id) INTO min_id
      FROM public.provider_items
      WHERE attributes IS NULL;
      EXIT WHEN min_id IS NULL;

      UPDATE public.provider_items
        SET attributes = '{}'::jsonb
      WHERE id >= min_id AND id < min_id + batch_size
        AND attributes IS NULL;

      PERFORM pg_sleep(0.05);
    END LOOP;
  END IF;
END $$;

-- 1.4 provider_offers: backfill is_active
-- NOTE: Table and column may not exist yet. Skip if column doesn't exist.
DO $$
DECLARE
  batch_size int := 50000;
  min_id bigint;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_offers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_offers' AND column_name='is_active') THEN
    LOOP
      SELECT min(id) INTO min_id FROM public.provider_offers WHERE is_active IS NULL;
      EXIT WHEN min_id IS NULL;

      UPDATE public.provider_offers
        SET is_active = true
      WHERE id >= min_id AND id < min_id + batch_size AND is_active IS NULL;

      PERFORM pg_sleep(0.02);
    END LOOP;
  END IF;
END $$;

-- 1.5 provider_ingest_runs: backfill meta, stats, errors, items_processed, prices_written
-- NOTE: Table and columns may not exist yet. Skip if columns don't exist.
DO $$
DECLARE
  batch_size int := 50000;
  min_id bigint;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_ingest_runs')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='meta')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='stats')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='errors')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='items_processed')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='prices_written') THEN
    LOOP
      SELECT min(id) INTO min_id
      FROM public.provider_ingest_runs
      WHERE meta IS NULL OR stats IS NULL OR errors IS NULL OR items_processed IS NULL OR prices_written IS NULL;
      EXIT WHEN min_id IS NULL;

      UPDATE public.provider_ingest_runs
        SET meta            = coalesce(meta, '{}'::jsonb),
            stats           = coalesce(stats, '{}'::jsonb),
            errors          = coalesce(errors, '{}'::jsonb),
            items_processed = coalesce(items_processed, 0),
            prices_written  = coalesce(prices_written, 0)
      WHERE id >= min_id AND id < min_id + batch_size
        AND (meta IS NULL OR stats IS NULL OR errors IS NULL OR items_processed IS NULL OR prices_written IS NULL);

      PERFORM pg_sleep(0.05);
    END LOOP;
  END IF;
END $$;

-- 1.6 provider_sync_states: backfill sync_details
-- NOTE: Table and column may not exist yet. Skip if column doesn't exist.
DO $$
DECLARE
  batch_size int := 20000;
  min_id bigint;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_sync_states')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_sync_states' AND column_name='sync_details') THEN
    LOOP
      SELECT min(id) INTO min_id FROM public.provider_sync_states WHERE sync_details IS NULL;
      EXIT WHEN min_id IS NULL;

      UPDATE public.provider_sync_states
        SET sync_details = '{}'::jsonb
      WHERE id >= min_id AND id < min_id + batch_size AND sync_details IS NULL;

      PERFORM pg_sleep(0.02);
    END LOOP;
  END IF;
END $$;

-- ====== 2. Encryption placeholder and migration notes ======
-- Add an encrypted column credentials_enc; application should backfill using a secret key outside SQL.
-- NOTE: Table may not exist yet. Skip if table doesn't exist.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_providers') THEN
    ALTER TABLE public.retailer_providers
      ADD COLUMN IF NOT EXISTS credentials_enc bytea;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='credentials_enc'
    ) THEN
      COMMENT ON COLUMN public.retailer_providers.credentials_enc IS
        'Encrypted credentials (pgp_sym_encrypt). Backfill via application; do NOT store keys in SQL.';
    END IF;
  END IF;
END $$;

-- ====== 3. Safe view + guarded grants (hide credentials from authenticated) ======
-- NOTE: Table and columns may not exist yet. Skip if columns don't exist.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='settings')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='jurisdiction_scope')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='is_enabled')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='priority') THEN
    -- Create safe view
    CREATE OR REPLACE VIEW public.retailer_providers_safe AS
    SELECT id, retailer_id, provider_id, settings, jurisdiction_scope, last_synced_at, next_sync_at,
           sync_status, sync_cursor, sync_error, rate_limit_per_minute, rate_limit_burst,
           metadata, is_enabled, priority
    FROM public.retailer_providers;

    -- Grant SELECT on the safe view to authenticated role (if role exists)
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
      EXECUTE 'GRANT SELECT ON public.retailer_providers_safe TO authenticated';
    END IF;

    -- Revoke direct SELECT on base table from authenticated (guarded)
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
      EXECUTE 'REVOKE SELECT ON public.retailer_providers FROM authenticated';
    END IF;

    -- Enable RLS
    ALTER TABLE public.retailer_providers ENABLE ROW LEVEL SECURITY;
  END IF;
END $$;

-- ====== 4. RLS helper function ======

-- Define function without dynamic EXECUTE to avoid parser issues under custom runner.
-- The function itself checks for the presence of retailer_users at execution time.
CREATE OR REPLACE FUNCTION public.is_retailer_member(rid bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER STABLE AS $$
BEGIN
  IF to_regclass('public.retailer_users') IS NOT NULL THEN
    RETURN EXISTS (
      SELECT 1
      FROM public.retailer_users ru
      WHERE ru.retailer_id = rid
        AND ru.user_id::text = (SELECT current_setting('request.jwt.claim.sub', true))
    );
  ELSE
    RETURN false;
  END IF;
END;
$$;

-- Ensure function execute is not public
REVOKE EXECUTE ON FUNCTION public.is_retailer_member(bigint) FROM PUBLIC;

-- Recreate policies idempotently (no dynamic EXECUTE; split write-deny into separate actions)
-- NOTE: Table and is_enabled column may not exist yet. Skip if column doesn't exist.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='is_enabled') THEN
    DROP POLICY IF EXISTS retailer_providers_select_for_members ON public.retailer_providers;
    CREATE POLICY retailer_providers_select_for_members
      ON public.retailer_providers
      FOR SELECT TO authenticated
      USING ( is_enabled = true AND public.is_retailer_member(retailer_id) );

    DROP POLICY IF EXISTS retailer_providers_no_insert_for_authenticated ON public.retailer_providers;
    CREATE POLICY retailer_providers_no_insert_for_authenticated
      ON public.retailer_providers
      FOR INSERT TO authenticated
      WITH CHECK (false);

    DROP POLICY IF EXISTS retailer_providers_no_update_for_authenticated ON public.retailer_providers;
    CREATE POLICY retailer_providers_no_update_for_authenticated
      ON public.retailer_providers
      FOR UPDATE TO authenticated
      USING (false)
      WITH CHECK (false);

    DROP POLICY IF EXISTS retailer_providers_no_delete_for_authenticated ON public.retailer_providers;
    CREATE POLICY retailer_providers_no_delete_for_authenticated
      ON public.retailer_providers
      FOR DELETE TO authenticated
      USING (false);
  END IF;
END $$;

-- ====== 5. Credential update helper (SECURITY DEFINER) ======
DO $$ BEGIN
  -- Only create the encryption helper if pgcrypto is present
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgcrypto') THEN
    CREATE OR REPLACE FUNCTION public.update_retailer_credentials_enc(rp_id bigint, plaintext_credentials text, key text)
    RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $FUNC$
    BEGIN
      UPDATE public.retailer_providers
        SET credentials_enc = pgp_sym_encrypt(plaintext_credentials, key, 'compress-algo=1')
      WHERE id = rp_id;
    END;
    $FUNC$;
    REVOKE EXECUTE ON FUNCTION public.update_retailer_credentials_enc(bigint, text, text) FROM PUBLIC;
  END IF;
END $$;
-- Example (do not grant to authenticated):
-- GRANT EXECUTE ON FUNCTION public.update_retailer_credentials_enc(bigint, text, text) TO internal_migration_role;

-- ====== 6. Audit table + trigger for credential changes ======
CREATE TABLE IF NOT EXISTS public.retailer_credentials_audit (
  id bigserial PRIMARY KEY,
  retailer_provider_id bigint NOT NULL,
  changed_by text,
  changed_at timestamptz NOT NULL DEFAULT now(),
  operation text NOT NULL,
  cred_hash text
);

DO $$ BEGIN
  -- Create audit function only if pgcrypto (digest) is available
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgcrypto') THEN
    CREATE OR REPLACE FUNCTION public.retailer_credentials_audit_fn()
    RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $FUNC$
    BEGIN
      IF TG_OP = 'UPDATE' THEN
        INSERT INTO public.retailer_credentials_audit (retailer_provider_id, changed_by, operation, cred_hash)
        VALUES (NEW.id, current_setting('request.jwt.claim.sub', true), 'UPDATE', encode(digest(coalesce(NEW.credentials_enc, ''::bytea), 'sha256'), 'hex'));
        RETURN NEW;
      ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO public.retailer_credentials_audit (retailer_provider_id, changed_by, operation, cred_hash)
        VALUES (NEW.id, current_setting('request.jwt.claim.sub', true), 'INSERT', encode(digest(coalesce(NEW.credentials_enc, ''::bytea), 'sha256'), 'hex'));
        RETURN NEW;
      ELSE
        RETURN NULL;
      END IF;
    END;
    $FUNC$;

    -- audit function created; trigger will be (re)created below if table exists
  END IF;
END $$;

-- Ensure trigger exists when function is present and base table exists
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgcrypto')
     AND to_regclass('public.retailer_providers') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS retailer_credentials_audit_trg ON public.retailer_providers;
    CREATE TRIGGER retailer_credentials_audit_trg
    AFTER INSERT OR UPDATE ON public.retailer_providers
    FOR EACH ROW EXECUTE FUNCTION public.retailer_credentials_audit_fn();
  END IF;
END $$;

-- ====== 7. Guidance ======
-- After backfilling credentials_enc via the application and validating, consider:
--  - Dropping plaintext column: ALTER TABLE public.retailer_providers DROP COLUMN IF EXISTS credentials;
--  - Enforcing NOT NULL on credentials_enc: ALTER TABLE public.retailer_providers ALTER COLUMN credentials_enc SET NOT NULL;
--  - Keep access restricted to the safe view for non-privileged roles.
