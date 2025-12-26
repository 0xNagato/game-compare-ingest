-- 0017_add_brin_indexes.sql
-- Purpose: Add BRIN and supporting composite indexes for large time-series partitions (prices)
-- Idempotent: Safe to re-run; skips existing indexes.
-- Strategy:
--   1. Iterate existing prices_* partitions and create BRIN on recorded_at if missing.
--   2. Create composite btree (offer_jurisdiction_id, recorded_at) if not already present (some partitions may predate parent-only index cloning).
--   3. (Optional commented) Create recent partial index confined to partition range for hot 30d queries.

DO $$ DECLARE
  part REGCLASS;
  part_name TEXT;
BEGIN
  FOR part IN
    SELECT oid::regclass
    FROM pg_class
    WHERE relname LIKE 'prices_%' AND relkind = 'r'
      AND relname ~ 'prices_\\d{4}_\\d{2}'
  LOOP
    part_name := part::text;

    -- BRIN index on recorded_at
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes WHERE tablename = part_name AND indexname = part_name || '_recorded_brin_idx'
    ) THEN
      EXECUTE format('CREATE INDEX %I_recorded_brin_idx ON %s USING brin(recorded_at) WITH (pages_per_range = 128);', part_name, part_name);
      RAISE NOTICE 'Created BRIN index on %', part_name;
    END IF;

    -- Composite btree (offer_jurisdiction_id, recorded_at) if missing
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes WHERE tablename = part_name AND indexname = part_name || '_oj_recorded_btree'
    ) THEN
      EXECUTE format('CREATE INDEX %I_oj_recorded_btree ON %s (offer_jurisdiction_id, recorded_at);', part_name, part_name);
      RAISE NOTICE 'Created composite btree index on %', part_name;
    END IF;

    -- Optional: Recent partial index (uncomment if desired)
    -- EXECUTE format('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I_recent_partial_idx ON %s (offer_jurisdiction_id, recorded_at DESC) WHERE recorded_at > now() - interval ''30 days'';', part_name, part_name);
  END LOOP;
END $$;

-- Verification query (run manually):
-- SELECT tablename, indexname FROM pg_indexes WHERE tablename LIKE 'prices_%' ORDER BY tablename, indexname;