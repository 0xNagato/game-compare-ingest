-- 0027_worker_concurrent_indexes.sql
-- Switch partition_util.process_partition_index_jobs back to CREATE INDEX CONCURRENTLY
-- Idempotent: CREATE OR REPLACE FUNCTION

SET search_path TO public;

CREATE OR REPLACE FUNCTION partition_util.process_partition_index_jobs(max_items int DEFAULT 5)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  r record;
  done_count int := 0;
  idx text;
BEGIN
  FOR r IN
    SELECT id, partition_name, index_type FROM partition_util.partition_index_jobs
    WHERE status IN ('pending','failed')
    ORDER BY created_at
    LIMIT max_items
  LOOP
    BEGIN
      UPDATE partition_util.partition_index_jobs
        SET status='running', last_attempt_at=now(), attempts = COALESCE(attempts,0) + 1, error_message=NULL
      WHERE id = r.id;

      IF r.index_type = 'btree_series' THEN
        idx := format('%s_offer_jurisdiction_id_recorded_at_idx1', r.partition_name);
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = idx AND c.relkind='i') THEN
          EXECUTE format('CREATE INDEX CONCURRENTLY %I ON %I (offer_jurisdiction_id, recorded_at);', idx, r.partition_name);
        END IF;
      ELSIF r.index_type = 'brin_recorded' THEN
        idx := format('%s_brin', r.partition_name);
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = idx AND c.relkind='i') THEN
          EXECUTE format('CREATE INDEX CONCURRENTLY %I ON %I USING brin (recorded_at);', idx, r.partition_name);
        END IF;
      END IF;

      UPDATE partition_util.partition_index_jobs
        SET status='completed', completed_at=now()
      WHERE id = r.id;
      done_count := done_count + 1;
    EXCEPTION WHEN OTHERS THEN
      UPDATE partition_util.partition_index_jobs
        SET status='failed', error_message=SQLERRM, last_attempt_at=now()
      WHERE id = r.id;
    END;
  END LOOP;
  RETURN done_count;
END$$;
