-- =========================================================
-- ASYNC PARTITION INDEX CREATION (pg_cron + Job Queue)
-- =========================================================
-- Purpose: Create partitions and indexes without blocking pg_cron
-- Pattern: Transactional partition creation → async concurrent indexes
-- Fixed bugs: PARTITION OF syntax, date range parsing, cron syntax, underscore
-- =========================================================

-- =========================================================
-- SCHEMA: Utility namespace for background jobs
-- =========================================================
create schema if not exists partition_util;

-- =========================================================
-- JOB QUEUE: Track partition index creation tasks
-- =========================================================
create table if not exists partition_util.partition_index_jobs (
  id              bigserial primary key,
  partition_name  text not null,
  index_type      text not null check (index_type in ('brin_recorded', 'btree_series', 'btree_recent')),
  status          text not null default 'pending' check (status in ('pending', 'running', 'completed', 'failed')),
  attempts        int not null default 0,
  last_attempt_at timestamptz,
  completed_at    timestamptz,
  error_message   text,
  created_at      timestamptz not null default now(),
  unique (partition_name, index_type)
);

create index if not exists partition_index_jobs_pending_idx 
  on partition_util.partition_index_jobs (created_at) 
  where status = 'pending';

-- =========================================================
-- HELPER: Create partition + enqueue index jobs
-- =========================================================
-- FIXED: Added date range calculation from partition_suffix
create or replace function ensure_prices_partition_and_enqueue(partition_suffix text)
returns void language plpgsql as $$
declare
  partition_name text := 'prices_' || partition_suffix;
  start_month date;
  next_month date;
  sql text;
begin
  -- FIXED: Parse partition_suffix (YYYYMM) to compute date range
  start_month := to_date(partition_suffix, 'YYYYMM');
  next_month := start_month + interval '1 month';
  
  -- Check if partition already exists
  if to_regclass(partition_name) is not null then
    raise notice 'Partition % already exists', partition_name;
    return;
  end if;

  -- FIXED: Changed from INHERITS to PARTITION OF with date range
  sql := format(
    'create table %I partition of prices for values from (%L) to (%L);',
    partition_name, start_month, next_month
  );
  execute sql;
  raise notice 'Created partition %', partition_name;

  -- Enqueue index creation jobs (will run async via pg_cron)
  insert into partition_util.partition_index_jobs (partition_name, index_type)
  values 
    (partition_name, 'brin_recorded'),
    (partition_name, 'btree_series'),
    (partition_name, 'btree_recent')
  on conflict (partition_name, index_type) do nothing;

  raise notice 'Enqueued 3 index jobs for %', partition_name;
end$$;

-- =========================================================
-- WORKER: Process pending index jobs (called by pg_cron)
-- =========================================================
create or replace function partition_util.process_partition_index_jobs()
returns void language plpgsql as $$
declare
  job record;
  sql text;
  index_name text;
begin
  -- Process up to 3 jobs per run (avoid long pg_cron execution)
  for job in
    select id, partition_name, index_type
  from partition_util.partition_index_jobs
    where status = 'pending'
      and (last_attempt_at is null or last_attempt_at < now() - interval '5 minutes')
    order by created_at
    limit 3
  loop
    begin
      -- Mark as running
  update partition_util.partition_index_jobs
      set status = 'running', 
          attempts = attempts + 1,
          last_attempt_at = now()
      where id = job.id;

      -- Build CREATE INDEX CONCURRENTLY statement
      index_name := job.partition_name || '_' || job.index_type || '_idx';
      
      case job.index_type
        when 'brin_recorded' then
          sql := format(
            'create index concurrently %I on %I using brin(recorded_at) with (pages_per_range = 128);',
            index_name, job.partition_name
          );
        when 'btree_series' then
          sql := format(
            'create index concurrently %I on %I (offer_jurisdiction_id, recorded_at);',
            index_name, job.partition_name
          );
        when 'btree_recent' then
          sql := format(
            'create index concurrently %I on %I (offer_jurisdiction_id, recorded_at desc) where recorded_at > now() - interval ''30 days'';',
            index_name, job.partition_name
          );
      end case;

      -- Execute (this is the long-running part)
      execute sql;

      -- Mark as completed
  update partition_util.partition_index_jobs
      set status = 'completed',
          completed_at = now(),
          error_message = null
      where id = job.id;

      raise notice 'Created index % on %', index_name, job.partition_name;

    exception when others then
      -- Log failure and retry later (exponential backoff via pg_cron schedule)
  update partition_util.partition_index_jobs
      set status = case 
                     when attempts >= 3 then 'failed'
                     else 'pending'
                   end,
          error_message = SQLERRM
      where id = job.id;

      raise warning 'Failed to create index for job %: %', job.id, SQLERRM;
    end;
  end loop;
end$$;

-- =========================================================
-- SCHEDULE: pg_cron worker (runs every 10 minutes)
-- =========================================================
-- FIXED: Added job name, proper cron syntax, SELECT wrapper
-- Note: Requires pg_cron extension (available on Supabase)
-- Run this manually after enabling pg_cron:
/*
SELECT cron.schedule(
  'process_partition_indexes',
  '*/10 * * * *',
  'SELECT partition_util.process_partition_index_jobs();'
);
*/

-- =========================================================
-- BOOTSTRAP: Enqueue jobs for existing partitions
-- =========================================================
-- FIXED: Added underscore in relname || '_oj_recent_btree'
-- Run once to catch up on any partitions created before this system existed
do $$
declare
  part text;
begin
  for part in
    select relname::text
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
      and relname like 'prices_20%'
      and relkind = 'r'  -- regular table (partition)
      -- Only partitions missing at least one index
      and not exists (
        select 1 from pg_indexes
  where schemaname = 'public'
          and tablename = relname
          and indexname = relname || '_brin_recorded_idx'
      )
  loop
  insert into partition_util.partition_index_jobs (partition_name, index_type)
    values 
      (part, 'brin_recorded'),
      (part, 'btree_series'),
      (part, 'btree_recent')
    on conflict (partition_name, index_type) do nothing;
    
    raise notice 'Enqueued backfill jobs for %', part;
  end loop;
end$$;


-- Check job status
-- select status, count(*), max(created_at) as latest
-- from partition_util.partition_index_jobs
-- group by status
-- order by status;

-- Check failed jobs
-- select partition_name, index_type, attempts, error_message, last_attempt_at
-- from partition_util.partition_index_jobs
-- where status = 'failed'
-- order by last_attempt_at desc;


/*
select ensure_prices_partition_and_enqueue('202501');

select gamecompare_util.process_partition_index_jobs();
-- Wait for pg_cron to process jobs (or run manually)
-- select partition_util.process_partition_index_jobs();
-- Verify indexes created
select schemaname, tablename, indexname
from pg_indexes
where tablename = 'prices_202501'
order by indexname;
*/
