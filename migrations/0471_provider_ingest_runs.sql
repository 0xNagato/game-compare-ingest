-- Provider ingest runs (observability)
-- Idempotent: safe to re-run
do $$ begin
  create table if not exists public.provider_ingest_runs (
    id               bigserial primary key,
    provider_id      bigint not null references public.providers(id) on delete cascade,
    started_at       timestamptz not null default now(),
    ended_at         timestamptz,
    status           text not null default 'running', -- running|ok|partial|error
    region_code      text,
    items_processed  bigint not null default 0,
    prices_written   bigint not null default 0,
    errors           jsonb,
    meta             jsonb
  );
exception when duplicate_table then null; end $$;

create index if not exists pir_provider_started_idx
  on public.provider_ingest_runs(provider_id, started_at desc);
