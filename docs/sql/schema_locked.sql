-- =========================================================
-- SCHEMA: gamecompare (HARD-LOCKED PRODUCTION SCHEMA)
-- =========================================================
-- VERSION: 1.0.0
-- DATE: 2025-11-10
-- DESCRIPTION: Production-locked schema for GameCompare platform
-- 
-- CRITICAL RULES:
-- 1. All tables in gamecompare schema (explicit qualification)
-- 2. NO "IF NOT EXISTS" - fails fast on conflicts
-- 3. Schema changes ONLY via numbered migrations
-- 4. All FKs use ON DELETE CASCADE for lifecycle binding
-- 5. Money stored as BIGINT minor units (never NUMERIC)
-- 6. Timestamps always TIMESTAMPTZ (UTC)
-- 7. Search columns use citext + pg_trgm GIN indexes
-- 8. Prices table partitioned monthly by recorded_at
-- =========================================================

-- Create schema
create schema if not exists gamecompare;
set search_path to gamecompare, public;

-- =========================================================
-- EXTENSIONS (Supabase-friendly)
-- =========================================================
create extension if not exists citext schema public;
create extension if not exists pg_trgm schema public;
create extension if not exists bloom schema public;

-- =========================================================
-- ENUMS
-- =========================================================
create type cmp_op as enum ('above', 'below');
create type sellable_kind as enum ('software', 'hardware');

-- =========================================================
-- LOOKUPS / SLOW-CHANGING DIMENSIONS
-- =========================================================
create table currencies (
  id          bigserial primary key,
  code        text not null unique check (length(code) = 3),
  name        text not null check (length(name) > 0)
);

create table countries (
  id          bigserial primary key,
  iso2        char(2) not null unique,
  iso3        char(3) not null unique,
  name        text not null check (length(name) > 0),
  currency_id bigint references currencies(id) on delete restrict
);

-- Jurisdiction = country + optional region/subdivision (e.g., US-CA)
create table jurisdictions (
  id           bigserial primary key,
  country_id   bigint not null references countries(id) on delete cascade,
  region_code  text check (region_code is null or length(region_code) > 0),
  unique (country_id, coalesce(region_code, ''))
);

-- Effective tax rules per jurisdiction
create table tax_rules (
  id                 bigserial primary key,
  jurisdiction_id    bigint not null references jurisdictions(id) on delete cascade,
  effective_from     date not null,
  effective_to       date check (effective_to is null or effective_to >= effective_from),
  rate_basis_points  integer not null check (rate_basis_points >= 0 and rate_basis_points <= 10000),
  inclusive          boolean not null default true,
  notes              text,
  unique (jurisdiction_id, effective_from)
);

-- =========================================================
-- PRODUCTS (kept cold), SOFTWARE/HARDWARE split
-- =========================================================
create table products (
  id          bigserial primary key,
  slug        citext not null unique check (length(slug) > 0),
  name        text not null check (length(name) > 0),
  category    text,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

-- SOFTWARE side
create table software (
  video_game_id  bigint primary key references products(id) on delete cascade
);

-- Titles are the canonical "franchise/name" users see
create table video_game_titles (
  id                 bigserial primary key,
  video_game_id      bigint not null unique references software(video_game_id) on delete cascade,
  title              text not null check (length(title) > 0),
  normalized_title   text,
  search_vector      tsvector generated always as (
                        to_tsvector('english', coalesce(title,'') || ' ' || coalesce(normalized_title,''))
                      ) stored,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

-- A sellable software variant (per platform/edition/regionization)
create table platforms (
  id      bigserial primary key,
  code    text not null unique check (length(code) > 0),
  name    text not null check (length(name) > 0),
  family  text
);

create table video_games (
  id                 bigserial primary key,
  title_id           bigint not null references video_game_titles(id) on delete cascade,
  platform_id        bigint not null references platforms(id) on delete restrict,
  edition            text,
  slug               citext unique,
  release_date       date,
  metadata           jsonb not null default '{}',
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  unique (title_id, platform_id, coalesce(edition, ''))
);
create index idx_video_games_release_date_brin
  on video_games using brin (release_date) with (pages_per_range = 64);
create index blm_video_games_slug_platform
  on video_games using bloom ((cast(slug as text)), platform_id) with (col1 = 4, col2 = 4);

-- Bloom coverage handled via migration 0489_decouple_titles_products (slug + platform)

-- HARDWARE side
create table hardware (
  platform_id  bigint primary key references products(id) on delete cascade
);

-- Console model/SKU lines
create table game_consoles (
  id                 bigserial primary key,
  platform_id        bigint not null references hardware(platform_id) on delete cascade,
  model              text not null check (length(model) > 0),
  variant            text,
  slug               citext unique,
  release_date       date,
  metadata           jsonb not null default '{}',
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  unique (platform_id, model, coalesce(variant, ''))
);

-- =========================================================
-- SELLABLES unify software/hardware for commerce
-- =========================================================
create table sellables (
  id                 bigserial primary key,
  kind               sellable_kind not null,
  software_title_id  bigint references video_game_titles(id) on delete cascade,
  console_id         bigint references game_consoles(id) on delete cascade,
  check (
    (kind = 'software' and software_title_id is not null and console_id is null)
    or
    (kind = 'hardware' and console_id is not null and software_title_id is null)
  )
);

-- =========================================================
-- RETAIL, OFFERS, OFFER-JURISDICTIONS
-- =========================================================
create table retailers (
  id          bigserial primary key,
  slug        citext unique,
  name        text not null check (length(name) > 0),
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

create table offers (
  id          bigserial primary key,
  sellable_id bigint not null references sellables(id) on delete cascade,
  retailer_id bigint not null references retailers(id) on delete cascade,
  sku         text,
  is_active   boolean not null default true,
  metadata    jsonb not null default '{}',
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  unique (sellable_id, retailer_id, coalesce(sku, ''))
);

-- Where an offer is valid + its currency
create table offer_jurisdictions (
  id               bigserial primary key,
  offer_id         bigint not null references offers(id) on delete cascade,
  jurisdiction_id  bigint not null references jurisdictions(id) on delete cascade,
  currency_id      bigint not null references currencies(id) on delete restrict,
  unique (offer_id, jurisdiction_id)
);

-- =========================================================
-- PRICES (HOT FACT) — partitioned monthly by recorded_at
-- =========================================================
create table prices (
  id                     bigserial,
  offer_jurisdiction_id  bigint not null references offer_jurisdictions(id) on delete cascade,
  recorded_at            timestamptz not null,
  amount_minor           bigint not null check (amount_minor >= 0),
  tax_inclusive          boolean not null,
  fx_minor_per_unit      bigint check (fx_minor_per_unit is null or fx_minor_per_unit > 0),
  btc_sats_per_unit      bigint check (btc_sats_per_unit is null or btc_sats_per_unit > 0),
  meta                   jsonb not null default '{}',
  primary key (id, recorded_at)
) partition by range (recorded_at);

-- Parent-level indexes (materialized per partition)
create index prices_series_idx on prices (offer_jurisdiction_id, recorded_at);
create index prices_recent_idx on prices (offer_jurisdiction_id, recorded_at desc)
  where recorded_at > now() - interval '30 days';

-- Helper: create a rolling next-month partition
create or replace function ensure_price_partition(ts timestamptz)
returns void language plpgsql as $$
declare
  start_month date := date_trunc('month', ts)::date;
  next_month  date := (date_trunc('month', ts) + interval '1 month')::date;
  part_name   text := format('prices_%s', to_char(start_month, 'YYYY_MM'));
  fq_part     text := '' || part_name;
begin
  if to_regclass(fq_part) is null then
    execute format(
      'create table %I.%I partition of prices for values from (%L) to (%L)',
      'gamecompare', part_name, start_month, next_month
    );
    execute format('create index on %I (offer_jurisdiction_id, recorded_at)', part_name);
    execute format(
      'create index on %I (offer_jurisdiction_id, recorded_at desc) where recorded_at > now() - interval ''30 days''',
      part_name
    );
  end if;
end$$;

-- =========================================================
-- LATEST (tiny read-hot table)
-- =========================================================
create table current_price (
  offer_jurisdiction_id  bigint primary key
    references offer_jurisdictions(id) on delete cascade,
  amount_minor           bigint not null check (amount_minor >= 0),
  recorded_at            timestamptz not null
);

-- =========================================================
-- USERS & ALERTS
-- =========================================================
create table users (
  id          bigserial primary key,
  email       citext not null unique check (length(email) >= 3 and email ~ '^[^@]+@[^@]+\.[^@]+$'),
  name        text,
  timezone    text not null default 'UTC',
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

-- Alerts target an offer in a specific jurisdiction
create table alerts (
  id                      bigserial primary key,
  user_id                 bigint not null references users(id) on delete cascade,
  offer_jurisdiction_id   bigint not null references offer_jurisdictions(id) on delete cascade,
  op                      cmp_op not null,
  threshold_minor         bigint not null check (threshold_minor >= 0),
  active                  boolean not null default true,
  last_triggered_at       timestamptz,
  settings                jsonb not null default '{}',
  created_at              timestamptz not null default now(),
  updated_at              timestamptz not null default now()
);

create index alerts_active_oj_idx on alerts (offer_jurisdiction_id) where active;
create index alerts_user_active_idx on alerts (user_id) where active;

-- =========================================================
-- SEARCH / USABILITY INDEXES
-- =========================================================
create index products_slug_trgm_idx on products using gin (slug gin_trgm_ops);
create index products_name_trgm_idx on products using gin (name gin_trgm_ops);
create index titles_title_trgm_idx on video_game_titles using gin (title gin_trgm_ops);
create index video_game_titles_search_idx on video_game_titles using gin (search_vector);
create index consoles_model_trgm_idx on game_consoles using gin (model gin_trgm_ops);
create index offers_active_idx on offers (sellable_id) where is_active;
create index jurisdictions_country_region_idx on jurisdictions (country_id, coalesce(region_code, ''));

-- =========================================================
-- SCHEMA LOCK VERIFICATION
-- =========================================================
comment on schema gamecompare is 'GameCompare production schema v1.0.0 - locked 2025-11-10';
comment on table prices is 'Partitioned by month on recorded_at - hot write path';
comment on table current_price is 'Latest price snapshot - hot read path';
comment on function ensure_price_partition is 'Auto-creates monthly partitions on demand';
