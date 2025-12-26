-- =========================================================
-- EXTENSIONS (Supabase-friendly)
-- =========================================================
create extension if not exists citext;
create extension if not exists pg_trgm;
create extension if not exists bloom;

-- =========================================================
-- ENUMS
-- =========================================================
create type cmp_op as enum ('above','below');
create type sellable_kind as enum ('software','hardware');

-- =========================================================
-- LOOKUPS / SLOW-CHANGING DIMENSIONS
-- =========================================================
create table currencies (
  id          bigserial primary key,
  code        text not null unique,      -- e.g., USD, EUR, JPY
  name        text not null
);

create table countries (
  id          bigserial primary key,
  iso2        char(2) not null unique,
  iso3        char(3) not null unique,
  name        text not null,
  currency_id bigint references currencies(id)
);

-- Jurisdiction = country + optional region/subdivision (e.g., US-CA)
create table jurisdictions (
  id           bigserial primary key,
  country_id   bigint not null references countries(id) on delete cascade,
  region_code  text,                                -- null = national
  unique (country_id, coalesce(region_code,''))
);

-- Effective tax rules per jurisdiction
create table tax_rules (
  id                 bigserial primary key,
  jurisdiction_id    bigint not null references jurisdictions(id) on delete cascade,
  effective_from     date not null,
  effective_to       date,
  rate_basis_points  integer not null,              -- 750 => 7.50%
  inclusive          boolean not null default true,
  notes              text,
  unique (jurisdiction_id, effective_from)
);

-- =========================================================
-- PRODUCTS (kept cold), SOFTWARE/HARDWARE split
-- =========================================================
create table products (
  id          bigserial primary key,
  slug        citext not null unique,
  name        text not null,
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
  -- 1:1 with software via video_game_id unique FK
  video_game_id      bigint not null unique references software(video_game_id) on delete cascade,
  title              text not null,
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
  code    text not null unique,
  name    text not null,
  family  text
);

create table video_games (
  id                 bigserial primary key,
  title_id           bigint not null references video_game_titles(id) on delete cascade,
  platform_id        bigint not null references platforms(id),
  edition            text,                              -- e.g., Deluxe
  slug               citext unique,
  release_date       date,
  metadata           jsonb,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  unique (title_id, platform_id, coalesce(edition,''))
);
create index idx_video_games_release_date_brin
  on video_games using brin (release_date) with (pages_per_range=64);

-- Optional multi-column bloom index for slug+platform probes (if extension available)
do $$
begin
  if exists (select 1 from pg_extension where extname = 'bloom') then
    begin
      execute 'create index if not exists blm_video_games_slug_platform on public.video_games using bloom ((cast(slug as text)), platform_id) with (col1=4, col2=4)';
    exception when undefined_object or invalid_object_definition then
      null;
    end;
  end if;
end$$;

-- HARDWARE side
create table hardware (
  platform_id  bigint primary key references products(id) on delete cascade
);

-- Console model/SKU lines
create table game_consoles (
  id                 bigserial primary key,
  platform_id        bigint not null references hardware(platform_id) on delete cascade,
  model              text not null,
  variant            text,
  slug               citext unique,
  release_date       date,
  metadata           jsonb,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  unique (platform_id, model, coalesce(variant,''))
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
  name        text not null
);

create table offers (
  id          bigserial primary key,
  sellable_id bigint not null references sellables(id) on delete cascade,
  retailer_id bigint not null references retailers(id) on delete cascade,
  sku         text,
  is_active   boolean not null default true,
  metadata    jsonb,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  unique (sellable_id, retailer_id, coalesce(sku,''))
);

-- Where an offer is valid + its currency
create table offer_jurisdictions (
  id               bigserial primary key,
  offer_id         bigint not null references offers(id) on delete cascade,
  jurisdiction_id  bigint not null references jurisdictions(id) on delete cascade,
  currency_id      bigint not null references currencies(id),
  unique (offer_id, jurisdiction_id)
);

-- =========================================================
-- PRICES (HOT FACT) — partitioned monthly by recorded_at
-- =========================================================
create table prices (
  id                     bigserial,
  offer_jurisdiction_id  bigint not null references offer_jurisdictions(id) on delete cascade,
  recorded_at            timestamptz not null,
  amount_minor           bigint not null check (amount_minor >= 0), -- minor units
  tax_inclusive          boolean not null,
  fx_minor_per_unit      bigint,            -- optional: e.g., minor units of user currency per unit
  btc_sats_per_unit      bigint,            -- optional: snapshot to BTC sats
  meta                   jsonb,
  primary key (id, recorded_at)
) partition by range (recorded_at);

-- Parent-level indexes (materialized per partition)
create index prices_series_idx on only prices (offer_jurisdiction_id, recorded_at);
create index prices_recent_idx on only prices (offer_jurisdiction_id, recorded_at desc)
  where recorded_at > now() - interval '30 days';

-- Example monthly partition (repeat per month, or use pg_partman)
-- create table if not exists prices_2025_11 partition of prices
--   for values from ('2025-11-01') to ('2025-12-01');

-- Helper: create a rolling next-month partition (simple version)
create or replace function ensure_prices_partition_for(ts timestamptz)
returns void language plpgsql as $$
declare
  start_month date := date_trunc('month', ts)::date;
  next_month  date := (date_trunc('month', ts) + interval '1 month')::date;
  part_name   text := format('prices_%s', to_char(start_month, 'YYYY_MM'));
  sql text;
begin
  if to_regclass(part_name) is null then
    -- Create partition
    sql := format(
      'create table %I partition of prices for values from (%L) to (%L);',
      part_name, start_month, next_month
    );
    execute sql;
    
    -- Create indexes CONCURRENTLY (runs outside transaction context)
    -- These will not block writes to other partitions or tables
    execute format(
      'create index concurrently %I_series_idx on %I (offer_jurisdiction_id, recorded_at);',
      part_name, part_name
    );
    execute format(
      'create index concurrently %I_recent_idx on %I (offer_jurisdiction_id, recorded_at desc) where recorded_at > now() - interval ''30 days'';',
      part_name, part_name
    );
    
    raise notice 'Created partition % with concurrent indexes', part_name;
  end if;
end$$;

-- Optional trigger to auto-provision partition on insert (lightweight)
create or replace function prices_partition_guard()
returns trigger language plpgsql as $$
begin
  perform ensure_prices_partition_for(new.recorded_at);
  return new;
end$$;

drop trigger if exists prices_partition_guard_trg on prices;
create trigger prices_partition_guard_trg
before insert on prices
for each row execute function prices_partition_guard();

-- =========================================================
-- LATEST (tiny read-hot table)
-- =========================================================
create table current_price (
  offer_jurisdiction_id  bigint primary key
    references offer_jurisdictions(id) on delete cascade,
  amount_minor           bigint not null,
  recorded_at            timestamptz not null
);

-- Upsert pattern (for Rust ingestion):
-- insert into current_price(offer_jurisdiction_id, amount_minor, recorded_at)
-- values ($1,$2,$3)
-- on conflict (offer_jurisdiction_id) do update
--   set amount_minor=excluded.amount_minor,
--       recorded_at=excluded.recorded_at;

-- =========================================================
-- USERS & ALERTS
-- =========================================================
-- If you already have users, keep yours. This is a minimal stub:
create table users (
  id          bigserial primary key,
  email       citext not null unique,
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
  threshold_minor         bigint not null,       -- store in user's chosen currency if you wish
  active                  boolean not null default true,
  last_triggered_at       timestamptz,
  settings                jsonb,
  created_at              timestamptz not null default now(),
  updated_at              timestamptz not null default now()
);

create index alerts_active_oj_idx
  on alerts (offer_jurisdiction_id) where active;

create index alerts_user_active_idx
  on alerts (user_id) where active;

-- =========================================================
-- SEARCH / USABILITY INDEXES
-- =========================================================
-- Slug/name lookups (fast UX)
create index products_slug_trgm_idx on products using gin (slug gin_trgm_ops);
create index products_name_trgm_idx on products using gin (name gin_trgm_ops);

create index titles_title_trgm_idx on video_game_titles using gin (title gin_trgm_ops);
create index video_game_titles_search_idx on video_game_titles using gin (search_vector);
create index consoles_model_trgm_idx on game_consoles using gin (model gin_trgm_ops);

-- Retailer quick filters
create index offers_active_idx on offers (sellable_id) where is_active;

-- Jurisdiction routing
create index jurisdictions_country_region_idx
  on jurisdictions (country_id, coalesce(region_code,''));

-- =========================================================
-- SPATIE / LARAVEL MEDIALIBRARY COMPAT
-- =========================================================
-- Keep Spatie's `media` table unmodified. No extra FKs (it's polymorphic).
-- If not present, a minimal compatible table (commented out):
/*
create table if not exists media (
  id bigserial primary key,
  uuid uuid unique,
  model_type varchar not null,
  model_id bigint not null,
  collection_name varchar not null,
  name varchar not null,
  file_name varchar not null,
  mime_type varchar,
  disk varchar not null,
  conversions_disk varchar,
  size bigint not null,
  manipulations json not null,
  custom_properties jsonb not null,
  generated_conversions json not null,
  responsive_images json not null,
  order_column int,
  created_at timestamptz,
  updated_at timestamptz
);
*/
