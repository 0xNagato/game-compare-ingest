-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public._sqlx_migrations (
  version bigint NOT NULL,
  description text NOT NULL,
  installed_on timestamp with time zone NOT NULL DEFAULT now(),
  success boolean NOT NULL,
  checksum bytea NOT NULL,
  execution_time bigint NOT NULL,
  CONSTRAINT _sqlx_migrations_pkey PRIMARY KEY (version)
);
CREATE TABLE public.alerts (
  id bigint NOT NULL DEFAULT nextval('alerts_id_seq'::regclass),
  user_id bigint NOT NULL,
  product_id bigint NOT NULL,
  region_code character varying NOT NULL,
  threshold_btc numeric NOT NULL,
  comparison_operator character varying NOT NULL DEFAULT 'below'::character varying CHECK (comparison_operator::text = ANY (ARRAY['below'::character varying, 'above'::character varying]::text[])),
  channel character varying NOT NULL CHECK (channel::text = ANY (ARRAY['email'::character varying, 'discord'::character varying]::text[])),
  is_active boolean NOT NULL DEFAULT true,
  last_triggered_at timestamp with time zone,
  settings jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  user_auth_id uuid,
  CONSTRAINT alerts_pkey PRIMARY KEY (id),
  CONSTRAINT alerts_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id),
  CONSTRAINT alerts_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
);
CREATE TABLE public.canonical_media (
  id bigint NOT NULL DEFAULT nextval('canonical_media_id_seq'::regclass),
  url text NOT NULL,
  url_hash text NOT NULL UNIQUE,
  cdn_url text,
  mime_type text,
  width integer CHECK (width IS NULL OR width > 0),
  height integer CHECK (height IS NULL OR height > 0),
  size_bytes bigint CHECK (size_bytes IS NULL OR size_bytes > 0),
  hash text,
  storage_provider text,
  metadata jsonb DEFAULT '{}'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  last_verified_at timestamp with time zone,
  access_count bigint DEFAULT 0,
  CONSTRAINT canonical_media_pkey PRIMARY KEY (id)
);
CREATE TABLE public.countries (
  id bigint NOT NULL DEFAULT nextval('countries_id_seq'::regclass),
  iso2 character NOT NULL UNIQUE,
  iso3 character NOT NULL UNIQUE,
  name text NOT NULL,
  currency_id bigint,
  code2 character UNIQUE,
  CONSTRAINT countries_pkey PRIMARY KEY (id),
  CONSTRAINT countries_currency_id_fkey FOREIGN KEY (currency_id) REFERENCES public.currencies(id)
);
CREATE TABLE public.currencies (
  id bigint NOT NULL DEFAULT nextval('currencies_id_seq'::regclass),
  code text NOT NULL UNIQUE,
  name text NOT NULL,
  minor_unit smallint NOT NULL,
  CONSTRAINT currencies_pkey PRIMARY KEY (id)
);
CREATE TABLE public.current_price (
  offer_jurisdiction_id bigint NOT NULL,
  amount_minor bigint NOT NULL,
  recorded_at timestamp with time zone NOT NULL,
  amount_display_price numeric DEFAULT ((amount_minor)::numeric / (100)::numeric),
  agent text NOT NULL DEFAULT 'unknown'::text,
  agent_priority smallint NOT NULL DEFAULT 0,
  CONSTRAINT current_price_pkey PRIMARY KEY (offer_jurisdiction_id)
);
CREATE TABLE public.exchange_rates (
  id bigint NOT NULL DEFAULT nextval('exchange_rates_id_seq'::regclass),
  base_currency text NOT NULL,
  quote_currency text NOT NULL,
  rate double precision NOT NULL,
  provider text NOT NULL,
  fetched_at timestamp with time zone NOT NULL,
  metadata jsonb,
  CONSTRAINT exchange_rates_pkey PRIMARY KEY (id)
);
CREATE TABLE public.game_consoles (
  id bigint NOT NULL DEFAULT nextval('game_consoles_id_seq'::regclass),
  product_id bigint NOT NULL,
  model text NOT NULL,
  variant text,
  slug USER-DEFINED UNIQUE,
  release_date date,
  metadata jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  platform_id bigint NOT NULL UNIQUE,
  CONSTRAINT game_consoles_pkey PRIMARY KEY (id),
  CONSTRAINT game_consoles_platform_id_fkey FOREIGN KEY (platform_id) REFERENCES public.platforms(id)
);
CREATE TABLE public.game_images (
  id bigint NOT NULL DEFAULT nextval('game_images_id_seq'::regclass),
  game_provider_id bigint NOT NULL,
  image_key character varying,
  url character varying NOT NULL,
  mime_type character varying,
  width integer,
  height integer,
  rank integer NOT NULL DEFAULT 0,
  caption character varying,
  variants json,
  metadata json,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  media_id bigint,
  storage_disk character varying,
  storage_path character varying,
  provider_item_id character varying,
  video_game_source_id bigint,
  provider_payload json,
  small_url text,
  super_url text,
  platforms ARRAY,
  canonical_media_id bigint,
  video_game_id bigint,
  CONSTRAINT game_images_pkey PRIMARY KEY (id),
  CONSTRAINT game_images_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id),
  CONSTRAINT game_images_canonical_media_id_fkey FOREIGN KEY (canonical_media_id) REFERENCES public.canonical_media(id),
  CONSTRAINT game_images_video_game_id_fk FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
);
CREATE TABLE public.game_media (
  video_game_id bigint NOT NULL,
  source USER-DEFINED NOT NULL,
  external_id text NOT NULL,
  media_type USER-DEFINED NOT NULL,
  url text NOT NULL CHECK (length(url) > 0),
  cdn_url text,
  width integer CHECK (width IS NULL OR width > 0),
  height integer CHECK (height IS NULL OR height > 0),
  size_bytes bigint CHECK (size_bytes IS NULL OR size_bytes > 0),
  duration_seconds integer CHECK (duration_seconds IS NULL OR duration_seconds > 0),
  mime_type text,
  hash text,
  provider_data jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  original_url text,
  thumbnail_url text,
  stream_url text,
  poster_url text,
  CONSTRAINT game_media_pkey PRIMARY KEY (video_game_id, source, external_id)
);
CREATE TABLE public.game_videos (
  id bigint NOT NULL DEFAULT nextval('game_videos_id_seq'::regclass),
  game_provider_id bigint NOT NULL,
  video_key character varying,
  name character varying,
  description text,
  site_detail_url character varying,
  embed_url character varying,
  stream_url character varying,
  duration_seconds integer,
  published_at timestamp with time zone,
  thumbnails json,
  metadata json,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  media_id bigint,
  storage_disk character varying,
  storage_path character varying,
  provider_item_id character varying,
  video_game_source_id bigint,
  provider_payload json,
  canonical_media_id bigint,
  CONSTRAINT game_videos_pkey PRIMARY KEY (id),
  CONSTRAINT game_videos_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id),
  CONSTRAINT game_videos_canonical_media_id_fkey FOREIGN KEY (canonical_media_id) REFERENCES public.canonical_media(id)
);
CREATE TABLE public.jurisdictions (
  id bigint NOT NULL DEFAULT nextval('jurisdictions_id_seq'::regclass),
  country_id bigint NOT NULL,
  region_code text,
  region_key text DEFAULT COALESCE(region_code, ''::text),
  CONSTRAINT jurisdictions_pkey PRIMARY KEY (id),
  CONSTRAINT jurisdictions_country_id_fkey FOREIGN KEY (country_id) REFERENCES public.countries(id)
);
CREATE TABLE public.legacy_import_checkpoints (
  source text NOT NULL,
  last_legacy_id bigint NOT NULL,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT legacy_import_checkpoints_pkey PRIMARY KEY (source)
);
CREATE TABLE public.platform_hardware_map (
  platform_id bigint NOT NULL,
  hardware_product_id bigint UNIQUE,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT platform_hardware_map_pkey PRIMARY KEY (platform_id),
  CONSTRAINT platform_hardware_map_platform_id_fkey FOREIGN KEY (platform_id) REFERENCES public.platforms(id)
);
CREATE TABLE public.platform_merge_audit (
  audit_id bigint NOT NULL DEFAULT nextval('platform_merge_audit_audit_id_seq'::regclass),
  old_id bigint NOT NULL UNIQUE,
  new_id bigint NOT NULL,
  old_code text NOT NULL,
  new_code text NOT NULL,
  merged_rows bigint,
  merged_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT platform_merge_audit_pkey PRIMARY KEY (audit_id)
);
CREATE TABLE public.platforms (
  id bigint NOT NULL DEFAULT nextval('platforms_id_seq'::regclass),
  code text UNIQUE,
  name text NOT NULL UNIQUE,
  family text,
  canonical_code text,
  search_tsv tsvector,
  CONSTRAINT platforms_pkey PRIMARY KEY (id)
);
CREATE TABLE public.platforms_backup (
  backup_id bigint NOT NULL DEFAULT nextval('platforms_backup_backup_id_seq'::regclass),
  original_id bigint NOT NULL,
  name text NOT NULL,
  code text,
  canonical_code text,
  backed_up_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT platforms_backup_pkey PRIMARY KEY (backup_id)
);
CREATE TABLE public.platforms_dedupe_map (
  dupe_id bigint NOT NULL,
  canonical_id bigint NOT NULL,
  deduped_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT platforms_dedupe_map_pkey PRIMARY KEY (dupe_id)
);
CREATE TABLE public.prices (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_01 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_01_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_02 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_02_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_03 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_03_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_04 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_04_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_05 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_05_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_06 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_06_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_07 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_07_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_08 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_08_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_09 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_09_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_10 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_10_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_11 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_11_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2025_12 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2025_12_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_01 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_01_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_02 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_02_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_03 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_03_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_04 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_04_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_05 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_05_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_06 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_06_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.prices_2026_07 (
  id bigint NOT NULL DEFAULT nextval('prices_id_seq'::regclass),
  offer_jurisdiction_id bigint NOT NULL,
  provider_item_id bigint,
  recorded_at timestamp with time zone NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  CONSTRAINT prices_2026_07_pkey PRIMARY KEY (id, recorded_at),
  CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id)
);
CREATE TABLE public.products (
  id bigint NOT NULL DEFAULT nextval('products_id_seq'::regclass),
  slug USER-DEFINED NOT NULL UNIQUE,
  name text NOT NULL,
  category text NOT NULL DEFAULT 'software'::text CHECK (category = ANY (ARRAY['software'::text, 'hardware'::text])),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  software_children_count integer NOT NULL DEFAULT 0,
  hardware_children_count integer NOT NULL DEFAULT 0,
  search_tsv tsvector,
  CONSTRAINT products_pkey PRIMARY KEY (id)
);
CREATE TABLE public.provider_items (
  id bigint NOT NULL DEFAULT nextval('provider_items_id_seq'::regclass),
  provider_id bigint NOT NULL,
  external_id text,
  kind text,
  metadata jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  locked_by text,
  locked_at timestamp with time zone,
  updated_at timestamp with time zone,
  CONSTRAINT provider_items_pkey PRIMARY KEY (id),
  CONSTRAINT provider_items_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id)
);
CREATE TABLE public.provider_media_links (
  id bigint NOT NULL DEFAULT nextval('provider_media_links_id_seq'::regclass),
  provider_item_id bigint NOT NULL,
  media_id bigint,
  url text,
  video_game_id bigint,
  media_type text,
  title text,
  role text,
  source text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  kind USER-DEFINED NOT NULL DEFAULT 'image'::media_kind,
  provider_id bigint,
  CONSTRAINT provider_media_links_pkey PRIMARY KEY (id),
  CONSTRAINT provider_media_links_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id),
  CONSTRAINT provider_media_links_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id),
  CONSTRAINT provider_media_links_provider_fk FOREIGN KEY (provider_id) REFERENCES public.providers(id)
);
CREATE TABLE public.provider_usage_unified (
  provider character varying NOT NULL,
  total_calls bigint NOT NULL DEFAULT 0,
  daily_calls integer NOT NULL DEFAULT 0,
  daily_window date,
  last_called_at timestamp without time zone,
  breakdown jsonb DEFAULT '{}'::jsonb,
  stats jsonb DEFAULT '{}'::jsonb,
  created_at timestamp without time zone DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  CONSTRAINT provider_usage_unified_pkey PRIMARY KEY (provider)
);
CREATE TABLE public.providers (
  id bigint NOT NULL DEFAULT nextval('providers_id_seq'::regclass),
  slug USER-DEFINED UNIQUE,
  name text NOT NULL,
  kind text,
  api_source text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone,
  legacy_source_id bigint,
  search_tsv tsvector,
  CONSTRAINT providers_pkey PRIMARY KEY (id)
);
CREATE TABLE public.retailer_credentials_audit (
  id bigint NOT NULL DEFAULT nextval('retailer_credentials_audit_id_seq'::regclass),
  retailer_provider_id bigint NOT NULL,
  changed_by text,
  changed_at timestamp with time zone NOT NULL DEFAULT now(),
  operation text NOT NULL,
  cred_hash text,
  CONSTRAINT retailer_credentials_audit_pkey PRIMARY KEY (id)
);
CREATE TABLE public.retailer_providers (
  id bigint NOT NULL DEFAULT nextval('retailer_providers_id_seq'::regclass),
  retailer_id bigint NOT NULL,
  provider_id bigint NOT NULL,
  credentials_enc bytea,
  updated_at timestamp with time zone,
  CONSTRAINT retailer_providers_pkey PRIMARY KEY (id),
  CONSTRAINT retailer_providers_retailer_id_fkey FOREIGN KEY (retailer_id) REFERENCES public.retailers(id),
  CONSTRAINT retailer_providers_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id)
);
CREATE TABLE public.retailer_video_game_sources (
  id bigint NOT NULL DEFAULT nextval('retailer_video_game_sources_id_seq'::regclass),
  retailer_id bigint NOT NULL,
  video_game_source_id bigint NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  credentials jsonb NOT NULL DEFAULT '{}'::jsonb,
  settings jsonb NOT NULL DEFAULT '{}'::jsonb,
  jurisdiction_scope ARRAY DEFAULT '{}'::text[],
  last_synced_at timestamp with time zone,
  next_sync_at timestamp with time zone,
  sync_status text,
  sync_cursor text,
  sync_error jsonb,
  rate_limit_per_minute integer,
  rate_limit_burst integer,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_enabled boolean NOT NULL DEFAULT true,
  priority smallint NOT NULL DEFAULT 100,
  CONSTRAINT retailer_video_game_sources_pkey PRIMARY KEY (id),
  CONSTRAINT retailer_video_game_sources_retailer_id_fkey FOREIGN KEY (retailer_id) REFERENCES public.retailers(id),
  CONSTRAINT retailer_video_game_sources_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id)
);
CREATE TABLE public.retailers (
  id bigint NOT NULL DEFAULT nextval('retailers_id_seq'::regclass),
  slug USER-DEFINED UNIQUE,
  name text NOT NULL,
  search_tsv tsvector,
  CONSTRAINT retailers_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
  id bigint NOT NULL DEFAULT nextval('users_id_seq'::regclass),
  email USER-DEFINED NOT NULL UNIQUE,
  name text,
  timezone text NOT NULL DEFAULT 'UTC'::text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  auth_id uuid,
  CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.vg_source_media_links (
  id bigint NOT NULL DEFAULT nextval('vg_source_media_links_id_seq'::regclass),
  video_game_source_id bigint NOT NULL,
  media_id bigint,
  url text,
  video_game_id bigint,
  media_type text,
  title text,
  role text,
  source text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT vg_source_media_links_pkey PRIMARY KEY (id),
  CONSTRAINT vg_source_media_links_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id),
  CONSTRAINT vg_source_media_links_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
);
CREATE TABLE public.video_game_source_sync_states (
  id bigint NOT NULL DEFAULT nextval('video_game_source_sync_states_id_seq'::regclass),
  video_game_source_id bigint NOT NULL,
  retailer_video_game_source_id bigint,
  sync_kind text NOT NULL,
  last_synced_at timestamp with time zone,
  next_sync_at timestamp with time zone,
  sync_status text NOT NULL DEFAULT 'pending'::text,
  sync_details jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_details jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT video_game_source_sync_states_pkey PRIMARY KEY (id),
  CONSTRAINT video_game_source_sync_states_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id),
  CONSTRAINT video_game_source_sync_states_retailer_video_game_source_i_fkey FOREIGN KEY (retailer_video_game_source_id) REFERENCES public.retailer_video_game_sources(id)
);
CREATE TABLE public.video_game_sources (
  id bigint NOT NULL DEFAULT nextval('video_game_sources_id_seq'::regclass),
  video_game_id bigint,
  provider character varying,
  provider_game_id character varying,
  provider_slug character varying,
  provider_hash character varying,
  payload json,
  links json,
  media json,
  synced_at timestamp with time zone,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  provider_key character varying NOT NULL,
  display_name character varying,
  category character varying,
  slug character varying,
  metadata json,
  kind text,
  base_url text,
  website_url text,
  docs_url text,
  auth_mode text,
  auth_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_active boolean NOT NULL DEFAULT true,
  rate_limit_per_minute integer,
  rate_limit_burst integer,
  video_game_ids jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(video_game_ids) = 'array'::text),
  CONSTRAINT video_game_sources_pkey PRIMARY KEY (id),
  CONSTRAINT video_game_sources_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
);
CREATE TABLE public.video_game_title_dedupe_audit (
  winner_id bigint NOT NULL,
  loser_id bigint NOT NULL,
  loser_video_game_id bigint,
  loser_title text,
  loser_normalized_title text,
  loser_created_at timestamp with time zone,
  logged_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT video_game_title_dedupe_audit_pkey PRIMARY KEY (winner_id, loser_id),
  CONSTRAINT video_game_title_dedupe_audit_winner_id_fkey FOREIGN KEY (winner_id) REFERENCES public.video_game_titles(id)
);
CREATE TABLE public.video_game_titles (
  id bigint NOT NULL DEFAULT nextval('video_game_titles_id_seq'::regclass),
  product_id bigint NOT NULL,
  video_game_id bigint,
  title text NOT NULL,
  normalized_title text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  search_vector tsvector DEFAULT to_tsvector('english'::regconfig, ((COALESCE(title, ''::text) || ' '::text) || COALESCE(normalized_title, ''::text))),
  video_game_ids jsonb CHECK (video_game_ids IS NULL OR jsonb_typeof(video_game_ids) = 'array'::text),
  search_tsv tsvector,
  source_ids jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_ids) = 'array'::text),
  aliases jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(aliases) = 'array'::text),
  trailer_url text,
  gameplay_url text,
  CONSTRAINT video_game_titles_pkey PRIMARY KEY (id),
  CONSTRAINT video_game_titles_product_id_fk FOREIGN KEY (product_id) REFERENCES public.products(id)
);
CREATE TABLE public.video_games (
  id bigint NOT NULL DEFAULT nextval('video_games_id_seq'::regclass),
  title_id bigint NOT NULL,
  platform_id bigint NOT NULL,
  edition text,
  sellable_id bigint,
  slug USER-DEFINED UNIQUE,
  release_date date,
  metadata jsonb,
  average_rating real,
  rating_count bigint,
  rating_updated_at timestamp with time zone,
  genres ARRAY,
  display_title text,
  developer text,
  region_codes ARRAY,
  popularity_score numeric NOT NULL DEFAULT 0,
  rating numeric NOT NULL DEFAULT 0,
  synopsis text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  regional_prices jsonb NOT NULL DEFAULT '[]'::jsonb,
  search_tsv tsvector,
  CONSTRAINT video_games_pkey PRIMARY KEY (id),
  CONSTRAINT video_games_title_id_fk FOREIGN KEY (title_id) REFERENCES public.video_game_titles(id),
  CONSTRAINT video_games_platform_id_fkey FOREIGN KEY (platform_id) REFERENCES public.platforms(id)
);
CREATE TABLE public.video_games_dedupe_audit (
  winner_id bigint NOT NULL,
  loser_id bigint NOT NULL,
  loser_title_id bigint,
  loser_platform_id bigint,
  loser_edition text,
  loser_created_at timestamp with time zone,
  logged_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT video_games_dedupe_audit_pkey PRIMARY KEY (winner_id, loser_id),
  CONSTRAINT video_games_dedupe_audit_winner_id_fkey FOREIGN KEY (winner_id) REFERENCES public.video_games(id)
);