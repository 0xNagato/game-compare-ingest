-- =========================================================
-- MEDIA ARCHITECTURE EXTENSION (Massive Scale)
-- =========================================================
-- Designed for 100M+ images/videos with zero FK overhead
-- Hybrid approach: JSONB for hot path + overflow for bulk
-- =========================================================

-- Public-only schema
set search_path to public;

-- =========================================================
-- MEDIA TYPES & ENUMS
-- =========================================================
do $$ begin
create type media_type as enum (
  'cover',           -- Box art / cover image
  'hero',            -- Hero/banner image
  'screenshot',      -- In-game screenshot
  'artwork',         -- Promotional artwork
  'trailer',         -- Video trailer
  'gameplay',        -- Gameplay video
  'logo',            -- Game logo
  'icon'             -- Platform/app icon
);
exception when duplicate_object then null; end $$;

do $$ begin
create type media_source as enum (
  'igdb',
  'giant_bomb',
  'rawg',
  'tgdb',
  'psn',
  'steam',
  'youtube',
  'wikimedia',
  'nexarda',
  'manual'
);
exception when duplicate_object then null; end $$;

-- =========================================================
-- PRIMARY MEDIA (Denormalized JSONB in video_games)
-- =========================================================
-- Add media column to existing video_games table
alter table video_games 
  add column if not exists primary_media jsonb not null default '{
    "cover": null,
    "hero": null,
    "trailer": null,
    "logo": null
  }'::jsonb;

-- GIN index for fast JSONB queries
create index if not exists video_games_primary_media_idx 
  on video_games using gin(primary_media jsonb_path_ops);

-- Example data:
-- {
--   "cover": {"url": "https://cdn.../cover.jpg", "width": 800, "height": 1200},
--   "hero": {"url": "https://cdn.../hero.jpg", "width": 1920, "height": 1080},
--   "trailer": {"url": "https://youtube.com/...", "duration": 120},
--   "logo": {"url": "https://cdn.../logo.png", "width": 512, "height": 512}
-- }

-- =========================================================
-- BULK MEDIA (NO FK, Composite Key, BRIN Indexes)
-- =========================================================
-- For screenshots, artwork, gameplay videos (100M+ rows)
create table if not exists game_media (
  id                bigint generated always as identity,
  
  -- Reference WITHOUT FK constraint (app-level integrity)
  video_game_id     bigint not null,
  
  -- Deduplication keys
  source            media_source not null,
  external_id       text not null,  -- Provider's media ID
  
  -- Media details
  media_type        media_type not null,
  url               text not null check (length(url) > 0),
  cdn_url           text,  -- Cached/optimized version
  
  -- Metadata
  width             integer check (width is null or width > 0),
  height            integer check (height is null or height > 0),
  size_bytes        bigint check (size_bytes is null or size_bytes > 0),
  duration_seconds  integer check (duration_seconds is null or duration_seconds > 0),
  mime_type         text,
  hash              text,  -- For deduplication across providers
  
  -- Provider payload (full response for debugging)
  provider_data     jsonb not null default '{}'::jsonb,
  
  -- Timestamps (append-only, time-ordered)
  created_at        timestamptz not null default now(),
  
  -- Composite PK = natural deduplication
  primary key (video_game_id, source, external_id)
);

-- PERFORMANCE TRICK: BRIN index (1000x smaller than B-tree for time-series)
create index if not exists game_media_created_brin_idx 
  on game_media using brin(created_at) 
  with (pages_per_range = 128);

-- PERFORMANCE TRICK: Partial B-tree for recent lookups (hot data)
create index if not exists game_media_video_game_recent_idx 
  on game_media (video_game_id, media_type) 
  where created_at > now() - interval '1 year';

-- PERFORMANCE TRICK: Hash deduplication index (find duplicates across providers)
create index if not exists game_media_hash_idx 
  on game_media (hash) 
  where hash is not null;

-- PERFORMANCE TRICK: GIN index for provider_data queries
create index if not exists game_media_provider_data_idx 
  on game_media using gin(provider_data jsonb_path_ops);

-- =========================================================
-- MEDIA COLLECTIONS (Group related media)
-- =========================================================
-- For "screenshot galleries", "trailer playlists", etc.
create table if not exists media_collections (
  id             bigint generated always as identity primary key,
  video_game_id  bigint not null,  -- No FK
  name           text not null check (length(name) > 0),
  collection_type text not null,  -- 'screenshots', 'trailers', 'artwork'
  
  -- Media IDs in display order
  media_ids      bigint[] not null default '{}',
  
  -- Metadata
  is_featured    boolean not null default false,
  sort_order     integer not null default 0,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now(),
  
  unique (video_game_id, collection_type, name)
);

create index if not exists media_collections_video_game_idx 
  on media_collections (video_game_id, is_featured);

-- =========================================================
-- MEDIA STATISTICS (Aggregated for performance)
-- =========================================================
-- Materialized view for "media count per game" queries
create materialized view if not exists game_media_stats as
select 
  video_game_id,
  count(*) filter (where media_type in ('screenshot', 'artwork')) as image_count,
  count(*) filter (where media_type in ('trailer', 'gameplay')) as video_count,
  count(distinct source) as source_count,
  max(created_at) as latest_media_at
from game_media
group by video_game_id;

-- Index for fast lookups
create unique index if not exists game_media_stats_video_game_idx 
  on game_media_stats (video_game_id);

-- Refresh strategy (run via cron/scheduler):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.game_media_stats;

-- =========================================================
-- CONSOLE MEDIA (Same pattern for hardware)
-- =========================================================
alter table game_consoles 
  add column if not exists primary_media jsonb not null default '{
    "hero": null,
    "logo": null,
    "icon": null
  }'::jsonb;

create index if not exists game_consoles_primary_media_idx 
  on game_consoles using gin(primary_media jsonb_path_ops);

create table if not exists console_media (
  id                bigint generated always as identity,
  console_id        bigint not null,  -- No FK
  source            media_source not null,
  external_id       text not null,
  media_type        media_type not null,
  url               text not null,
  cdn_url           text,
  width             integer,
  height            integer,
  size_bytes        bigint,
  mime_type         text,
  hash              text,
  provider_data     jsonb not null default '{}'::jsonb,
  created_at        timestamptz not null default now(),
  
  primary key (console_id, source, external_id)
);

create index if not exists console_media_created_brin_idx 
  on console_media using brin(created_at);

create index if not exists console_media_console_recent_idx 
  on console_media (console_id, media_type) 
  where created_at > now() - interval '1 year';

-- =========================================================
-- MEDIA OPTIMIZATION QUEUE (Track CDN/resize jobs)
-- =========================================================
create table if not exists media_optimization_queue (
  id                bigint generated always as identity primary key,
  source_table      text not null,  -- 'game_media', 'console_media'
  source_id         bigint not null,
  original_url      text not null,
  
  -- Optimization status
  status            text not null default 'pending',  -- pending, processing, completed, failed
  cdn_url           text,  -- Result after optimization
  
  -- Optimization details
  target_width      integer,
  target_height     integer,
  target_format     text,  -- webp, avif, jpg
  
  -- Processing metadata
  attempts          integer not null default 0,
  last_error        text,
  processed_at      timestamptz,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

create index if not exists media_optimization_queue_status_idx 
  on media_optimization_queue (status, created_at) 
  where status in ('pending', 'processing');

-- =========================================================
-- ORPHAN CLEANUP (Scheduled job to remove orphaned media)
-- =========================================================
create or replace function cleanup_orphaned_media()
returns table (
  deleted_game_media bigint,
  deleted_console_media bigint
) language plpgsql as $$
declare
  v_game_media_deleted bigint;
  v_console_media_deleted bigint;
begin
  -- Delete game media for non-existent video_games
  with deleted as (
  delete from game_media gm
    where not exists (
  select 1 from video_games vg 
      where vg.id = gm.video_game_id
    )
    and gm.created_at < now() - interval '7 days'  -- Grace period
    returning 1
  )
  select count(*) into v_game_media_deleted from deleted;
  
  -- Delete console media for non-existent consoles
  with deleted as (
  delete from console_media cm
    where not exists (
  select 1 from game_consoles gc 
      where gc.id = cm.console_id
    )
    and cm.created_at < now() - interval '7 days'
    returning 1
  )
  select count(*) into v_console_media_deleted from deleted;
  
  return query select v_game_media_deleted, v_console_media_deleted;
end$$;

-- Run via cron: SELECT * FROM cleanup_orphaned_media();

-- =========================================================
-- HELPER FUNCTIONS
-- =========================================================

-- Get all media for a video game (primary + bulk)
create or replace function get_video_game_media(p_video_game_id bigint)
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'primary', vg.primary_media,
    'screenshots', (
      select jsonb_agg(jsonb_build_object(
        'url', url,
        'cdn_url', cdn_url,
        'width', width,
        'height', height,
        'source', source
      ) order by created_at desc)
  from game_media 
      where video_game_id = p_video_game_id 
        and media_type = 'screenshot'
      limit 20
    ),
    'videos', (
      select jsonb_agg(jsonb_build_object(
        'url', url,
        'duration', duration_seconds,
        'type', media_type,
        'source', source
      ) order by created_at desc)
  from game_media 
      where video_game_id = p_video_game_id 
        and media_type in ('trailer', 'gameplay')
      limit 10
    ),
    'stats', (
      select row_to_json(s)
  from game_media_stats s
      where s.video_game_id = p_video_game_id
    )
  )
  from video_games vg
  where vg.id = p_video_game_id;
$$;

-- Upsert primary media (safe concurrent updates)
create or replace function upsert_primary_media(
  p_video_game_id bigint,
  p_media_type text,  -- 'cover', 'hero', 'trailer', 'logo'
  p_data jsonb
)
returns void language plpgsql as $$
begin
  update video_games
  set 
    primary_media = jsonb_set(
      primary_media, 
      array[p_media_type], 
      p_data,
      true
    ),
    updated_at = now()
  where id = p_video_game_id;
end$$;

-- =========================================================
-- TABLE SETTINGS (Optimize for workload)
-- =========================================================

-- game_media is write-heavy, leave space for HOT updates
alter table game_media set (fillfactor = 90);

-- Aggressive autovacuum (prevents bloat on massive tables)
alter table game_media set (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01
);

-- Stats for better query planning
alter table game_media 
  alter column video_game_id set statistics 1000;

-- =========================================================
-- COMMENTS
-- =========================================================
comment on table game_media is 
  'Bulk media storage (100M+ rows) - NO FK for performance, composite PK for deduplication';
comment on index game_media_created_brin_idx is 
  'BRIN index for time-series (1000x smaller than B-tree)';
comment on index game_media_video_game_recent_idx is 
  'Partial B-tree for recent media only (hot queries)';
comment on function cleanup_orphaned_media is 
  'Scheduled job to remove media for deleted games (run daily)';
comment on materialized view game_media_stats is 
  'Aggregated media counts per game (refresh hourly)';
