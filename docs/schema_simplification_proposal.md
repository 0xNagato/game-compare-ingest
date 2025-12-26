# GameCompare Schema Simplification Proposal — 2025-11-26

## Objectives

- Reduce the active GameCompare relational model from 70+ tables to a concise core that supports ingestion, price tracking, and Laravel 12 APIs without sacrificing key business rules.
- Replace brittle bridge tables (`software`, `hardware`, `sellables`, legacy provider mirrors) with first-class domain entities that map cleanly to Eloquent models and queues.
- Preserve historical pricing (partitioned `prices`) and jurisdiction/tax metadata while simplifying provider integration and alert evaluation.
- Provide a phased migration playbook that can be executed safely in staging before production adoption.

_Source artifacts reviewed: [`docs/schema_map_pre_overhaul.json`](schema_map_pre_overhaul.json) and [`docs/schema_reference.sql`](schema_reference.sql)._ These enumerate 71 tables, of which 50+ lack primary keys and exist purely for legacy syncs, analytics, or deprecated provider pipelines.

## Current Pain Points

1. **Redundant product split** — `software`, `hardware`, `sellables`, `video_game_titles`, `video_games`, and multiple provider pivot tables represent the same commercial entity. This makes ingestion pipelines fan-out across six or more tables for each SKU.
2. **Legacy + staging bloat** — Legacy Steam/PSN/analytics tables without primary keys remain in the public schema, confusing migrations and ORM discovery.
3. **Provider indirection** — `provider_items`, `provider_offers`, `provider_media_links`, and several queue tables fragment the ingestion state. Many could be flattened into a single `provider_mappings` table with status columns.
4. **Laravel ergonomics** — Numerous nullable JSON columns and polymorphic pivots complicate Eloquent casting and resource transformers.

## Target Core Model

The simplified model keeps a lean set of tables designed around product → offer → jurisdiction → price. Everything else is either a lookup (currencies, countries, platforms) or a provider ingestion helper.

```
[products] --1:M--> [product_versions] --1:M--> [offers] --1:M--> [offer_regions] --1:M--> [prices]
     |                                 |                                  |
     |                                 |                                  +--> [current_prices]
     |                                 +--> [provider_items] --1:M--> [provider_runs]
     +--> [media] (Spatie)

[countries] 1:M [jurisdictions]  --+            [users] 1:M [alerts]
[currencies] ---------------------> |            [exchange_rates] (FX snapshots)
[tax_rules] 1:M [jurisdictions] ---+
```

### Table Retention / Consolidation

- **Keep (renamed or adjusted)**: `products`, `platforms`, `retailers`, `offers`, `offer_jurisdictions`, `prices`, `current_price`, `currencies`, `countries`, `jurisdictions`, `tax_rules`, `exchange_rates`, `users`, `alerts`, `media`.
- **Merge into new entities**:
  - `software`, `hardware`, `sellables` → folded into `products.kind` enum and `product_versions`.
  - `video_game_titles`, `video_games`, `game_consoles` → consolidated as `product_versions` with optional `edition` + `form_factor` columns.
  - `provider_*` tables → collapsed to `provider_items` + `provider_runs` (status + payload).
  - `provider_media_links`, `game_images`, `game_videos` → unify to Spatie `media` collections; retain `provider_resource_id` in `media` custom properties.
- **Archive (move to `legacy` schema or drop after export)**: analytics/session tables, `legacy_*`, Steam-specific sync tables, PSN media staging, queue tables (`provider_price_queue*`, `provider_sample_prices`), snapshot views (`price_latest`, `offer_jurisdictions_latest`, etc.). Export the data to S3 before dropping.

## Simplified DDL (PostgreSQL)

> _Idempotent guard: wrap each definition in `DO $$ BEGIN ... EXCEPTION WHEN duplicate_object THEN NULL; END $$` during actual migration._

```sql
create type product_kind as enum ('software', 'hardware');

create table products (
  id             bigserial primary key,
  kind           product_kind not null,
  slug           citext not null unique,
  name           text not null,
  short_name     text,
  metadata       jsonb,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now()
);

create table product_versions (
  id             bigserial primary key,
  product_id     bigint not null references products(id) on delete cascade,
  platform_id    bigint references platforms(id) on delete set null,
  edition        text,
  form_factor    text,
  release_date   date,
  metadata       jsonb,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now(),
  unique (product_id, coalesce(platform_id, 0), coalesce(edition, ''))
);

create table retailers (
  id         bigserial primary key,
  slug       citext not null unique,
  name       text not null,
  metadata   jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table offers (
  id                bigserial primary key,
  product_version_id bigint not null references product_versions(id) on delete cascade,
  retailer_id       bigint not null references retailers(id) on delete cascade,
  sku               text,
  is_active         boolean not null default true,
  metadata          jsonb,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now(),
  unique (product_version_id, retailer_id, coalesce(sku, ''))
);

create table offer_regions (
  id               bigserial primary key,
  offer_id         bigint not null references offers(id) on delete cascade,
  jurisdiction_id  bigint not null references jurisdictions(id) on delete cascade,
  currency_id      bigint not null references currencies(id),
  tax_rule_id      bigint references tax_rules(id) on delete set null,
  metadata         jsonb,
  unique (offer_id, jurisdiction_id)
);

create table prices (
  id                     bigserial,
  offer_region_id        bigint not null references offer_regions(id) on delete cascade,
  recorded_at            timestamptz not null,
  amount_minor           bigint not null check (amount_minor >= 0),
  tax_inclusive          boolean not null,
  fx_minor_per_unit      bigint,
  btc_sats_per_unit      bigint,
  meta                   jsonb,
  primary key (id, recorded_at)
) partition by range (recorded_at);

create table current_prices (
  offer_region_id bigint primary key references offer_regions(id) on delete cascade,
  amount_minor    bigint not null,
  recorded_at     timestamptz not null
);

create table providers (
  id         bigserial primary key,
  code       citext not null unique,
  name       text not null,
  kind       text not null check (kind in ('retailer_api','catalog','media','pricing_api')),
  metadata   jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table provider_items (
  id                   bigserial primary key,
  provider_id          bigint not null references providers(id) on delete cascade,
  external_id          text not null,
  product_version_id   bigint references product_versions(id) on delete set null,
  offer_id             bigint references offers(id) on delete set null,
  last_synced_at       timestamptz,
  payload_hash         text,
  metadata             jsonb,
  unique (provider_id, external_id)
);

create table provider_runs (
  id              bigserial primary key,
  provider_id     bigint not null references providers(id) on delete cascade,
  started_at      timestamptz not null default now(),
  finished_at     timestamptz,
  status          text not null check (status in ('queued','running','succeeded','failed','partial')),
  item_count      integer,
  error_summary   jsonb
);

-- Alerts unchanged apart from enforcing fk to offer_regions for precise targeting

alter table alerts
  add column offer_region_id bigint,
  add foreign key (offer_region_id) references offer_regions(id) on delete cascade;
```

Partition helper (`ensure_prices_partition_for`) from the existing schema can be retained with minimal adjustments.

## Phased Migration Skeleton

### Phase 0 — Preparation (non-destructive)

```sql
-- 0.1 Add product_kind and populate
alter type product_kind add value if not exists 'hardware'; -- guard
alter table products add column if not exists kind product_kind;
update products p
set kind = case
  when exists (select 1 from hardware h where h.product_id = p.id) then 'hardware'
  else 'software'
end
where kind is null;

-- 0.2 Create new tables (product_versions, offer_regions, provider_runs)
-- Use the DDL above wrapped in DO $$ blocks.

-- 0.3 Seed product_versions from video_games + video_game_titles
insert into product_versions (product_id, platform_id, edition, form_factor, release_date, metadata)
select vg.title_id, vg.platform_id, vg.edition, gc.model, vg.release_date, vg.metadata
from video_games vg
left join game_consoles gc on gc.product_id = vg.title_id
on conflict do nothing;
```

### Phase 1 — Offer & Jurisdiction Backfill

```sql
insert into offers (product_version_id, retailer_id, sku, is_active, metadata)
select pv.id, o.retailer_id, o.sku, o.is_active, o.metadata
from offers o
join sellables s on s.id = o.sellable_id
join product_versions pv on pv.product_id = s.software_title_id -- adjust for hardware
on conflict do nothing;

insert into offer_regions (offer_id, jurisdiction_id, currency_id, tax_rule_id, metadata)
select o_new.id, oj.jurisdiction_id, oj.currency_id, tr.id, oj.metadata
from offer_jurisdictions oj
join offers o_old on o_old.id = oj.offer_id
join offers o_new on o_new.product_version_id = o_old.sellable_id -- through mapping table created in phase 0
left join tax_rules tr on tr.jurisdiction_id = oj.jurisdiction_id
on conflict do nothing;

insert into prices (offer_region_id, recorded_at, amount_minor, tax_inclusive, fx_minor_per_unit, btc_sats_per_unit, meta)
select or_new.id, p.recorded_at, p.amount_minor, p.tax_inclusive, p.fx_minor_per_unit, p.btc_sats_per_unit, p.meta
from prices p
join offer_jurisdictions oj on oj.id = p.offer_jurisdiction_id
join offer_regions or_new on or_new.offer_id = oj.offer_id and or_new.jurisdiction_id = oj.jurisdiction_id;

insert into current_prices (offer_region_id, amount_minor, recorded_at)
select or_new.id, cp.amount_minor, cp.recorded_at
from current_price cp
join offer_jurisdictions oj on oj.id = cp.offer_jurisdiction_id
join offer_regions or_new on or_new.offer_id = oj.offer_id and or_new.jurisdiction_id = oj.jurisdiction_id
on conflict (offer_region_id) do update set amount_minor = excluded.amount_minor, recorded_at = excluded.recorded_at;
```

### Phase 2 — Provider Alignment

```sql
insert into providers (code, name, kind, metadata)
select distinct provider_key, provider_key, 'retailer_api', jsonb_build_object('legacy_id', id)
from game_providers
on conflict (code) do nothing;

insert into provider_items (provider_id, external_id, product_version_id, offer_id, last_synced_at, payload_hash, metadata)
select pr.id, pi.external_id, pv.id, o_new.id, pi.updated_at, encode(digest(pi.metadata::text, 'sha256'), 'hex'), pi.metadata
from provider_items pi
join providers pr on pr.code = pi.provider_id::text
left join product_versions pv on pv.product_id = pi.providable_id
left join offers o_new on o_new.product_version_id = pv.id
on conflict (provider_id, external_id) do update set payload_hash = excluded.payload_hash, metadata = excluded.metadata;
```

### Phase 3 — Decommission Legacy Tables

- Verify new tables are serving API reads (update Laravel models/controllers).
- Rename legacy tables with `_legacy2025` suffix (`alter table ... rename`), retain for 30 days.
- Drop staging/queue tables after successful cutover.

## Laravel Alignment

Proposed Eloquent models (namespace `App\Domain`):

| Model | Table | Key Relationships |
| --- | --- | --- |
| `Product` | `products` | `hasMany ProductVersion`, casts: `metadata` json |
| `ProductVersion` | `product_versions` | `belongsTo Product`, `belongsTo Platform`, `hasMany Offer` |
| `Offer` | `offers` | `belongsTo ProductVersion`, `belongsTo Retailer`, `hasMany OfferRegion` |
| `OfferRegion` | `offer_regions` | `belongsTo Offer`, `belongsTo Jurisdiction`, `belongsTo Currency`, `hasOne CurrentPrice`, `hasMany Price` |
| `Price` | `prices` | `belongsTo OfferRegion` (partitioned table — use `withoutTimestamps`) |
| `CurrentPrice` | `current_prices` | `belongsTo OfferRegion` |
| `Provider` | `providers` | `hasMany ProviderItem`, `hasMany ProviderRun` |
| `ProviderItem` | `provider_items` | `belongsTo Provider`, `belongsTo ProductVersion`, `belongsTo Offer` |
| `ProviderRun` | `provider_runs` | `belongsTo Provider` |
| `Alert` | `alerts` | `belongsTo User`, `belongsTo OfferRegion` |

Laravel migration stubs can be generated with `php artisan make:migration` and the SQL above translated using column helpers (`$table->enum`, `$table->foreignId`, `$table->json`, `$table->timestampTz`).

## Operational Considerations

- **Backups**: Take physical backups of `prices`, `offers`, `offer_jurisdictions`, `provider_*`, and `video_game_*` before Phase 1. Retain for 90 days.
- **Runtime impact**: Partitioned inserts remain consistent. Expect additional upserts during backfill — schedule outside ingestion peak.
- **Alert recalculation**: After migrating `alerts.offer_region_id`, re-run alert threshold sync to ensure each alert points to the new region IDs.
- **Testing**: Run Rust ingestion in dry-run mode (set `DRY_RUN=1`) against staging to ensure provider mappings continue to land in `provider_items`.
- **Rollback**: Because legacy tables are renamed (not dropped) until Phase 3, rollback is a matter of re-pointing Laravel models to legacy tables and reversing renames.

## Next Steps

1. Implement Phase 0 + Phase 1 in staging, validate price parity by comparing totals in legacy vs new tables.
2. Update Laravel models/controllers + Rust ingestion to target the new tables (feature flag recommended).
3. Execute Phase 2 provider alignment; monitor provider run metrics.
4. After a week of dual-writing success, proceed with Phase 3 cleanup and drop unused tables.
5. Follow up with updated TypeScript API contracts for the Next.js microsite once new endpoints are in place.

This proposal delivers a lean schema that matches the ingestion flow, reduces Laravel model count, and sets the stage for maintainable migrations going forward.