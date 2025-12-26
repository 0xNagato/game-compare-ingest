# Legacy Migrations Salvage Summary

Date: 2025-11-09

This document records the consolidation and removal of legacy Laravel-style PHP migrations found under `migrations_old/`.

## Guiding Schema Reference

Consolidation aligned strictly to the master prompt in `.github/instructions/co-pilot.instructions.md` and the applied Postgres schema (`gamecompare`):

Included (already present):

- Core domain tables (currencies, countries, jurisdictions, tax_rules)
- Product split (products, software, hardware, video_game_titles, video_games, game_consoles)
- Commerce layer (sellables, retailers, offers, offer_jurisdictions)
- Provider chain (providers, retailer_providers, provider_items, provider_offers, provider_ingest_runs, provider_media_links)
- Time-series pricing (prices partitioned, current_price)
- Users & alerts
- Media tables (game_images, game_videos) plus optional linking table (provider_media_links)

Excluded (removed):

- GiantBomb-specific tables (giant_bomb_games, giant_bomb_game_images/videos, cross references, payload variations)
- Steam-specific / IGDB / vendor cache & sync state tables
- Generic `product_media` abstractions (superseded by explicit game_images/game_videos + provider_media_links)
- Telescope / jobs / runtime settings / queue infra (out of current scope)
- Aggregation & legacy price series tables (replaced by partitioned `prices` + `current_price` hot path)

Rationale: These legacy structures either duplicated current generalized tables or were source/vendor-specific. Retaining them would increase maintenance surface, diverge from the normalized provider ingestion pipeline, or introduce unused complexity.

## Spatie Media Table

Per design: Spatie `media` table remains untouched. It is not recreated here; integration uses `provider_media_links` when needed. If later required, add a dedicated migration replicating Spatie's canonical schema.

## Idempotency & Clean Slate

All removed migrations are fully superseded by the consolidated SQL migrations in `migrations/0001_init.sql`, `0002_prices_partitions.sql`, and `0003_media_tables.sql`. No data dependencies remain because the database was reset prior to consolidation.

## Next Steps

1. (Optional) Add partial recent index on prices for last 30 days.
2. (Optional) Introduce analytical BRIN index for long-range scans.
3. Seed richer demo data (additional offers, media rows, alerts).
4. Keep this document updated if any legacy table is reconsidered.

## Audit Stamp

Removed all files under `migrations_old/` to prevent accidental re-application.
