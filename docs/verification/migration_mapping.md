# Legacy → Squashed Migration Mapping

This maps legacy migration files to consolidated squashed migrations now used.

- 0003_media_tables.sql →
  - migrations_squashed/0360_game_images.sql
  - migrations_squashed/0370_game_videos.sql
  - migrations_squashed/0380_provider_media_links.sql
- 0005_add_video_game_ratings.sql → migrations_squashed/0390_video_game_ratings_by_locale.sql
- 0007_extend_media_tables.sql → migrations_squashed/0360_game_images.sql and 0370_game_videos.sql (merged)
- 0009_video_game_ratings_by_locale.sql → migrations_squashed/0390_video_game_ratings_by_locale.sql
- 0016_extend_game_metadata.sql → migrations_squashed/0340_video_games.sql (enrichment fields)
- 0020_extend_video_game_titles.sql → migrations_squashed/0330_video_game_titles.sql
- 0029_provider_media_and_vg_enhance.sql → migrations_squashed/0340_video_games.sql and 0380_provider_media_links.sql
- 0030_provider_media_enhance.sql → migrations_squashed/0380_provider_media_links.sql

Core objects (for reference):

- 0001_init.sql and early core → migrations_squashed/0200_extensions_enums.sql, 0225_jurisdictions.sql, 0240_products.sql
- Prices partitioning and helpers → migrations_squashed/0290_prices.sql, 0295_current_price.sql
- Alerts → migrations_squashed/0310_alerts.sql
- Platforms/titles/games/consoles → 0320_platforms.sql, 0330_video_game_titles.sql, 0340_video_games.sql, 0350_game_consoles.sql
- Title sources, enriched view → 0400_video_game_title_sources.sql, 0410_video_games_enriched_vw.sql

Notes:

- All squashed files are idempotent and CI-safe (IF NOT EXISTS, guarded DO blocks, comments preserved).
- Concurrency: no CREATE INDEX CONCURRENTLY inside implicit transactions; index creation is fast on current dataset.
- pg_trgm operator class is schema-qualified where used (ext.gin_trgm_ops) in trigram indexes.
