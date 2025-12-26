Verification notes for squashed migrations

- Created table: public.video_game_title_sources with uniques on (provider_item_id) and (title_id, provider_item_id).
- Created view: public.video_games_enriched_vw joining titles and platforms.
- Confirmed unique constraints:
  - provider_items (provider_id, external_item_id)
  - provider_offers (provider_item_id, offer_id)
  - video_game_ratings_by_locale (video_game_id, locale)
- Provider media links: ensured indexes uq(provider_item_id, url) and (video_game_id, media_type) present; trigger updates updated_at.

Next checks (optional):
- EXPLAIN representative reads using video_games_enriched_vw.
- Validate title source mappings during ingestion.
