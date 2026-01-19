
# RAWG Top Games Ingestion

## Overview

The RAWG integration now supports fetching "top games" lists for trending discovery:

1. **Top 20 Games of the Month** - Highest rated games released in the current calendar month
2. **Top 20 by Genre** - Highest rated games in 2025 for specific genres (Action, Sports, Shooter)

## Key Features

### Retailer Tracking
- **Stores Captured**: Steam, Epic Games, GOG, Nintendo Store, itch.io, etc.
- **Minimal Missing Marker**: Games without retailer data are flagged with `"no_retailers": true` in metadata
- **Database Integration**: Retailer info is stored in the `retailers` table and linked via provider metadata

### Data Storage
Retailer information is stored in the provider item metadata JSON:
```json
{
  "stores": [
    {
      "retailer_id": 1,
      "retailer_slug": "steam",
      "name": "Steam",
      "url": "https://store.steampowered.com/...",
      "domain": "steampowered.com"
    }
  ],
  "no_retailers": true  // Only present when NO retailers found
}
```

## Usage

### Environment Variables

```bash
# Required
RAWG_API_KEY=your_api_key_here

# Optional
RAWG_REQS_PER_MIN=30          # Rate limit (default: 30)
RAWG_MODE=top_monthly          # Mode selection (see below)
```

### Modes

Set `RAWG_MODE` to control ingestion behavior:

#### 1. Top Monthly (Current Month)
```bash
RAWG_MODE=top_monthly cargo run --bin ingest -- --only rawg
```
Fetches the top 20 highest-rated games released in December 2025 (current month).

#### 2. Top by Genre
```bash
RAWG_MODE=top_genres cargo run --bin ingest -- --only rawg
```
Fetches top 20 games in 2025 for:
- **Action** (genre_id: 4)
- **Sports** (genre_id: 15)
- **Shooter** (genre_id: 2)

#### 3. All Top Lists
```bash
RAWG_MODE=all cargo run --bin ingest -- --only rawg
```
Runs both monthly AND genre ingestion.

#### 4. Range Mode (Default)
```bash
# No RAWG_MODE or RAWG_MODE=range
RAWG_YEAR_MIN=2020 RAWG_YEAR_MAX=2025 cargo run --bin ingest -- --only rawg
```
Original behavior: fetches all games in year range.

## API Endpoints Used

### Top Monthly
```
GET https://api.rawg.io/api/games?dates=2025-12-01,2025-12-31&ordering=-rating&page_size=20&key={API_KEY}
```

### Top by Genre (Action)
```
GET https://api.rawg.io/api/games?dates=2025-01-01,2025-12-31&ordering=-rating&page_size=20&genres=4&key={API_KEY}
```

## Database Schema Impact

### Tables Affected
1. **`products`** - Game products created
2. **`software`** - Software entries linked to products
3. **`video_game_titles`** - Title registry
4. **`video_games`** - Platform-specific game entries
5. **`retailers`** - Retailer/store entries (Steam, Epic, etc.)
6. **`provider_items`** - RAWG provider metadata (includes retailer info)
7. **`provider_media_links`** - Media (screenshots, videos, covers)

### Retailer Lookup

Games **without** retailer data:
```sql
SELECT
  vg.id,
  vgt.normalized_title,
  pi.metadata->>'no_retailers' as missing_retailers
FROM video_games vg
JOIN video_game_titles vgt ON vg.title_id = vgt.id
JOIN provider_items pi ON pi.metadata->>'rawg_numeric' = vg.metadata->'rawg_numeric'
WHERE pi.metadata->>'no_retailers' = 'true';
```

Games **with** retailers:
```sql
SELECT
  vgt.normalized_title,
  jsonb_array_elements(pi.metadata->'stores')->>'name' as retailer_name,
  jsonb_array_elements(pi.metadata->'stores')->>'url' as store_url
FROM provider_items pi
JOIN video_game_titles vgt ON pi.metadata->>'rawg_slug' = vgt.slug
WHERE pi.metadata ? 'stores';
```

## Example Output

```bash
$ RAWG_MODE=top_monthly cargo run --bin ingest -- --only rawg

INFO rawg: RAWG ingesting top 20 games of the month date_start="2025-12-01" date_end="2025-12-31"
INFO rawg: RAWG top games fetched date_start="2025-12-01" date_end="2025-12-31" genre=null ordering="-rating" count=20
INFO ingest: RAWG finished provider="rawg" status="ok" elapsed_ms=8432 note="rawg sync completed" touched=0
```

## Notes

- **No Pricing Data**: RAWG API only provides retailer availability, not actual prices
- **Detail Fetch**: Top games mode ALWAYS fetches full game details to get retailer info (slower but complete)
- **Rate Limiting**: Respects `RAWG_REQS_PER_MIN` (default 30 req/min)
- **FPS Note**: "First-Person Shooter" is typically a tag in RAWG, not a genre. "Shooter" (genre_id: 2) is used instead.

## Genre IDs Reference

| Genre | ID | Description |
|-------|------|-------------|
| Action | 4 | 191,340+ games |
| Shooter | 2 | Includes FPS |
| Sports | 15 | Sports games |
| Indie | 51 | 86,138+ games |
| Adventure | 3 | 151,533+ games |
| RPG | 5 | 61,864+ games |
| Strategy | 10 | Strategy games |
| Platformer | 83 | Platform games |

## Troubleshooting

### "RAWG top games request failed: 401"
- Check `RAWG_API_KEY` is set correctly
- Verify API key at https://rawg.io/apidocs

### "no_retailers: true" for all games
- Some games may not have store data in RAWG
- Check game details on rawg.io to verify
- This is expected for unreleased or indie games

### Rate limit errors (429)
- Increase sleep time: `RAWG_REQS_PER_MIN=15`
- Use lower page sizes: `RAWG_PAGE_SIZE=10`
