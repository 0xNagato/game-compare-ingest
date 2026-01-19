# RAWG Top Games - Quick Start Guide

## What's New

I've implemented RAWG top games ingestion with retailer tracking and continuous loop support for unified-ingest:

### Features Added

1. **Top 20 Games of the Month** - Current month's highest-rated releases
2. **Top 20 by Genre** - Action, Sports, and Shooter genres for 2025
3. **Retailer Tracking** - Stores retailers (Steam, Epic, GOG, etc.) with `"no_retailers": true` flag when unavailable
4. **Continuous Loop Mode** - Run all providers except Xbox in a loop

---

## Quick Commands

### 1. Fetch Top Games of Current Month
```bash
RAWG_MODE=top_monthly cargo run --bin ingest -- --only rawg
```

### 2. Fetch Top Games by Genre (Action, Sports, Shooter)
```bash
RAWG_MODE=top_genres cargo run --bin ingest -- --only rawg
```

### 3. Fetch All Top Lists (Monthly + Genres)
```bash
RAWG_MODE=all cargo run --bin ingest -- --only rawg
```

### 4. Run All Providers (Except Xbox) - Single Pass
```bash
cargo run --bin ingest -- --skip xbox
```

### 5. Run All Providers (Except Xbox) - Continuous Loop (Every 3600 seconds)
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 3600
```

### 6. Run All Providers (Except Xbox) - Continuous Loop (No Delay)
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 0
```

---

## Retailer Tracking

### How It Works

When fetching games, the system:
1. **Fetches full game details** to get retailer info
2. **Creates retailer entries** in the `retailers` table
3. **Stores retailer data** in provider item metadata:
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
     ]
   }
   ```
4. **Marks missing retailers** with `"no_retailers": true` when unavailable

### Query Games Without Retailers

```sql
SELECT
  vgt.normalized_title,
  pi.metadata->>'rawg_id' as rawg_id,
  pi.metadata->>'no_retailers' as missing
FROM provider_items pi
JOIN video_games vg ON vg.metadata->'rawg_numeric' = pi.metadata->'rawg_numeric'
JOIN video_game_titles vgt ON vg.title_id = vgt.id
WHERE pi.metadata->>'no_retailers' = 'true'
  AND pi.provider_id = (SELECT id FROM providers WHERE slug = 'rawg');
```

---

## Environment Variables

```bash
# Required
RAWG_API_KEY=your_api_key_here

# RAWG Mode (optional, defaults to "range")
RAWG_MODE=top_monthly    # or "top_genres" or "all" or "range"

# Rate Limiting (optional)
RAWG_REQS_PER_MIN=30     # Default: 30 requests per minute

# Year Range (for default "range" mode)
RAWG_YEAR_MIN=2020       # Default: 2015
RAWG_YEAR_MAX=2025       # Default: 2025
```

---

## Continuous Unified Ingest (All Providers Except Xbox)

### Why Skip Xbox?

Based on your request, Xbox is excluded from the loop since it may be unreliable or failing.

### Run Continuous Loop

#### With 1 Hour Delay Between Cycles
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 3600
```

#### With 30 Minute Delay
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 1800
```

#### Continuous (No Delay - High CPU)
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 0
```

### What Runs in Each Loop Iteration?

1. Nexarda
2. GiantBomb
3. PlayStation Store
4. Steam
5. IGDB
6. TheGamesDB (TGDB)
7. RAWG
8. IsThereAnyDeal (ITAD)

**Xbox is SKIPPED** as requested.

### Loop Output Example

```
INFO ingest run start (iteration 1) providers=nexarda,giantbomb,playstation_store,steam,igdb,thegamesdb,rawg,itad iteration=1
INFO Nexarda catalogue finished provider=nexarda status=ok elapsed_ms=8234
INFO GiantBomb JSON finished provider=giantbomb status=ok elapsed_ms=12451
INFO PlayStation Store finished provider=playstation_store status=ok elapsed_ms=45678
INFO Steam finished provider=steam_store status=ok elapsed_ms=67890
INFO IGDB finished provider=igdb status=ok elapsed_ms=23456
INFO TheGamesDB (TGDB) finished provider=thegamesdb status=ok elapsed_ms=8901
INFO RAWG finished provider=rawg status=ok elapsed_ms=5432
INFO IsThereAnyDeal (ITAD) finished provider=itad status=ok elapsed_ms=7890
INFO ingest run complete (iteration 1) total_ms=178932 iteration=1
INFO sleeping before next iteration sleep_secs=3600
```

---

## Advanced Usage

### Run Only Specific Providers in Loop
```bash
cargo run --bin ingest -- --only rawg,steam,playstation --loop-secs 1800
```

### Run Without Alerts
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 3600 --no-alerts
```

### Combine RAWG Mode with Loop
```bash
# Set RAWG to fetch top games, run all providers in loop
RAWG_MODE=all cargo run --bin ingest -- --skip xbox --loop-secs 3600
```

---

## Monitoring the Loop

### View Logs in Real-Time
```bash
cargo run --bin ingest -- --skip xbox --loop-secs 3600 2>&1 | tee ingest_loop.log
```

### Count Iterations
```bash
grep "ingest run complete" ingest_loop.log | wc -l
```

### Check for Failures
```bash
grep "failed" ingest_loop.log
```

---

## Stopping the Loop

**Press `Ctrl+C`** to gracefully stop the loop after the current provider finishes.

---

## Database Impact

### Tables Written To
- `products` - Game products
- `software` - Software entries
- `video_game_titles` - Title registry
- `video_games` - Platform-specific games
- `retailers` - Store/retailer entries (Steam, Epic, GOG, etc.)
- `provider_items` - Provider metadata (includes retailer info)
- `provider_media_links` - Screenshots, covers, trailers

### Checking Retailer Coverage

```sql
-- Count games with retailers
SELECT
  COUNT(*) FILTER (WHERE pi.metadata ? 'stores') as with_retailers,
  COUNT(*) FILTER (WHERE pi.metadata->>'no_retailers' = 'true') as without_retailers,
  COUNT(*) as total
FROM provider_items pi
WHERE pi.provider_id = (SELECT id FROM providers WHERE slug = 'rawg');
```

---

## Troubleshooting

### Rate Limit Errors (429)
```bash
# Reduce requests per minute
RAWG_REQS_PER_MIN=15 cargo run --bin ingest -- --only rawg
```

### API Key Errors (401)
- Verify `RAWG_API_KEY` is set in `.env`
- Get key at: https://rawg.io/apidocs

### Loop Consuming Too Much CPU
```bash
# Increase delay between iterations
cargo run --bin ingest -- --skip xbox --loop-secs 7200  # 2 hours
```

### Provider Keeps Failing
```bash
# Skip the failing provider
cargo run --bin ingest -- --skip xbox,failing_provider --loop-secs 3600
```

---

## Summary

You now have:
1. ✅ **Top games ingestion** (monthly + genres)
2. ✅ **Retailer tracking** with minimal missing markers
3. ✅ **Continuous unified ingest** (all providers except Xbox)
4. ✅ **Configurable loop delays**
5. ✅ **Database persistence** of all game/retailer data

Run your continuous ingest:
```bash
RAWG_MODE=all cargo run --bin ingest -- --skip xbox --loop-secs 3600
```

This will fetch RAWG top lists and run all other providers every hour! 🚀
