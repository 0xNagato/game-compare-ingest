# Xbox Store Browse Discovery Implementation Guide

## Overview

This document describes how to implement comprehensive Xbox Store game discovery using the Browse API, similar to the PlayStation Store year-based pagination approach.

## Xbox Browse API Capabilities

### Endpoint
```
GET https://displaycatalog.mp.microsoft.com/v7.0/products/browse
```

### Key Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `market` | US, GB, JP, etc. | Market/region code |
| `languages` | en-us, ja-jp, etc. | Comma-separated language codes |
| `skipItems` | 0, 100, 200, etc. | Pagination offset |
| `top` | 1-200 | Results per page (max ~200) |
| `categoryId` | Games, Apps, etc. | Category filter |
| `productFamilyNames` | Games | Product family filter |
| `deviceFamily` | Windows.Xbox | Target platform |
| `orderBy` | releaseDate, rank, title | Sort field |
| `sortOrder` | asc, desc | Sort direction |

### Additional Filters (Discovered)

| Parameter | Values | Description |
|-----------|--------|-------------|
| `priceRange` | Free, Under5, Under10, etc. | Price filtering |
| `ageRating` | E, T, M, etc. | ESRB/age rating |
| `genres` | Action, RPG, Sports, etc. | Genre filtering |

## Implementation Strategy

### 1. Year-Based Pagination (Recommended)

Similar to PlayStation Store (`YEAR_MIN`/`YEAR_MAX`):

```rust
// Environment variables
XBOX_YEAR_MIN=2010          // Start year (inclusive)
XBOX_YEAR_MAX=2025          // End year (inclusive)
XBOX_BROWSE_PAGE_SIZE=200   // Results per page
XBOX_BROWSE_MAX_PAGES=100   // Max pages per year
```

**Algorithm:**
1. For each year from YEAR_MAX down to YEAR_MIN (descending):
2. Query browse API with `orderBy=releaseDate&sortOrder=desc`
3. Paginate through results until:
   - Reached end of results (fewer than `top` returned)
   - Found game released before current year (year boundary)
   - Hit max page limit

**Why descending?** Start with newest games (2025 → 2010) to prioritize current catalog.

### 2. Multi-Market Discovery

```bash
XBOX_BROWSE_MARKETS=US,GB,JP,AU,CA,DE,FR
```

**Strategy:**
- Browse each market independently
- Deduplicate product IDs across markets
- Track market-specific availability

### 3. Category-Based Discovery

```bash
XBOX_BROWSE_CATEGORIES=Games  # Or specific: Action,RPG,Sports
```

**Categories available:**
- Games (all games)
- Action
- Adventure
- Puzzle & Trivia
- Role Playing
- Strategy & Simulation
- Sports & Racing
- Shooter
- Platformer
- Fighting
- Music

## Comparison: PlayStation Store vs Xbox Store

| Feature | PlayStation Store | Xbox Store (Browse API) |
|---------|------------------|-------------------------|
| **Year filtering** | ✅ Native (via sort + cutoff) | ⚠️ Manual (detect year boundary) |
| **Pagination** | ✅ `offset` parameter | ✅ `skipItems` parameter |
| **Page size** | ✅ 100-200 | ✅ 100-200 |
| **Sort by date** | ✅ `productReleaseDate` | ✅ `orderBy=releaseDate` |
| **Multi-market** | ✅ Region parameter | ✅ `market` parameter |
| **Categories** | ✅ Category IDs | ✅ `categoryId` parameter |

## Implementation Code Outline

```rust
// Add to xbox/provider.rs

pub async fn browse_by_year_range(
    &self,
    db: &Db,
    markets: &[String],
    year_min: i32,
    year_max: i32,
    page_size: usize,
    max_pages_per_year: usize,
) -> Result<Vec<String>> {
    let mut all_product_ids = HashSet::new();

    for market in markets {
        info!(market=%market, "xbox_browse: starting market");

        // Iterate years descending (newest first)
        for year in (year_min..=year_max).rev() {
            let year_product_ids = self.browse_year(
                market,
                year,
                page_size,
                max_pages_per_year
            ).await?;

            info!(
                market=%market,
                year=year,
                products=%year_product_ids.len(),
                "xbox_browse: year complete"
            );

            all_product_ids.extend(year_product_ids);
        }
    }

    Ok(all_product_ids.into_iter().collect())
}

async fn browse_year(
    &self,
    market: &str,
    year: i32,
    page_size: usize,
    max_pages: usize,
) -> Result<Vec<String>> {
    let mut product_ids = Vec::new();
    let language = "en-us"; // or from config
    let ms_cv = "DGU1mcuYo0WMMp+"; // or from config

    let year_start = format!("{}-01-01T00:00:00Z", year);
    let year_end = format!("{}-12-31T23:59:59Z", year);

    let mut page = 0;
    let mut skip = 0;

    loop {
        if page >= max_pages {
            warn!(
                market=%market,
                year=year,
                page=page,
                "xbox_browse: hit max page limit"
            );
            break;
        }

        let url = format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products/browse?\
             market={}&languages={}&skipItems={}&top={}&\
             categoryId=Games&productFamilyNames=Games&\
             deviceFamily=Windows.Xbox&\
             orderBy=releaseDate&sortOrder=desc",
            market, language, skip, page_size
        );

        let mut req = self.client.get(&url).header("MS-CV", ms_cv);
        if let Some(token) = self.auth_token().await? {
            req = req.bearer_auth(token);
        }

        let response = req.send().await?;
        if !response.status().is_success() {
            warn!(
                market=%market,
                year=year,
                page=page,
                status=%response.status(),
                "xbox_browse: request failed"
            );
            break;
        }

        let payload: Value = response.json().await?;
        let page_products = Self::extract_product_ids(&payload);

        if page_products.is_empty() {
            // No more results
            break;
        }

        // Check if we've crossed year boundary
        let mut found_older_year = false;
        if let Some(products) = payload.get("Products").and_then(|v| v.as_array()) {
            for product in products {
                if let Some(date_str) = product
                    .get("LocalizedProperties")
                    .and_then(|v| v.get(0))
                    .and_then(|v| v.get("OriginalReleaseDate"))
                    .and_then(|v| v.as_str())
                {
                    if let Some(release_year) = parse_year_from_date(date_str) {
                        if release_year < year {
                            found_older_year = true;
                            break;
                        }
                    }
                }
            }
        }

        product_ids.extend(page_products.clone());

        if found_older_year {
            info!(
                market=%market,
                year=year,
                page=page,
                "xbox_browse: crossed year boundary, stopping"
            );
            break;
        }

        if page_products.len() < page_size {
            // Last page (partial results)
            break;
        }

        page += 1;
        skip += page_size;

        // Rate limiting
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Ok(product_ids)
}

fn parse_year_from_date(date_str: &str) -> Option<i32> {
    // Parse ISO 8601 date: "2024-03-15T00:00:00Z"
    date_str.split('-').next()?.parse().ok()
}
```

## Environment Configuration

```bash
# Year Range (like PlayStation Store)
XBOX_YEAR_MIN=2010
XBOX_YEAR_MAX=2025

# Browse Settings
XBOX_BROWSE_PAGE_SIZE=200       # Results per page
XBOX_BROWSE_MAX_PAGES=100       # Max pages per year
XBOX_BROWSE_MARKETS=US,GB,JP    # Markets to browse

# Enable comprehensive discovery
XBOX_HARVEST_TOPLISTS=true      # Keep existing toplist discovery
XBOX_ENABLE_BROWSE=true         # Enable new year-based browse
```

## Usage Examples

```bash
# Discover all Xbox games 2020-2025 across 3 markets
XBOX_YEAR_MIN=2020 \
XBOX_YEAR_MAX=2025 \
XBOX_BROWSE_MARKETS=US,GB,JP \
cargo run --bin ingest run --only xbox

# Focus on recent releases (last 2 years)
XBOX_YEAR_MIN=2023 \
XBOX_YEAR_MAX=2025 \
cargo run --bin ingest run --only xbox

# Historical catalog (older games)
XBOX_YEAR_MIN=2010 \
XBOX_YEAR_MAX=2019 \
XBOX_BROWSE_MARKETS=US \
cargo run --bin ingest run --only xbox
```

## Expected Coverage

### Toplist Harvesting (Current)
- ~5,000-10,000 products (top charts, categories, collections)
- Biased toward popular/recent games
- Good for trending price data

### Year-Based Browse (New)
- ~50,000-100,000+ products (comprehensive catalog)
- Systematic coverage by release year
- Captures long-tail and niche titles

### Combined Approach (Recommended)
- Run toplists for quick popular game discovery
- Run browse for comprehensive catalog backfill
- Deduplicate product IDs before ingestion

## Performance Considerations

### API Rate Limiting
- Xbox doesn't publish official rate limits
- Conservative: 5-10 requests/second
- Implement exponential backoff on errors

### Time Estimates
- ~200 products per page
- ~100 pages per year = 20,000 products/year
- ~15 years (2010-2025) = 300,000 products
- At 10 req/s with pagination: ~8-10 hours for full catalog

### Optimization Strategies
1. **Parallel market processing**: Browse US, GB, JP concurrently
2. **Resume capability**: Save progress, resume from last year/page
3. **Incremental updates**: Only browse recent years (2024-2025) for daily updates
4. **Smart deduplication**: Cache product IDs across runs

## Next Steps

1. Implement `browse_by_year_range()` in `xbox/provider.rs`
2. Add environment configuration (`XBOX_YEAR_MIN`, etc.)
3. Integrate into `run_from_env()` alongside toplist harvesting
4. Test with small year range first (e.g., 2024-2025)
5. Monitor API responses for rate limiting
6. Expand to full catalog (2010-2025)

## Additional Discovery Methods

### Search API (Future Enhancement)
```
GET https://displaycatalog.mp.microsoft.com/v7.0/products/search
?query={search_term}
&market={market}
&languages={language}
```

**Use cases:**
- Genre-specific discovery ("action games")
- Publisher catalogs ("EA games")
- Franchise discovery ("Call of Duty")

### Recommendations API (Future Enhancement)
```
GET https://displaycatalog.mp.microsoft.com/v7.0/products/recommendations
```

**Use cases:**
- Related products discovery
- Cross-market recommendations
