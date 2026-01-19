# Provider Enhancement Implementation Plan

## Overview
This document outlines the complete enhancement plan for all data providers based on comprehensive API audits. The goal is to maximize metadata completeness while implementing smart media filtering.

---

## Media Extraction Strategy (FINAL)

### Priority Order
1. **Background images** (HIGHEST PRIORITY - large/high quality)
2. **Videos** (YouTube preferred, NO podcasts)
   - ✅ Trailers
   - ✅ Gameplay
   - ❌ Podcasts/Interviews
3. **Covers** (box art, cover art)
4. **Artwork** (key art, promotional art)
5. **Banners**
6. **Screenshots** (SMART LIMITS)
   - Max 3 when video present
   - Max 6 when no video
7. **Thumbnails**

### Exclude
- ❌ Logos
- ❌ Icons
- ❌ Podcast videos

---

## Smart Screenshot Limiting Algorithm

```rust
async fn limit_screenshots(
    screenshots: Vec<MediaItem>,
    has_video: bool,
) -> Vec<MediaItem> {
    let limit = if has_video { 3 } else { 6 };

    screenshots
        .into_iter()
        .take(limit)
        .collect()
}
```

**Implementation Steps:**
1. Extract all videos first
2. Check if `videos.len() > 0`
3. Limit screenshots accordingly
4. Process in order: backgrounds → videos → covers → artwork → screenshots → thumbnails

---

## Podcast Filtering for Videos

```rust
fn is_podcast_video(title: &str, video_type: Option<&str>) -> bool {
    let title_lower = title.to_lowercase();
    let keywords = [
        "podcast", "interview", "discussion", "talk show",
        "developer diary", "behind the scenes", "making of"
    ];

    // Check title
    if keywords.iter().any(|k| title_lower.contains(k)) {
        return true;
    }

    // Check video type if available
    if let Some(vt) = video_type {
        let vt_lower = vt.to_lowercase();
        if vt_lower.contains("podcast") || vt_lower.contains("interview") {
            return true;
        }
    }

    false
}
```

---

## Provider-Specific Implementation Tasks

### 1. PlayStation Store

**Synopsis Extraction:**
- ✅ COMPLETE - Already extracting `longDesc` and `description`

**Video Extraction:**
- ❌ ISSUE: Videos mixed with images, not classified
- **Fix:** Lines 185-203 in `src/database_ops/playstation/ingest_demo.rs`
  ```rust
  // BEFORE (mixed):
  all_urls.extend(item.media_image_urls.iter().cloned());
  all_urls.extend(item.media_video_urls.iter().cloned());

  // AFTER (separated):
  // Images
  for img_url in &item.media_image_urls {
      let role = classify_ps_image_role(img_url);
      if role.is_none() { continue; } // Skip logos/icons
      media_images.push((img_url, "image", role));
  }

  // Videos (NEW)
  for video in &item.media_videos {
      if let Some(url) = &video.url {
          let title = video.role.as_deref().unwrap_or("");
          if is_podcast_video(title, None) { continue; }

          let role = if title.contains("trailer") { "trailer" }
                     else { "gameplay" };
          media_videos.push((url, "video", role, title));
      }
  }
  ```

**Screenshot Limiting:**
- Implement smart limiting based on video presence

**YouTube Extraction:**
- Check PlayStation API for YouTube video IDs
- Prioritize YouTube URLs over direct video URLs

---

### 2. Steam

**Synopsis Extraction:**
- ✅ COMPLETE - Extracts all three fields (short, about, detailed)

**Video Extraction:**
- ✅ Extracting direct video URLs
- ❌ MISSING: YouTube support

**Fix:** Lines 1934-1956 in `src/database_ops/steam/provider.rs`
```rust
// Add before extracting WebM/MP4:
if let Some(id) = m.get("id").and_then(|v| v.as_str()) {
    // Check if this is a YouTube ID
    if id.len() == 11 && id.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        let youtube_url = format!("https://www.youtube.com/watch?v={}", id);
        out.push(youtube_url);
        continue; // Skip direct URLs if YouTube available
    }
}

// Also check for explicit youtube_id field
if let Some(yt_id) = m.get("youtube_id").and_then(|v| v.as_str()) {
    let youtube_url = format!("https://www.youtube.com/watch?v={}", yt_id);
    out.push(youtube_url);
    continue;
}
```

**Screenshot Limiting:**
- Current: Extracting ALL screenshots (lines 1883-1898)
- Fix: Implement smart limiting

---

### 3. IGDB

**Synopsis Extraction:**
- ❌ MISSING: Both `summary` and `storyline` fields
- **Fix:** Lines 106-113 in `src/database_ops/igdb/ingest.rs`
  ```rust
  let synopsis = obj.get("summary")
      .or_else(|| obj.get("storyline"))
      .and_then(|v| v.as_str())
      .filter(|s| !s.is_empty())
      .map(|s| s.to_string());

  let meta = VideoGameProductMetadata {
      title: &name,
      synopsis: synopsis.as_deref(),
      // ... other fields
  };
  ```

**Video Extraction:**
- ✅ EXCELLENT - Already extracting YouTube URLs
- ✅ Already classifying trailer vs gameplay

**Screenshot Limiting:**
- Current: Extracting ALL screenshots (lines 162-197)
- Fix: Implement smart limiting based on video presence

---

### 4. GiantBomb

**Synopsis Extraction:**
- ❌ MISSING: `deck` field
- **Fix:** Add to ingestion at lines 160-166
  ```rust
  let synopsis = obj.get("deck")
      .and_then(|v| v.as_str())
      .map(|s| s.to_string());

  let meta = VideoGameProductMetadata {
      title: &name,
      synopsis: synopsis.as_deref(),
      // ... other fields
  };
  ```

**Video Extraction:**
- ✅ Extracting direct video URLs
- ❌ MISSING: YouTube support
- **Fix:** Check API response for `youtube_id` field

**Screenshot Limiting:**
- Current: Extracting all "gallery" images (lines 283-327)
- Fix: Implement smart limiting

---

### 5. RAWG

**Critical Issues:**
1. ❌ Using OLD `ensure_video_game_for_product` helper
2. ❌ NO synopsis extraction (need to verify API)
3. ⚠️ Only extracting PRIMARY video clip

**Migration to Laravel Helper:**
```rust
// REPLACE lines 311, 388, 762, 847
// FROM:
let vg_id = ensure_video_game_for_product(&db, product_id, ...).await?;

// TO:
let vg_id = entity_cache
    .ensure_video_game_for_product_laravel(
        product_id,
        &name,
        slug.as_deref(),
        Some(metadata_json),
    )
    .await?;
```

**Synopsis Extraction:**
- Research RAWG API for `description` or `description_raw` fields
- Add to `RawgGameDetail` struct
- Extract during ingestion

**Video Extraction:**
- Current: Only `detail.clip.clip` (single URL)
- Fix: Extract ALL clips from `clips` HashMap
  ```rust
  // REPLACE lines 596-611
  if let Some(clip_obj) = &detail.clip {
      // Extract primary clip
      if let Some(url) = &clip_obj.clip {
          videos.push((url, "video", "trailer"));
      }

      // Extract ALL additional clips
      if let Some(clips_map) = &clip_obj.clips {
          for (quality, url) in clips_map {
              // Check if YouTube URL
              if url.contains("youtube.com") || url.contains("youtu.be") {
                  videos.push((url, "video", "trailer"));
              }
          }
      }
  }
  ```

**Screenshot Limiting:**
- Current: Max 8 screenshots via `RAWG_MAX_SCREENSHOTS`
- Fix: Replace with smart limiting (3/6 based on video)

---

### 6. TheGamesDB (TGDB)

**Critical Issues:**
1. ❌ NOT using Laravel-compatible helpers
2. ❌ NO media extraction at all
3. ❌ NO synopsis extraction

**Migration to Laravel Pattern:**
```rust
// REPLACE lines 257-258
// FROM:
let title_id = ensure_video_game_title(&db, product_id, ...).await?;
let vg_id = ensure_video_game(&db, ...).await?;

// TO:
let vg_id = entity_cache
    .ensure_video_game_for_product_laravel(
        product_id,
        &name,
        Some(&slug),
        Some(metadata_json),
    )
    .await?;
```

**Synopsis Extraction:**
- Research TGDB API for `overview` or `description` fields
- Add to extraction

**Media Extraction (NEW):**
- Extract boxart (covers)
- Extract fanart (backgrounds) - PRIORITIZE
- Extract banners
- Implement screenshot limiting
- Skip logos/icons

**Video Extraction (NEW):**
- Research TGDB API for video fields
- Implement if available

---

### 7. Nexarda

**Synopsis Extraction:**
- ❌ NO description fields found
- Research Nexarda v3 API documentation
- Add if available

**Video Extraction:**
- ❌ NO video extraction
- Research API for video fields

**Current Status:**
- ✅ Extracting cover and banner (minimal but clean)

---

### 8. Xbox/Microsoft Store

**Synopsis Extraction:**
- ✅ COMPLETE - Extracting all three description fields

**Video Extraction:**
- ❌ NONE - Critical gap
- **Add:** Research Xbox Game Pass API for video fields
- Likely has trailers and gameplay videos

**Media Extraction:**
- Current: Only first image as cover
- Enhance: Add backgrounds, artwork, limited screenshots

---

### 9. ITAD (IsThereAnyDeal)

**Critical Issue:**
- ❌ NO ingestion logic - only a provider client

**Action Required:**
- Decide: Build full ingestion OR keep as client-only
- ITAD is primarily a price aggregator
- May not need full catalog ingestion

---

## Implementation Priority

### Phase 1: Critical Migrations (Week 1)
1. **RAWG** - Migrate to Laravel helper + fix video extraction
2. **TGDB** - Migrate to Laravel pattern + add media extraction
3. **ITAD** - Decide on ingestion strategy

### Phase 2: Synopsis Gaps (Week 1-2)
4. **IGDB** - Add summary/storyline extraction
5. **GiantBomb** - Add deck extraction
6. **RAWG** - Research and add description fields
7. **TGDB** - Research and add overview
8. **Nexarda** - Research API documentation

### Phase 3: Video Enhancements (Week 2)
9. **Steam** - Add YouTube video ID extraction
10. **PlayStation** - Separate and classify videos properly
11. **GiantBomb** - Add YouTube support
12. **RAWG** - Extract all clips, add YouTube
13. **Xbox** - Add video extraction
14. **All Providers** - Implement podcast filtering

### Phase 4: Screenshot Limiting (Week 2-3)
15. **All Providers** - Implement smart screenshot limiting algorithm
16. **Test** - Verify 3/6 limits work correctly

### Phase 5: Media Prioritization (Week 3)
17. **All Providers** - Ensure backgrounds extracted first
18. **All Providers** - Verify thumbnail extraction
19. **Quality Check** - Ensure no logos/icons captured

---

## Testing Strategy

### Unit Tests
```rust
#[test]
fn test_screenshot_limiting_with_video() {
    let screenshots = vec![/* 10 screenshots */];
    let has_video = true;
    let limited = limit_screenshots(screenshots, has_video);
    assert_eq!(limited.len(), 3);
}

#[test]
fn test_screenshot_limiting_without_video() {
    let screenshots = vec![/* 10 screenshots */];
    let has_video = false;
    let limited = limit_screenshots(screenshots, has_video);
    assert_eq!(limited.len(), 6);
}

#[test]
fn test_podcast_filtering() {
    assert!(is_podcast_video("Developer Podcast Episode 1", None));
    assert!(is_podcast_video("Behind the Scenes Interview", None));
    assert!(!is_podcast_video("Official Gameplay Trailer", None));
}
```

### Integration Tests
1. Run ingestion on sample products
2. Verify synopsis populated for all providers
3. Verify YouTube URLs preferred over direct videos
4. Verify screenshot counts (3 or 6)
5. Verify no podcasts in video list
6. Verify backgrounds prioritized

---

## Success Metrics

### Metadata Completeness
- **Synopsis**: 90%+ of products should have synopsis
- **Videos**: All providers with video APIs should extract videos
- **YouTube**: 70%+ of videos should be YouTube links
- **Screenshots**: Correctly limited to 3 or 6

### Media Quality
- **Backgrounds**: Should be prioritized in media arrays
- **No Logos**: 0 logos/icons in media tables
- **No Podcasts**: 0 podcast videos in media tables

### Performance
- **Batch Operations**: Where applicable (RAWG, TGDB)
- **Rate Limits**: All providers stay within documented limits

---

## Database Queries for Validation

```sql
-- Check synopsis population rate
SELECT
    source,
    COUNT(*) as total,
    COUNT(synopsis) as with_synopsis,
    ROUND(100.0 * COUNT(synopsis) / COUNT(*), 2) as pct
FROM video_games vg
JOIN video_game_titles vgt ON vg.id = vgt.video_game_id
JOIN video_game_title_sources vgts ON vgt.id = vgts.video_game_title_id
GROUP BY source;

-- Check YouTube video rate
SELECT
    source,
    COUNT(*) as total_videos,
    COUNT(*) FILTER (WHERE url LIKE '%youtube.com%' OR url LIKE '%youtu.be%') as youtube_videos,
    ROUND(100.0 * COUNT(*) FILTER (WHERE url LIKE '%youtube.com%') / COUNT(*), 2) as pct_youtube
FROM game_media
WHERE media_type IN ('trailer', 'gameplay', 'video')
GROUP BY source;

-- Check screenshot limits per game
SELECT
    video_game_id,
    COUNT(*) FILTER (WHERE media_type = 'screenshot') as screenshot_count,
    COUNT(*) FILTER (WHERE media_type IN ('trailer', 'gameplay', 'video')) as video_count
FROM game_media
GROUP BY video_game_id
HAVING COUNT(*) FILTER (WHERE media_type = 'screenshot') > 6;
-- Should return 0 rows

-- Check for podcast videos
SELECT *
FROM game_media
WHERE media_type IN ('trailer', 'gameplay', 'video')
  AND (
      provider_data->>'title' ILIKE '%podcast%'
      OR provider_data->>'title' ILIKE '%interview%'
  )
LIMIT 20;
-- Should return 0 rows after filtering
```

---

## Next Steps

1. Review this plan
2. Prioritize which providers to enhance first
3. Start with Phase 1 (Critical Migrations)
4. Implement smart screenshot limiting algorithm
5. Add podcast filtering
6. Test and validate

---

**Document Version:** 1.0
**Last Updated:** 2025-12-27
**Status:** Ready for Implementation
