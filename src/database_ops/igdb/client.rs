use crate::database_ops::db::Db;
use crate::database_ops::ingest_providers::{
    ProviderEntityCache, ensure_platform, ensure_provider, ensure_provider_media_links_with_meta,
    ingest_run_finish, ingest_run_start, upsert_game_media,
};
use crate::database_ops::media_map::normalize_title;
use anyhow::{Context, Result, anyhow};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument};

#[derive(Debug, Clone)]
pub struct IgdbServiceConfig {
    pub reqs_per_min: Option<u32>,
    pub rps: Option<f32>,
    pub concurrency: usize,
    pub page_size: usize,
    pub max_pages: usize,
    pub max_retries: u32,
    pub backoff_ms: u64,
    pub from_year: Option<i32>,
    pub to_year: Option<i32>,
    pub window_start: Option<NaiveDate>,
    pub window_end: Option<NaiveDate>,
    pub platform_ids: Vec<i32>,
    pub characters_per_game: usize,
}

impl Default for IgdbServiceConfig {
    fn default() -> Self {
        Self {
            reqs_per_min: Some(30),
            rps: None,
            concurrency: 1,
            page_size: 200,
            max_pages: 50,
            max_retries: 5,
            backoff_ms: 1000,
            from_year: None,
            to_year: None,
            window_start: None,
            window_end: None,
            platform_ids: Vec::new(),
            characters_per_game: 3,
        }
    }
}

impl IgdbServiceConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("IGDB_REQS_PER_MIN") {
            if let Ok(n) = v.parse::<u32>() {
                cfg.reqs_per_min = Some(n);
            }
        }
        if let Ok(v) = std::env::var("IGDB_RPS") {
            if let Ok(n) = v.parse::<f32>() {
                cfg.rps = Some(n);
            }
        }
        if let Ok(v) = std::env::var("IGDB_CONCURRENCY") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.concurrency = n.max(1);
            }
        }
        if let Ok(v) = std::env::var("IGDB_PAGE_SIZE") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.page_size = n.max(1);
            }
        }
        if let Ok(v) = std::env::var("IGDB_MAX_PAGES") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.max_pages = n.max(1);
            }
        }
        if let Ok(v) = std::env::var("IGDB_MAX_RETRIES") {
            if let Ok(n) = v.parse::<u32>() {
                cfg.max_retries = n;
            }
        }
        if let Ok(v) = std::env::var("IGDB_BACKOFF_MS") {
            if let Ok(n) = v.parse::<u64>() {
                cfg.backoff_ms = n;
            }
        }
        if let Ok(v) = std::env::var("IGDB_YEAR_START") {
            if let Ok(n) = v.parse::<i32>() {
                cfg.from_year = Some(n);
            }
        }
        if let Ok(v) = std::env::var("IGDB_YEAR_END") {
            if let Ok(n) = v.parse::<i32>() {
                cfg.to_year = Some(n);
            }
        }
        if let Ok(v) = std::env::var("IGDB_WINDOW_START") {
            if let Ok(date) = NaiveDate::parse_from_str(v.trim(), "%Y-%m-%d") {
                cfg.window_start = Some(date);
            }
        }
        if let Ok(v) = std::env::var("IGDB_WINDOW_END") {
            if let Ok(date) = NaiveDate::parse_from_str(v.trim(), "%Y-%m-%d") {
                cfg.window_end = Some(date);
            }
        }
        if let Ok(v) = std::env::var("IGDB_PLATFORM_IDS") {
            let parsed: Vec<i32> = v
                .split(|c| (c == ',' || c == ' '))
                .filter_map(|raw| {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        trimmed.parse::<i32>().ok()
                    }
                })
                .collect();
            if !parsed.is_empty() {
                cfg.platform_ids = parsed;
            }
        }
        if let Ok(v) = std::env::var("IGDB_CHARACTERS_PER_GAME") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.characters_per_game = n.clamp(0, 5);
            }
        }
        cfg
    }

    pub fn sleep_ms_between_requests(&self) -> Option<u64> {
        if let Some(rpm) = self.reqs_per_min {
            if rpm > 0 {
                return Some(60_000u64 / (rpm as u64));
            }
        }
        if let Some(rps) = self.rps {
            if rps > 0.0 {
                return Some((1000f32 / rps) as u64);
            }
        }
        None
    }
}

const TWITCH_TOKEN_URL: &str = "https://id.twitch.tv/oauth2/token";
const IGDB_GAMES_ENDPOINT: &str = "https://api.igdb.com/v4/games";
const IGDB_CHARACTERS_ENDPOINT: &str = "https://api.igdb.com/v4/characters";
const IGDB_IMAGE_BASE: &str = "https://images.igdb.com/igdb/image/upload";
const IGDB_COVER_SIZES: &[&str] = &[
    "t_cover_small",
    "t_cover_big",
    "t_cover_big_2x",
    "t_original",
];
const IGDB_SCREENSHOT_SIZES: &[&str] = &[
    "t_screenshot_med",
    "t_screenshot_big",
    "t_screenshot_huge",
    "t_original",
];
const IGDB_MAX_LIMIT: usize = 500;
const IGDB_CHARACTER_BATCH: usize = 500;
const IGDB_MAX_SCREENSHOTS: usize = 6;
const IGDB_PROVIDER_NAME: &str = "IGDB";
const IGDB_PROVIDER_KIND: &str = "catalog";
const IGDB_PROVIDER_SLUG: &str = "igdb";
const IGDB_FALLBACK_PLATFORM_NAME: &str = "Unknown Platform (IGDB)";
const IGDB_FALLBACK_PLATFORM_SLUG: &str = "igdb-generic";

#[derive(Debug, Clone, Deserialize)]
struct IgdbImage {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    image_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgdbVideo {
    #[serde(default)]
    video_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgdbPlatform {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    slug: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgdbReleaseDate {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    date: Option<i64>,
    #[serde(default)]
    platform: Option<i64>,
    #[serde(default)]
    region: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgdbGame {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    storyline: Option<String>,
    #[serde(default)]
    first_release_date: Option<i64>,
    #[serde(default)]
    genres: Option<Vec<i64>>,
    #[serde(default)]
    platforms: Option<Vec<IgdbPlatform>>,
    #[serde(default)]
    cover: Option<IgdbImage>,
    #[serde(default)]
    screenshots: Option<Vec<IgdbImage>>,
    #[serde(default)]
    videos: Option<Vec<IgdbVideo>>,
    #[serde(default)]
    release_dates: Option<Vec<IgdbReleaseDate>>,
    #[serde(default)]
    characters: Option<Vec<i64>>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgdbCharacter {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    games: Option<Vec<i64>>,
}

#[derive(Debug, Clone)]
struct CharacterSummary {
    id: i64,
    name: Option<String>,
    slug: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TwitchTokenResponse {
    access_token: String,
    expires_in: u64,
    #[allow(dead_code)]
    token_type: String,
}

#[derive(Debug, Clone)]
struct IgdbToken {
    access_token: String,
    expires_at: Instant,
}

pub struct IgdbService {
    cfg: IgdbServiceConfig,
    http: Client,
    token: Arc<Mutex<Option<IgdbToken>>>,
}

impl IgdbService {
    pub fn new_from_env() -> Result<Self> {
        let cfg = IgdbServiceConfig::from_env();
        let user_agent = std::env::var("IGDB_USER_AGENT")
            .unwrap_or_else(|_| "gamecompare-igdb-ingest/1.0".to_string());
        let http = Client::builder()
            .user_agent(user_agent)
            .build()
            .context("failed to construct IGDB HTTP client")?;
        Ok(Self {
            cfg,
            http,
            token: Arc::new(Mutex::new(None)),
        })
    }

    fn effective_page_size(&self, override_size: usize) -> usize {
        let base = if override_size > 0 {
            override_size
        } else {
            self.cfg.page_size
        };
        base.clamp(1, IGDB_MAX_LIMIT)
    }

    fn effective_max_pages(&self, override_pages: usize) -> usize {
        if override_pages > 0 {
            override_pages
        } else {
            self.cfg.max_pages.max(1)
        }
    }

    async fn throttle(&self) {
        if let Some(ms) = self.cfg.sleep_ms_between_requests() {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }

    async fn ensure_token(&self) -> Result<String> {
        {
            let guard = self.token.lock().await;
            if let Some(token) = guard.as_ref() {
                if token.expires_at > Instant::now() + Duration::from_secs(30) {
                    return Ok(token.access_token.clone());
                }
            }
        }
        let token = self.request_new_token().await?;
        let mut guard = self.token.lock().await;
        *guard = Some(token.clone());
        Ok(token.access_token)
    }

    async fn request_new_token(&self) -> Result<IgdbToken> {
        let client_id =
            std::env::var("TWITCH_CLIENT_ID").context("TWITCH_CLIENT_ID missing for IGDB")?;
        let client_secret = std::env::var("TWITCH_CLIENT_SECRET")
            .context("TWITCH_CLIENT_SECRET missing for IGDB")?;
        let response = self
            .http
            .post(TWITCH_TOKEN_URL)
            .query(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .context("requesting Twitch OAuth token")?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "twitch token request failed (status={}): {}",
                status,
                text
            ));
        }
        let token: TwitchTokenResponse = response.json().await?;
        let ttl = token.expires_in.saturating_sub(30).max(30);
        Ok(IgdbToken {
            access_token: token.access_token,
            expires_at: Instant::now() + Duration::from_secs(ttl),
        })
    }

    async fn execute_request<T>(&self, endpoint: &str, body: String) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let client_id =
            std::env::var("TWITCH_CLIENT_ID").context("TWITCH_CLIENT_ID missing for IGDB")?;
        let mut attempt = 0u32;
        loop {
            let token = self.ensure_token().await?;
            let response = self
                .http
                .post(endpoint)
                .header("Client-ID", &client_id)
                .header("Content-Type", "text/plain")
                .header("Authorization", format!("Bearer {}", token))
                .body(body.clone())
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let text = resp.text().await?;
                        let parsed: Vec<T> = serde_json::from_str(&text).map_err(|err| {
                            anyhow!("failed to parse IGDB payload ({err}): {text}")
                        })?;
                        return Ok(parsed);
                    }

                    if status == StatusCode::UNAUTHORIZED {
                        let mut guard = self.token.lock().await;
                        *guard = None;
                    }

                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        if attempt >= self.cfg.max_retries {
                            let text = resp.text().await.unwrap_or_default();
                            return Err(anyhow!(
                                "igdb request failed after retries (status={}): {}",
                                status,
                                text
                            ));
                        }
                        let wait = self.cfg.backoff_ms * ((attempt + 1) as u64);
                        tokio::time::sleep(Duration::from_millis(wait)).await;
                        attempt += 1;
                        continue;
                    }

                    let text = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("igdb request failed (status={}): {}", status, text));
                }
                Err(err) => {
                    if attempt >= self.cfg.max_retries {
                        return Err(err.into());
                    }
                    let wait = self.cfg.backoff_ms * ((attempt + 1) as u64);
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    attempt += 1;
                    continue;
                }
            }
        }
    }

    async fn fetch_games(&self, body: &str) -> Result<Vec<IgdbGame>> {
        self.execute_request(IGDB_GAMES_ENDPOINT, body.to_string())
            .await
    }

    async fn fetch_characters_for_games(
        &self,
        games: &[IgdbGame],
    ) -> Result<HashMap<i64, Vec<CharacterSummary>>> {
        if self.cfg.characters_per_game == 0 {
            return Ok(HashMap::new());
        }
        let mut unique_ids: Vec<i64> = games
            .iter()
            .filter_map(|g| g.characters.clone())
            .flatten()
            .collect();
        unique_ids.sort_unstable();
        unique_ids.dedup();
        if unique_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let target_game_ids: HashSet<i64> = games.iter().filter_map(|g| g.id).collect();
        let mut map: HashMap<i64, Vec<CharacterSummary>> = HashMap::new();
        for chunk in unique_ids.chunks(IGDB_CHARACTER_BATCH) {
            let list = chunk
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let body = format!(
                "fields id,name,slug,games; where id = ({list}); limit {};",
                chunk.len()
            );
            let records: Vec<IgdbCharacter> =
                self.execute_request(IGDB_CHARACTERS_ENDPOINT, body).await?;
            for character in records {
                let char_id = match character.id {
                    Some(id) => id,
                    None => {
                        continue;
                    }
                };
                let summary = CharacterSummary {
                    id: char_id,
                    name: character.name.clone(),
                    slug: character.slug.clone(),
                };
                if let Some(gids) = character.games {
                    for gid in gids {
                        if target_game_ids.contains(&gid) {
                            map.entry(gid).or_default().push(summary.clone());
                        }
                    }
                }
            }
            self.throttle().await;
        }
        for entry in map.values_mut() {
            entry.sort_by_key(|c| c.id);
            entry.dedup_by_key(|c| c.id);
            entry.truncate(self.cfg.characters_per_game);
        }
        Ok(map)
    }

    async fn persist_games(
        &self,
        db: &Db,
        provider_id: i64,
        cache: &mut ProviderEntityCache,
        games: &[IgdbGame],
        characters: &HashMap<i64, Vec<CharacterSummary>>,
    ) -> Result<usize> {
        let mut processed = 0usize;
        for game in games {
            if let Some(id) = game.id {
                if self
                    .persist_single_game(db, provider_id, cache, game, characters.get(&id))
                    .await?
                {
                    processed += 1;
                }
            }
        }
        Ok(processed)
    }

    async fn persist_single_game(
        &self,
        db: &Db,
        provider_id: i64,
        cache: &mut ProviderEntityCache,
        game: &IgdbGame,
        characters: Option<&Vec<CharacterSummary>>,
    ) -> Result<bool> {
        let igdb_id = match game.id {
            Some(id) => id,
            None => {
                return Ok(false);
            }
        };
        let name = game.name.as_deref().unwrap_or("Untitled IGDB Game");
        let slug = match game.slug.as_deref() {
            Some(slug) if !slug.is_empty() => slug.to_string(),
            _ => normalize_title(name),
        };
        let product_slug = format!("igdb-{}", igdb_id);
        let product_id = cache
            .ensure_product_named("software", &product_slug, name)
            .await?;
        cache.ensure_software_row(product_id).await?;
        let title_id = cache
            .ensure_video_game_title(product_id, name, Some(&slug))
            .await?;
        let metadata = self.build_provider_metadata(game, characters);
        let provider_item_id = cache
            .ensure_provider_item(
                provider_id,
                &igdb_id.to_string(),
                Some(metadata.clone()),
                true,
            )
            .await?;
        let platforms = self.extract_platforms(game);
        if platforms.is_empty() {
            let platform_id = ensure_platform(
                db,
                IGDB_FALLBACK_PLATFORM_NAME,
                Some(IGDB_FALLBACK_PLATFORM_SLUG),
            )
            .await?;
            let video_game_id = cache.ensure_video_game(title_id, platform_id, None).await?;
            self.persist_media_records(db, provider_item_id, video_game_id, igdb_id, game)
                .await?;
            return Ok(true);
        }
        for platform in platforms {
            let platform_name = platform
                .name
                .as_deref()
                .unwrap_or(IGDB_FALLBACK_PLATFORM_NAME);
            let platform_slug = platform
                .slug
                .as_deref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| normalize_title(platform_name));
            let platform_id = ensure_platform(db, platform_name, Some(&platform_slug)).await?;
            let video_game_id = cache.ensure_video_game(title_id, platform_id, None).await?;
            self.persist_media_records(db, provider_item_id, video_game_id, igdb_id, game)
                .await?;
        }
        Ok(true)
    }

    async fn persist_media_records(
        &self,
        db: &Db,
        provider_item_id: i64,
        video_game_id: i64,
        igdb_id: i64,
        game: &IgdbGame,
    ) -> Result<()> {
        if let Some(cover) = game.cover.as_ref() {
            let variants = Self::image_variant_urls(cover, IGDB_COVER_SIZES);
            if !variants.is_empty() {
                let provider_entries: Vec<_> = variants
                    .iter()
                    .map(|(url, size)| {
                        (
                            url.clone(),
                            Some("image".into()),
                            Some(format!("cover:{}", size).into()),
                            None,
                        )
                    })
                    .collect();
                ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
                    Some(video_game_id),
                    &provider_entries,
                    IGDB_PROVIDER_SLUG,
                    None,
                )
                .await?;
                for (url, size) in variants {
                    let meta = json!({
                        "asset": "cover",
                        "igdb_id": igdb_id,
                        "image_id": cover.image_id,
                        "size": size,
                    });
                    upsert_game_media(
                        db,
                        video_game_id,
                        IGDB_PROVIDER_SLUG,
                        &format!("{}-cover-{}", igdb_id, size),
                        "cover",
                        &url,
                        meta,
                    )
                    .await?;
                }
            }
        }
        if let Some(shots) = game.screenshots.as_ref() {
            for (idx, shot) in shots.iter().take(IGDB_MAX_SCREENSHOTS).enumerate() {
                let variants = Self::image_variant_urls(shot, IGDB_SCREENSHOT_SIZES);
                if variants.is_empty() {
                    continue;
                }
                let provider_entries: Vec<_> = variants
                    .iter()
                    .map(|(url, size)| {
                        (
                            url.clone(),
                            Some("image".into()),
                            Some(format!("screenshot:{}:{}", idx, size).into()),
                            None,
                        )
                    })
                    .collect();
                ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
                    Some(video_game_id),
                    &provider_entries,
                    IGDB_PROVIDER_SLUG,
                    None,
                )
                .await?;
                for (url, size) in variants {
                    let meta = json!({
                        "asset": "screenshot",
                        "index": idx,
                        "igdb_id": igdb_id,
                        "image_id": shot.image_id,
                        "size": size,
                    });
                    upsert_game_media(
                        db,
                        video_game_id,
                        IGDB_PROVIDER_SLUG,
                        &format!("{}-screenshot-{}-{}", igdb_id, idx, size),
                        "screenshot",
                        &url,
                        meta,
                    )
                    .await?;
                }
            }
        }
        if let Some(videos) = game.videos.as_ref() {
            for (idx, video) in videos.iter().enumerate() {
                if let Some(video_id) = video.video_id.as_ref() {
                    let normalized_media_type =
                        Self::classify_video_media_type(video.name.as_deref());
                    let media_kind = normalized_media_type.to_string();
                    let variants = Self::video_variant_urls(video_id);
                    let provider_entries: Vec<_> = variants
                        .iter()
                        .map(|(url, variant, _quality)| {
                            (
                                url.clone(),
                                Some(media_kind.clone()),
                                Some(format!("trailer:{}", variant).into()),
                                None,
                            )
                        })
                        .collect();
                    ensure_provider_media_links_with_meta(
                        db,
                        provider_item_id,
                        Some(video_game_id),
                        &provider_entries,
                        IGDB_PROVIDER_SLUG,
                        None,
                    )
                    .await?;
                    for (url, variant, quality) in variants {
                        let meta = json!({
                            "asset": "video",
                            "index": idx,
                            "igdb_id": igdb_id,
                            "video_id": video_id,
                            "name": video.name,
                            "variant": variant,
                            "quality": quality,
                        });
                        upsert_game_media(
                            db,
                            video_game_id,
                            IGDB_PROVIDER_SLUG,
                            &format!("{}-video-{}-{}", igdb_id, idx, variant),
                            normalized_media_type,
                            &url,
                            meta,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }

    fn build_provider_metadata(
        &self,
        game: &IgdbGame,
        characters: Option<&Vec<CharacterSummary>>,
    ) -> Value {
        let release_dates: Vec<Value> = game
            .release_dates
            .as_ref()
            .map(|dates| {
                dates
                    .iter()
                    .filter_map(|rd| {
                        rd.id.map(|id| {
                            json!({
                                "id": id,
                                "date": rd.date,
                                "platform": rd.platform,
                                "region": rd.region,
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let platforms: Vec<Value> = game
            .platforms
            .as_ref()
            .map(|plats| {
                plats
                    .iter()
                    .map(|p| json!({ "id": p.id, "name": p.name, "slug": p.slug }))
                    .collect()
            })
            .unwrap_or_default();
        let character_values: Vec<Value> = characters
            .map(|chars| {
                chars
                    .iter()
                    .map(|c| json!({ "id": c.id, "name": c.name, "slug": c.slug }))
                    .collect()
            })
            .unwrap_or_default();
        json!({
            "igdb": {
                "id": game.id,
                "name": game.name,
                "slug": game.slug,
                "summary": game.summary,
                "storyline": game.storyline,
                "first_release_date": game.first_release_date,
                "genres": game.genres,
                "platforms": platforms,
                "release_dates": release_dates,
                "characters": character_values,
            },
            "ingested_at": Utc::now(),
        })
    }

    fn extract_platforms(&self, game: &IgdbGame) -> Vec<IgdbPlatform> {
        game.platforms
            .as_ref()
            .map(|plats| plats.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn build_games_query(
        &self,
        start_epoch: Option<i64>,
        end_epoch: Option<i64>,
        platforms: &[i32],
        offset: usize,
        limit: usize,
    ) -> String {
        let mut filters: Vec<String> = Vec::new();
        if let Some(start) = start_epoch {
            filters.push(format!("first_release_date >= {}", start));
        }
        if let Some(end) = end_epoch {
            filters.push(format!("first_release_date <= {}", end));
        }
        if !platforms.is_empty() {
            let ids = platforms
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            filters.push(format!("platforms = ({})", ids));
        }
        let where_clause = if filters.is_empty() {
            String::new()
        } else {
            format!("where {};", filters.join(" & "))
        };
        format!(
            "fields id,name,slug,summary,storyline,first_release_date,genres,platforms.id,platforms.name,platforms.slug,cover.image_id,cover.url,screenshots.image_id,screenshots.url,videos.video_id,videos.name,release_dates.id,release_dates.date,release_dates.platform,release_dates.region; {where_clause} sort first_release_date desc; limit {limit}; offset {offset};",
            limit = limit.min(IGDB_MAX_LIMIT),
            offset = offset
        )
    }

    fn resolve_release_bounds(
        &self,
        from_year: Option<i32>,
        to_year: Option<i32>,
    ) -> (Option<i64>, Option<i64>) {
        let start = if let Some(date) = self.cfg.window_start {
            Self::naive_to_epoch(date.and_hms_opt(0, 0, 0))
        } else if let Some(year) = from_year {
            Self::naive_to_epoch(
                NaiveDate::from_ymd_opt(year, 1, 1).and_then(|d| d.and_hms_opt(0, 0, 0)),
            )
        } else {
            None
        };
        let end = if let Some(date) = self.cfg.window_end {
            Self::naive_to_epoch(date.and_hms_opt(23, 59, 59))
        } else if let Some(year) = to_year {
            Self::naive_to_epoch(
                NaiveDate::from_ymd_opt(year, 12, 31).and_then(|d| d.and_hms_opt(23, 59, 59)),
            )
        } else {
            None
        };
        (start, end)
    }

    fn naive_to_epoch(naive: Option<NaiveDateTime>) -> Option<i64> {
        naive.map(|dt| Utc.from_utc_datetime(&dt).timestamp())
    }

    fn build_image_url(image: &IgdbImage, size: &str) -> Option<String> {
        if let Some(image_id) = image.image_id.as_ref() {
            return Some(format!("{}/{}/{}.jpg", IGDB_IMAGE_BASE, size, image_id));
        }
        image.url.as_ref().map(|raw| Self::normalize_url(raw))
    }

    fn normalize_url(raw: &str) -> String {
        if raw.starts_with("//") {
            format!("https:{}", raw)
        } else {
            raw.to_string()
        }
    }

    fn youtube_url(video_id: &str) -> String {
        format!("https://www.youtube.com/watch?v={}", video_id)
    }

    fn youtube_hd_embed_url(video_id: &str) -> String {
        format!(
            "https://www.youtube.com/embed/{}?vq=hd1080&autoplay=0",
            video_id
        )
    }

    fn image_variant_urls(
        image: &IgdbImage,
        sizes: &[&'static str],
    ) -> Vec<(String, &'static str)> {
        sizes
            .iter()
            .filter_map(|size| Self::build_image_url(image, size).map(|url| (url, *size)))
            .collect()
    }

    fn video_variant_urls(video_id: &str) -> Vec<(String, &'static str, &'static str)> {
        vec![
            (Self::youtube_url(video_id), "watch", "standard"),
            (Self::youtube_hd_embed_url(video_id), "embed_hd", "hd"),
        ]
    }

    fn classify_video_media_type(name: Option<&str>) -> &'static str {
        if let Some(name) = name {
            let lowered = name.to_ascii_lowercase();
            if lowered.contains("preview") {
                return "preview";
            }
            if lowered.contains("gameplay") || lowered.contains("playthrough") {
                return "gameplay";
            }
        }
        "trailer"
    }

    async fn backfill_range_inner(
        &self,
        db: &Db,
        provider_id: i64,
        platforms: Vec<i32>,
        year_from: i32,
        year_to: i32,
        limit: usize,
        max_pages: usize,
    ) -> Result<usize> {
        let (start_epoch, end_epoch) = self.resolve_release_bounds(Some(year_from), Some(year_to));
        let mut cache = ProviderEntityCache::new(db.clone());
        let mut total = 0usize;
        let mut page = 0usize;
        let mut offset = 0usize;
        loop {
            if page >= max_pages {
                break;
            }
            let query = self.build_games_query(start_epoch, end_epoch, &platforms, offset, limit);
            let games = self.fetch_games(&query).await?;
            if games.is_empty() {
                break;
            }
            let characters = self.fetch_characters_for_games(&games).await?;
            let processed = self
                .persist_games(db, provider_id, &mut cache, &games, &characters)
                .await?;
            total += processed;
            page += 1;
            offset += limit;
            self.throttle().await;
            debug!(
                target = "igdb",
                page, processed, total, "igdb page ingested"
            );
            if games.len() < limit {
                break;
            }
        }
        Ok(total)
    }

    #[instrument(skip(self, db))]
    pub async fn backfill_range(
        &self,
        db: &Db,
        y_from: i32,
        y_to: i32,
        platforms: &[i32],
        page_size_override: usize,
        max_pages_override: usize,
    ) -> Result<usize> {
        let provider_id = ensure_provider(
            db,
            IGDB_PROVIDER_NAME,
            IGDB_PROVIDER_KIND,
            Some(IGDB_PROVIDER_SLUG),
        )
        .await?;
        let platform_filter = if !platforms.is_empty() {
            platforms.to_vec()
        } else if !self.cfg.platform_ids.is_empty() {
            self.cfg.platform_ids.clone()
        } else {
            Vec::new()
        };
        let from_year = self.cfg.from_year.unwrap_or(y_from);
        let to_year = self.cfg.to_year.unwrap_or(y_to);
        let meta = json!({
            "from_year": from_year,
            "to_year": to_year,
            "platform_ids": platform_filter,
        });
        let ingest_id = ingest_run_start(db, provider_id, None, Some(meta)).await?;
        let limit = self.effective_page_size(page_size_override);
        let max_pages = self.effective_max_pages(max_pages_override);
        let result = self
            .backfill_range_inner(
                db,
                provider_id,
                platform_filter.clone(),
                from_year,
                to_year,
                limit,
                max_pages,
            )
            .await;
        match &result {
            Ok(count) => {
                ingest_run_finish(db, ingest_id, "completed", *count as i64, 0, None).await?;
            }
            Err(err) => {
                ingest_run_finish(
                    db,
                    ingest_id,
                    "failed",
                    0,
                    0,
                    Some(json!({ "error": err.to_string() })),
                )
                .await?;
            }
        }
        log_provider_snapshot(db, provider_id).await?;
        result
    }
}

#[instrument(skip(db))]
async fn log_provider_snapshot(db: &Db, provider_id: i64) -> Result<()> {
    let provider_items = fetch_provider_count(
        db,
        "SELECT COUNT(*)::bigint FROM provider_items WHERE provider_id=$1",
        provider_id,
    )
    .await?;
    let provider_media_links = fetch_provider_count(
        db,
        "SELECT COUNT(*)::bigint FROM provider_media_links WHERE provider_id=$1",
        provider_id,
    )
    .await?;
    info!(
        target = "igdb",
        provider_items, provider_media_links, "IGDB provider snapshot"
    );
    Ok(())
}

async fn fetch_provider_count(db: &Db, sql: &str, provider_id: i64) -> Result<i64> {
    use sqlx::Error;
    match sqlx::query_scalar::<_, i64>(sql)
        .persistent(false)
        .bind(provider_id)
        .fetch_one(&db.pool)
        .await
    {
        Ok(val) => Ok(val),
        Err(Error::ColumnDecode { .. }) => {
            let fallback: i32 = sqlx::query_scalar::<_, i32>(sql)
                .persistent(false)
                .bind(provider_id)
                .fetch_one(&db.pool)
                .await?;
            Ok(fallback as i64)
        }
        Err(e) => Err(e.into()),
    }
}

#[instrument(skip(db))]
pub async fn run_from_env(db: &Db) -> Result<()> {
    let service = IgdbService::new_from_env()?;
    let current_year = Utc::now().year();
    let mut from_year = service
        .cfg
        .from_year
        .unwrap_or(current_year.saturating_sub(1));
    let mut to_year = service.cfg.to_year.unwrap_or(current_year);
    if from_year > to_year {
        std::mem::swap(&mut from_year, &mut to_year);
    }
    let processed = service
        .backfill_range(db, from_year, to_year, &service.cfg.platform_ids, 0, 0)
        .await?;
    info!(
        target = "igdb",
        processed, from_year, to_year, "IGDB ingestion completed"
    );
    Ok(())
}
