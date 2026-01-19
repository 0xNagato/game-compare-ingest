use crate::database_ops::db::Db;
use crate::database_ops::ingest_providers::{
    ensure_platform, ensure_provider, ensure_vg_source_media_links_with_meta,
    ensure_video_game_source, ingest_run_finish, ingest_run_start, replace_provider_toplist_items,
    upsert_game_media, upsert_provider_toplist, ProviderEntityCache,
};
use crate::database_ops::media_map::normalize_title;
use anyhow::{anyhow, Context, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Clone, Copy)]
struct TitleSchemaFlags {
    keyed_by_source_item: bool,
    has_video_game_id: bool,
    has_product_id: bool,
    video_games_is_laravel: bool,
    source_id: Option<i64>,
}

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
const IGDB_GENRES_ENDPOINT: &str = "https://api.igdb.com/v4/genres";
const IGDB_THEMES_ENDPOINT: &str = "https://api.igdb.com/v4/themes";
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
const IGDB_CHARACTER_BATCH: usize = 3;
const IGDB_MAX_SCREENSHOTS: usize = 3;
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
struct IgdbGenre {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    slug: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgdbTheme {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    slug: Option<String>,
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
    total_rating: Option<f32>,
    #[serde(default)]
    total_rating_count: Option<i64>,
    #[serde(default)]
    aggregated_rating: Option<f32>,
    #[serde(default)]
    aggregated_rating_count: Option<i64>,
    #[serde(default)]
    genres: Option<Vec<i64>>,
    #[serde(default)]
    themes: Option<Vec<i64>>,
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

#[derive(Debug, Clone)]
struct GenreAttachContext {
    enabled: bool,
    igdb_genre_id_to_local_genre_id: HashMap<i64, i64>,
    igdb_theme_id_to_local_genre_id: HashMap<i64, i64>,
    game_genre_has_timestamps: bool,
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
        let client_id = std::env::var("TWITCH_CLIENT_ID")
            .context("missing env: TWITCH_CLIENT_ID (required for IGDB)")?;
        let client_secret = std::env::var("TWITCH_CLIENT_SECRET")
            .context("missing env: TWITCH_CLIENT_SECRET (required for IGDB)")?;
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
        let client_id = std::env::var("TWITCH_CLIENT_ID")
            .context("missing env: TWITCH_CLIENT_ID (required for IGDB)")?;
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

    async fn fetch_genres(&self, body: &str) -> Result<Vec<IgdbGenre>> {
        self.execute_request(IGDB_GENRES_ENDPOINT, body.to_string())
            .await
    }

    async fn fetch_all_genres(&self) -> Result<Vec<IgdbGenre>> {
        self.fetch_genres("fields id,name,slug; limit 500;").await
    }

    async fn fetch_themes(&self, body: &str) -> Result<Vec<IgdbTheme>> {
        self.execute_request(IGDB_THEMES_ENDPOINT, body.to_string())
            .await
    }

    async fn fetch_all_themes(&self) -> Result<Vec<IgdbTheme>> {
        self.fetch_themes("fields id,name,slug; limit 500;").await
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
        provider_items_exists: bool,
        provider_media_links_exists: bool,
        title_schema: TitleSchemaFlags,
        genre_attach: Option<&GenreAttachContext>,
    ) -> Result<Vec<i64>> {
        let mut product_ids: Vec<i64> = Vec::new();
        for game in games {
            if let Some(id) = game.id {
                if let Some(product_id) = self
                    .persist_single_game(
                        db,
                        provider_id,
                        cache,
                        game,
                        characters.get(&id),
                        provider_items_exists,
                        provider_media_links_exists,
                        title_schema,
                        genre_attach,
                    )
                    .await?
                {
                    product_ids.push(product_id);
                }
            }
        }
        Ok(product_ids)
    }

    async fn persist_single_game(
        &self,
        db: &Db,
        provider_id: i64,
        cache: &mut ProviderEntityCache,
        game: &IgdbGame,
        characters: Option<&Vec<CharacterSummary>>,
        provider_items_exists: bool,
        provider_media_links_exists: bool,
        title_schema: TitleSchemaFlags,
        genre_attach: Option<&GenreAttachContext>,
    ) -> Result<Option<i64>> {
        let igdb_id = match game.id {
            Some(id) => id,
            None => {
                return Ok(None);
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

        if let Some(ctx) = genre_attach {
            if ctx.enabled {
                let mut local_ids: Vec<i64> = Vec::new();
                if let Some(game_genres) = game.genres.as_ref() {
                    local_ids.extend(game_genres.iter().filter_map(|igdb_genre_id| {
                        ctx.igdb_genre_id_to_local_genre_id
                            .get(igdb_genre_id)
                            .copied()
                    }));
                }
                if let Some(game_themes) = game.themes.as_ref() {
                    local_ids.extend(game_themes.iter().filter_map(|igdb_theme_id| {
                        ctx.igdb_theme_id_to_local_genre_id
                            .get(igdb_theme_id)
                            .copied()
                    }));
                }

                local_ids.sort_unstable();
                local_ids.dedup();
                for genre_id in local_ids {
                    if let Err(err) = attach_genre_to_product(
                        db,
                        product_id,
                        genre_id,
                        ctx.game_genre_has_timestamps,
                    )
                    .await
                    {
                        warn!(
                            target = "igdb",
                            product_id,
                            genre_id,
                            error = %err,
                            "failed to attach genre to product (best-effort)"
                        );
                    }
                }
            }
        }
        let metadata = self.build_provider_metadata(game, characters);

        // Some DBs use a source-registry schema where `video_game_titles` is keyed by
        // (video_game_source_id, video_game_source_id), and may require video_game_id or product_id.
        let (_title_id, title_video_game_id) = if title_schema.keyed_by_source_item {
            let sid = title_schema
                .source_id
                .expect("source_id must exist when titles are keyed by source item");
            let provider_item_key = igdb_id.to_string();
            if title_schema.has_video_game_id {
                if !title_schema.video_games_is_laravel {
                    anyhow::bail!(
                        "video_game_titles.video_game_id is present but video_games is not Laravel-style (missing product_id/title)"
                    );
                }
                let canonical_vg_id = cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        name,
                        Some(&slug),
                        Some(metadata.clone()),
                        IGDB_PROVIDER_SLUG,
                    )
                    .await?;
                let title_id = cache
                    .ensure_video_game_title_for_source_item(
                        sid,
                        &provider_item_key,
                        Some(product_id),
                        Some(canonical_vg_id),
                        name,
                        Some(&slug),
                        None,
                        Some(metadata.clone()),
                    )
                    .await?;
                (title_id, Some(canonical_vg_id))
            } else {
                let legacy_link_id = if title_schema.has_product_id {
                    Some(product_id)
                } else {
                    None
                };
                let title_id = cache
                    .ensure_video_game_title_for_source_item(
                        sid,
                        &provider_item_key,
                        legacy_link_id,
                        None,
                        name,
                        Some(&slug),
                        None,
                        Some(metadata.clone()),
                    )
                    .await?;
                (title_id, None)
            }
        } else {
            let title_id = cache
                .ensure_video_game_title(product_id, name, Some(&slug))
                .await?;
            (title_id, None)
        };

        let video_game_source_id = if provider_items_exists {
            Some(
                cache
                    .ensure_provider_item(
                        provider_id,
                        &igdb_id.to_string(),
                        Some(metadata.clone()),
                        true,
                    )
                    .await?,
            )
        } else {
            None
        };
        let platforms = self.extract_platforms(game);
        if platforms.is_empty() {
            let _platform_id = ensure_platform(
                db,
                IGDB_FALLBACK_PLATFORM_NAME,
                Some(IGDB_FALLBACK_PLATFORM_SLUG),
            )
            .await?;
            // Laravel schema: use product_id directly
            let video_game_id = if let Some(vg_id) = title_video_game_id {
                vg_id
            } else {
                cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        name,
                        Some(&slug),
                            Some(metadata.clone()),
                            IGDB_PROVIDER_SLUG,
                    )
                    .await?
            };
            self.persist_media_records(
                db,
                video_game_source_id,
                provider_media_links_exists,
                video_game_id,
                igdb_id,
                game,
            )
            .await?;
            return Ok(Some(product_id));
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
            let _platform_id = ensure_platform(db, platform_name, Some(&platform_slug)).await?;
            // Laravel schema: use product_id directly
            let video_game_id = if let Some(vg_id) = title_video_game_id {
                vg_id
            } else {
                cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        name,
                        Some(&slug),
                        Some(metadata.clone()),
                        IGDB_PROVIDER_SLUG,
                    )
                    .await?
            };
            self.persist_media_records(
                db,
                video_game_source_id,
                provider_media_links_exists,
                video_game_id,
                igdb_id,
                game,
            )
            .await?;
        }
        Ok(Some(product_id))
    }

    async fn persist_media_records(
        &self,
        db: &Db,
        video_game_source_id: Option<i64>,
        provider_media_links_exists: bool,
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
                if provider_media_links_exists {
                    if let Some(video_game_source_id) = video_game_source_id {
                        ensure_vg_source_media_links_with_meta(
                            db,
                            video_game_source_id,
                            Some(video_game_id),
                            &provider_entries,
                            IGDB_PROVIDER_SLUG,
                            None,
                        )
                        .await?;
                    }
                }
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
                if provider_media_links_exists {
                    if let Some(video_game_source_id) = video_game_source_id {
                        ensure_vg_source_media_links_with_meta(
                            db,
                            video_game_source_id,
                            Some(video_game_id),
                            &provider_entries,
                            IGDB_PROVIDER_SLUG,
                            None,
                        )
                        .await?;
                    }
                }
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
                    if provider_media_links_exists {
                        if let Some(video_game_source_id) = video_game_source_id {
                            ensure_vg_source_media_links_with_meta(
                                db,
                                video_game_source_id,
                                Some(video_game_id),
                                &provider_entries,
                                IGDB_PROVIDER_SLUG,
                                None,
                            )
                            .await?;
                        }
                    }
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
                "total_rating": game.total_rating,
                "total_rating_count": game.total_rating_count,
                "aggregated_rating": game.aggregated_rating,
                "aggregated_rating_count": game.aggregated_rating_count,
                "genres": game.genres,
                "themes": game.themes,
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
            "fields id,name,slug,summary,storyline,first_release_date,total_rating,total_rating_count,aggregated_rating,aggregated_rating_count,genres,themes,platforms.id,platforms.name,platforms.slug,cover.image_id,cover.url,screenshots.image_id,screenshots.url,videos.video_id,videos.name,release_dates.id,release_dates.date,release_dates.platform,release_dates.region; {where_clause} sort first_release_date desc; limit {limit}; offset {offset};",
            limit = limit.min(IGDB_MAX_LIMIT),
            offset = offset
        )
    }

    fn build_top_monthly_games_query(
        &self,
        start_epoch: i64,
        end_epoch: i64,
        allowed_genre_ids: &[i64],
        allowed_theme_ids: &[i64],
        offset: usize,
        limit: usize,
    ) -> String {
        let mut filters: Vec<String> = vec![
            format!("first_release_date >= {}", start_epoch),
            format!("first_release_date <= {}", end_epoch),
        ];

        let mut tag_clauses: Vec<String> = Vec::new();
        if !allowed_genre_ids.is_empty() {
            let ids = allowed_genre_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            tag_clauses.push(format!("genres = ({})", ids));
        }
        if !allowed_theme_ids.is_empty() {
            let ids = allowed_theme_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            tag_clauses.push(format!("themes = ({})", ids));
        }
        if !tag_clauses.is_empty() {
            if tag_clauses.len() == 1 {
                filters.push(tag_clauses[0].clone());
            } else {
                filters.push(format!("({})", tag_clauses.join(" | ")));
            }
        }

        let where_clause = format!("where {};", filters.join(" & "));

        // Sort by engagement first (count), then by rating.
        format!(
            "fields id,name,slug,summary,storyline,first_release_date,total_rating,total_rating_count,aggregated_rating,aggregated_rating_count,genres,themes,platforms.id,platforms.name,platforms.slug,cover.image_id,cover.url,screenshots.image_id,screenshots.url,videos.video_id,videos.name,release_dates.id,release_dates.date,release_dates.platform,release_dates.region; {where_clause} sort total_rating_count desc; limit {limit}; offset {offset};",
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
        provider_items_exists: bool,
        provider_media_links_exists: bool,
        title_schema: TitleSchemaFlags,
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
            let processed_product_ids = self
                .persist_games(
                    db,
                    provider_id,
                    &mut cache,
                    &games,
                    &characters,
                    provider_items_exists,
                    provider_media_links_exists,
                    title_schema,
                    None,
                )
                .await?;

            let processed = processed_product_ids.len();
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
        let provider_items_exists = table_exists(db, "provider_items").await.unwrap_or(false);
        let provider_media_links_exists = table_exists(db, "vg_source_media_links")
            .await
            .unwrap_or(false);
        if !provider_items_exists {
            tracing::warn!(
                target = "igdb",
                "provider_items table missing; skipping provider_items writes (legacy DB compat)"
            );
        }
        if !provider_media_links_exists {
            tracing::warn!(
                target = "igdb",
                "provider_media_links table missing; skipping provider_media_links writes (legacy DB compat)"
            );
        }

        let provider_id = ensure_provider(
            db,
            IGDB_PROVIDER_NAME,
            IGDB_PROVIDER_KIND,
            Some(IGDB_PROVIDER_SLUG),
        )
        .await?;

        let title_schema = detect_title_schema(db).await?;

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

        info!(
            target = "igdb",
            page_size_override,
            max_pages_override,
            effective_page_size = limit,
            effective_max_pages = max_pages,
            "IGDB paging controls"
        );

        let result = self
            .backfill_range_inner(
                db,
                provider_id,
                platform_filter.clone(),
                from_year,
                to_year,
                limit,
                max_pages,
                provider_items_exists,
                provider_media_links_exists,
                title_schema,
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
        log_provider_snapshot(
            db,
            provider_id,
            provider_items_exists,
            provider_media_links_exists,
        )
        .await?;
        result
    }

    #[instrument(skip(self, db))]
    pub async fn ingest_top_monthly_from_env(&self, db: &Db) -> Result<usize> {
        let limit = std::env::var("IGDB_TOP_MONTHLY_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50)
            .clamp(1, IGDB_MAX_LIMIT);

        let month_spec = std::env::var("IGDB_TOP_MONTHLY_MONTH").ok();
        let (month_start, month_end) = resolve_month_bounds(month_spec.as_deref())?;

        let genre_tokens = std::env::var("IGDB_TOP_MONTHLY_GENRES")
            .ok()
            .map(|raw| split_csv_tokens(&raw))
            .filter(|tokens| !tokens.is_empty())
            .unwrap_or_else(default_top_monthly_genre_tokens);

        self.ingest_top_monthly(db, month_start, month_end, limit, genre_tokens)
            .await
    }

    async fn ingest_top_monthly(
        &self,
        db: &Db,
        month_start: NaiveDate,
        month_end: NaiveDate,
        limit: usize,
        genre_tokens: Vec<String>,
    ) -> Result<usize> {
        let provider_items_exists = table_exists(db, "provider_items").await.unwrap_or(false);
        let provider_media_links_exists = table_exists(db, "vg_source_media_links")
            .await
            .unwrap_or(false);

        if !provider_items_exists {
            tracing::warn!(
                target = "igdb",
                "provider_items table missing; skipping provider_items writes (legacy DB compat)"
            );
        }
        if !provider_media_links_exists {
            tracing::warn!(
                target = "igdb",
                "provider_media_links table missing; skipping provider_media_links writes (legacy DB compat)"
            );
        }

        let provider_id = ensure_provider(
            db,
            IGDB_PROVIDER_NAME,
            IGDB_PROVIDER_KIND,
            Some(IGDB_PROVIDER_SLUG),
        )
        .await?;

        let title_schema = detect_title_schema(db).await?;

        let genres = self.fetch_all_genres().await?;
        let genre_slug_to_id: HashMap<String, i64> = genres
            .into_iter()
            .filter_map(|g| {
                let id = g.id?;
                let slug = g.slug.or(g.name).unwrap_or_default();
                if slug.trim().is_empty() {
                    None
                } else {
                    Some((slug.to_ascii_lowercase(), id))
                }
            })
            .collect();

        let themes = self.fetch_all_themes().await?;
        let theme_slug_to_id: HashMap<String, i64> = themes
            .into_iter()
            .filter_map(|t| {
                let id = t.id?;
                let slug = t.slug.or(t.name).unwrap_or_default();
                if slug.trim().is_empty() {
                    None
                } else {
                    Some((slug.to_ascii_lowercase(), id))
                }
            })
            .collect();

        let desired = build_desired_tags(&genre_tokens);
        let mut allowed_genre_ids: Vec<i64> = Vec::new();
        let mut allowed_theme_ids: Vec<i64> = Vec::new();
        let mut igdb_genre_to_local: HashMap<i64, (String, String, String)> = HashMap::new();
        let mut igdb_theme_to_local: HashMap<i64, (String, String, String)> = HashMap::new();
        for d in desired {
            match d.kind {
                IgdbTagKind::Genre => {
                    if let Some(igdb_id) = genre_slug_to_id.get(&d.igdb_slug).copied() {
                        allowed_genre_ids.push(igdb_id);
                        igdb_genre_to_local.insert(
                            igdb_id,
                            (
                                d.local_slug.clone(),
                                d.igdb_slug.clone(),
                                d.display_name.clone(),
                            ),
                        );
                    } else if let Some(igdb_id) = theme_slug_to_id.get(&d.igdb_slug).copied() {
                        // IGDB taxonomy drift: some “genre-like” tags are actually themes.
                        // If the slug exists as a theme, treat it as such and avoid noisy warnings.
                        debug!(
                            target = "igdb",
                            igdb_slug = %d.igdb_slug,
                            "IGDB slug not found in genres; resolved as theme"
                        );
                        allowed_theme_ids.push(igdb_id);
                        igdb_theme_to_local.insert(
                            igdb_id,
                            (
                                d.local_slug.clone(),
                                d.igdb_slug.clone(),
                                d.display_name.clone(),
                            ),
                        );
                    } else {
                        warn!(
                            target = "igdb",
                            igdb_slug = %d.igdb_slug,
                            "IGDB genre slug not found; skipping"
                        );
                    }
                }
                IgdbTagKind::Theme => {
                    if let Some(igdb_id) = theme_slug_to_id.get(&d.igdb_slug).copied() {
                        allowed_theme_ids.push(igdb_id);
                        igdb_theme_to_local.insert(
                            igdb_id,
                            (
                                d.local_slug.clone(),
                                d.igdb_slug.clone(),
                                d.display_name.clone(),
                            ),
                        );
                    } else if let Some(igdb_id) = genre_slug_to_id.get(&d.igdb_slug).copied() {
                        // Symmetric fallback: treat it as a genre if IGDB reports it that way.
                        debug!(
                            target = "igdb",
                            igdb_slug = %d.igdb_slug,
                            "IGDB slug not found in themes; resolved as genre"
                        );
                        allowed_genre_ids.push(igdb_id);
                        igdb_genre_to_local.insert(
                            igdb_id,
                            (
                                d.local_slug.clone(),
                                d.igdb_slug.clone(),
                                d.display_name.clone(),
                            ),
                        );
                    } else {
                        warn!(
                            target = "igdb",
                            igdb_slug = %d.igdb_slug,
                            "IGDB theme slug not found; skipping"
                        );
                    }
                }
            }
        }
        allowed_genre_ids.sort_unstable();
        allowed_genre_ids.dedup();
        allowed_theme_ids.sort_unstable();
        allowed_theme_ids.dedup();

        if allowed_genre_ids.is_empty() && allowed_theme_ids.is_empty() {
            warn!(
                target = "igdb",
                "no valid IGDB genre/theme IDs resolved; top-monthly run will do nothing"
            );
            return Ok(0);
        }

        let genre_attach =
            build_genre_attach_context(db, &igdb_genre_to_local, &igdb_theme_to_local).await;
        if let Some(ctx) = genre_attach.as_ref() {
            if !ctx.enabled {
                warn!(
                    target = "igdb",
                    "genres/game_genre tables missing; skipping normalized genre pivot writes"
                );
            }
        }

        let start_epoch = Self::naive_to_epoch(month_start.and_hms_opt(0, 0, 0))
            .context("failed to compute month start epoch")?;
        let end_epoch = Self::naive_to_epoch(month_end.and_hms_opt(23, 59, 59))
            .context("failed to compute month end epoch")?;

        let meta = json!({
            "mode": "top-monthly",
            "month_start": month_start.to_string(),
            "month_end": month_end.to_string(),
            "limit": limit,
            "igdb_genre_ids": allowed_genre_ids,
            "igdb_theme_ids": allowed_theme_ids,
        });
        let ingest_id = ingest_run_start(db, provider_id, None, Some(meta)).await?;

        let mut cache = ProviderEntityCache::new(db.clone());
        let mut total = 0usize;
        let mut toplist_product_ids: Vec<i64> = Vec::new();
        let mut offset = 0usize;
        let mut page = 0usize;
        let page_limit = limit.min(self.effective_page_size(0));
        let max_pages = self.effective_max_pages(0);

        loop {
            if total >= limit {
                break;
            }
            if page >= max_pages {
                break;
            }

            let remaining = limit.saturating_sub(total).max(1);
            let q_limit = page_limit.min(remaining);
            let query = self.build_top_monthly_games_query(
                start_epoch,
                end_epoch,
                &allowed_genre_ids,
                &allowed_theme_ids,
                offset,
                q_limit,
            );
            let games = self.fetch_games(&query).await?;
            if games.is_empty() {
                break;
            }

            let characters = self.fetch_characters_for_games(&games).await?;
            let processed_product_ids = self
                .persist_games(
                    db,
                    provider_id,
                    &mut cache,
                    &games,
                    &characters,
                    provider_items_exists,
                    provider_media_links_exists,
                    title_schema,
                    genre_attach.as_ref(),
                )
                .await?;

            let processed = processed_product_ids.len();
            total += processed;
            toplist_product_ids.extend(processed_product_ids);
            page += 1;
            offset += q_limit;

            self.throttle().await;
            debug!(
                target = "igdb",
                page, processed, total, "igdb top-monthly page ingested"
            );

            if games.len() < q_limit {
                break;
            }
        }

        // Persist toplist membership for Spotlight consumption (best-effort; schema optional).
        let period_start = month_start.to_string();
        let period_end = month_end.to_string();
        let toplist_slug = format!("igdb:top_monthly:{period_start}:{period_end}");

        let mut ranked: Vec<(u32, i64)> = Vec::new();
        let mut seen: HashSet<i64> = HashSet::new();
        for pid in toplist_product_ids.into_iter() {
            if !seen.insert(pid) {
                continue;
            }
            let rank = (ranked.len() as u32) + 1;
            ranked.push((rank, pid));
            if ranked.len() >= limit {
                break;
            }
        }

        let toplist_meta = json!({
            "source": "igdb",
            "mode": "top-monthly",
            "month_start": period_start.clone(),
            "month_end": period_end.clone(),
            "limit": limit,
            "count": ranked.len(),
        });
        let toplist_id = upsert_provider_toplist(
            db,
            "igdb",
            &toplist_slug,
            "top_monthly",
            Some(period_start.as_str()),
            Some(period_end.as_str()),
            None,
            Some(toplist_meta),
        )
        .await?;
        replace_provider_toplist_items(db, toplist_id, &ranked).await?;

        ingest_run_finish(db, ingest_id, "completed", total as i64, 0, None).await?;
        log_provider_snapshot(
            db,
            provider_id,
            provider_items_exists,
            provider_media_links_exists,
        )
        .await?;

        Ok(total)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum IgdbTagKind {
    Genre,
    Theme,
}

#[derive(Debug, Clone)]
struct DesiredTag {
    kind: IgdbTagKind,
    igdb_slug: String,
    local_slug: String,
    display_name: String,
}

fn split_csv_tokens(raw: &str) -> Vec<String> {
    raw.split(|c| c == ',' || c == ';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn default_top_monthly_genre_tokens() -> Vec<String> {
    vec![
        "sports".into(),
        "shooters".into(),
        "fighting".into(),
        "action".into(),
        "simulators".into(),
    ]
}

fn normalize_genre_token(token: &str) -> String {
    token.trim().to_ascii_lowercase().replace(['_', '-'], " ")
}

fn build_desired_tags(tokens: &[String]) -> Vec<DesiredTag> {
    let mut out = Vec::new();
    for t in tokens {
        let norm = normalize_genre_token(t);

        let (canonical_local_slug, display_name) = match norm.as_str() {
            "sport" | "sports" => ("sports".to_string(), "Sports".to_string()),
            "shooter" | "shooters" | "shooting" => ("shooter".to_string(), "Shooter".to_string()),
            "fighting" | "fighter" | "fighters" => ("fighting".to_string(), "Fighting".to_string()),
            "action" => ("action".to_string(), "Action".to_string()),
            "sim" | "simulator" | "simulators" | "simulation" => {
                ("simulator".to_string(), "Simulator".to_string())
            }
            other => {
                let local_slug = other.replace(' ', "-");
                let title_case = local_slug
                    .split('-')
                    .filter(|p| !p.is_empty())
                    .map(|p| {
                        let mut chars = p.chars();
                        match chars.next() {
                            None => String::new(),
                            Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                (local_slug, title_case)
            }
        };

        // IGDB quirk: many people expect "action" to be a genre, but in IGDB it's often a theme.
        // We include both to be robust across potential IGDB taxonomy changes.
        match norm.as_str() {
            "sport" | "sports" => {
                out.push(DesiredTag {
                    kind: IgdbTagKind::Genre,
                    igdb_slug: "sport".to_string(),
                    local_slug: canonical_local_slug,
                    display_name,
                });
            }
            "shooter" | "shooters" | "shooting" => {
                out.push(DesiredTag {
                    kind: IgdbTagKind::Genre,
                    igdb_slug: "shooter".to_string(),
                    local_slug: canonical_local_slug,
                    display_name,
                });
            }
            "fighting" | "fighter" | "fighters" => {
                out.push(DesiredTag {
                    kind: IgdbTagKind::Genre,
                    igdb_slug: "fighting".to_string(),
                    local_slug: canonical_local_slug,
                    display_name,
                });
            }
            "action" => {
                out.push(DesiredTag {
                    kind: IgdbTagKind::Genre,
                    igdb_slug: "action".to_string(),
                    local_slug: canonical_local_slug.clone(),
                    display_name: display_name.clone(),
                });
                out.push(DesiredTag {
                    kind: IgdbTagKind::Theme,
                    igdb_slug: "action".to_string(),
                    local_slug: canonical_local_slug,
                    display_name,
                });
            }
            "sim" | "simulator" | "simulators" | "simulation" => {
                out.push(DesiredTag {
                    kind: IgdbTagKind::Genre,
                    igdb_slug: "simulator".to_string(),
                    local_slug: canonical_local_slug,
                    display_name,
                });
            }
            other => {
                out.push(DesiredTag {
                    kind: IgdbTagKind::Genre,
                    igdb_slug: other.replace(' ', "-"),
                    local_slug: canonical_local_slug,
                    display_name,
                });
            }
        }
    }

    out.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.igdb_slug.cmp(&b.igdb_slug)));
    out.dedup_by(|a, b| a.kind == b.kind && a.igdb_slug == b.igdb_slug);
    out
}

fn resolve_month_bounds(month: Option<&str>) -> Result<(NaiveDate, NaiveDate)> {
    let (year, month_num) = if let Some(spec) = month {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            let now = Utc::now().date_naive();
            (now.year(), now.month())
        } else {
            // Accept YYYY-MM
            let parts: Vec<&str> = trimmed.split('-').collect();
            if parts.len() != 2 {
                anyhow::bail!("IGDB_TOP_MONTHLY_MONTH must be YYYY-MM (got: {trimmed})");
            }
            let y: i32 = parts[0]
                .parse()
                .map_err(|_| anyhow!("invalid year in IGDB_TOP_MONTHLY_MONTH: {trimmed}"))?;
            let m: u32 = parts[1]
                .parse()
                .map_err(|_| anyhow!("invalid month in IGDB_TOP_MONTHLY_MONTH: {trimmed}"))?;
            (y, m)
        }
    } else {
        let now = Utc::now().date_naive();
        (now.year(), now.month())
    };

    let start = NaiveDate::from_ymd_opt(year, month_num, 1)
        .ok_or_else(|| anyhow!("invalid month bounds year={year} month={month_num}"))?;
    let (next_year, next_month) = if month_num == 12 {
        (year + 1, 1)
    } else {
        (year, month_num + 1)
    };
    let next_start = NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .ok_or_else(|| anyhow!("invalid next month bounds year={next_year} month={next_month}"))?;
    let end = next_start
        .pred_opt()
        .ok_or_else(|| anyhow!("failed to compute month end"))?;
    Ok((start, end))
}

async fn build_genre_attach_context(
    db: &Db,
    igdb_genre_id_to_local: &HashMap<i64, (String, String, String)>,
    igdb_theme_id_to_local: &HashMap<i64, (String, String, String)>,
) -> Option<GenreAttachContext> {
    let genres_exists = table_exists(db, "genres").await.unwrap_or(false);
    let game_genre_exists = table_exists(db, "game_genre").await.unwrap_or(false);
    if !genres_exists || !game_genre_exists {
        return Some(GenreAttachContext {
            enabled: false,
            igdb_genre_id_to_local_genre_id: HashMap::new(),
            igdb_theme_id_to_local_genre_id: HashMap::new(),
            game_genre_has_timestamps: false,
        });
    }

    let genres_has_slug = column_exists(db, "genres", "slug").await.unwrap_or(false);
    let genres_has_name = column_exists(db, "genres", "name").await.unwrap_or(false);
    let game_genre_has_product_id = column_exists(db, "game_genre", "product_id")
        .await
        .unwrap_or(false);
    let game_genre_has_genre_id = column_exists(db, "game_genre", "genre_id")
        .await
        .unwrap_or(false);

    if !genres_has_slug
        || !genres_has_name
        || !game_genre_has_product_id
        || !game_genre_has_genre_id
    {
        warn!(
            target = "igdb",
            genres_has_slug,
            genres_has_name,
            game_genre_has_product_id,
            game_genre_has_genre_id,
            "genres/game_genre columns missing; skipping normalized genre pivot writes"
        );
        return Some(GenreAttachContext {
            enabled: false,
            igdb_genre_id_to_local_genre_id: HashMap::new(),
            igdb_theme_id_to_local_genre_id: HashMap::new(),
            game_genre_has_timestamps: false,
        });
    }

    let genres_has_created_at = column_exists(db, "genres", "created_at")
        .await
        .unwrap_or(false);
    let genres_has_updated_at = column_exists(db, "genres", "updated_at")
        .await
        .unwrap_or(false);
    let game_genre_has_created_at = column_exists(db, "game_genre", "created_at")
        .await
        .unwrap_or(false);
    let game_genre_has_updated_at = column_exists(db, "game_genre", "updated_at")
        .await
        .unwrap_or(false);
    let game_genre_has_timestamps = game_genre_has_created_at && game_genre_has_updated_at;

    let has_timestamps = genres_has_created_at && genres_has_updated_at;

    let mut genre_map: HashMap<i64, i64> = HashMap::new();
    for (igdb_id, (primary_slug, alt_slug, display_name)) in igdb_genre_id_to_local {
        match find_or_create_genre(db, primary_slug, alt_slug, display_name, has_timestamps).await {
            Ok(Some(local_id)) => {
                genre_map.insert(*igdb_id, local_id);
            }
            Ok(None) => {}
            Err(err) => {
                warn!(
                    target = "igdb",
                    igdb_genre_id = *igdb_id,
                    primary_slug = %primary_slug,
                    alt_slug = %alt_slug,
                    error = %err,
                    "failed to resolve/create local genre (best-effort)"
                );
            }
        }
    }

    let mut theme_map: HashMap<i64, i64> = HashMap::new();
    for (igdb_id, (primary_slug, alt_slug, display_name)) in igdb_theme_id_to_local {
        match find_or_create_genre(db, primary_slug, alt_slug, display_name, has_timestamps).await {
            Ok(Some(local_id)) => {
                theme_map.insert(*igdb_id, local_id);
            }
            Ok(None) => {}
            Err(err) => {
                warn!(
                    target = "igdb",
                    igdb_theme_id = *igdb_id,
                    primary_slug = %primary_slug,
                    alt_slug = %alt_slug,
                    error = %err,
                    "failed to resolve/create local theme-as-genre (best-effort)"
                );
            }
        }
    }

    Some(GenreAttachContext {
        enabled: true,
        igdb_genre_id_to_local_genre_id: genre_map,
        igdb_theme_id_to_local_genre_id: theme_map,
        game_genre_has_timestamps,
    })
}

async fn find_or_create_genre(
    db: &Db,
    primary_slug: &str,
    alt_slug: &str,
    display_name: &str,
    has_timestamps: bool,
) -> Result<Option<i64>> {
    let existing: Option<i64> = sqlx::query_scalar(
        "select id from genres where lower(slug) = lower($1) or lower(slug) = lower($2) or lower(name) = lower($3) limit 1",
    )
    .persistent(false)
    .bind(primary_slug)
    .bind(alt_slug)
    .bind(display_name)
    .fetch_optional(&db.pool)
    .await?;
    if let Some(id) = existing {
        return Ok(Some(id));
    }

    // Best-effort insert if missing.
    if has_timestamps {
        let created: Option<i64> = sqlx::query_scalar(
            "insert into genres (slug, name, created_at, updated_at) values ($1, $2, now(), now()) returning id",
        )
        .persistent(false)
        .bind(primary_slug)
        .bind(display_name)
        .fetch_optional(&db.pool)
        .await
        .ok()
        .flatten();
        return Ok(created);
    }

    let created: Option<i64> =
        sqlx::query_scalar("insert into genres (slug, name) values ($1, $2) returning id")
            .persistent(false)
            .bind(primary_slug)
            .bind(display_name)
            .fetch_optional(&db.pool)
            .await
            .ok()
            .flatten();
    Ok(created)
}

async fn attach_genre_to_product(
    db: &Db,
    product_id: i64,
    genre_id: i64,
    has_timestamps: bool,
) -> Result<()> {
    if has_timestamps {
        // Prefer a lightweight existence check to avoid relying on unique constraints.
        let exists: Option<i64> = sqlx::query_scalar(
            "select 1 from game_genre where product_id=$1 and genre_id=$2 limit 1",
        )
        .persistent(false)
        .bind(product_id)
        .bind(genre_id)
        .fetch_optional(&db.pool)
        .await?;
        if exists.is_some() {
            return Ok(());
        }
        sqlx::query(
            "insert into game_genre (product_id, genre_id, created_at, updated_at) values ($1, $2, now(), now())",
        )
        .persistent(false)
        .bind(product_id)
        .bind(genre_id)
        .execute(&db.pool)
        .await?;
        return Ok(());
    }

    let exists: Option<i64> =
        sqlx::query_scalar("select 1 from game_genre where product_id=$1 and genre_id=$2 limit 1")
            .persistent(false)
            .bind(product_id)
            .bind(genre_id)
            .fetch_optional(&db.pool)
            .await?;
    if exists.is_some() {
        return Ok(());
    }
    sqlx::query("insert into game_genre (product_id, genre_id) values ($1, $2)")
        .persistent(false)
        .bind(product_id)
        .bind(genre_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}

async fn column_exists(db: &Db, table: &str, column: &str) -> Result<bool> {
    // NOTE: do not rely solely on search_path/current_schemas(true) here.
    // In some environments (including Supabase + pooled connections), search_path can vary
    // per-session, which can cause schema probing to miss `public.*` tables.
    let exists_in_search_path: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = ANY (current_schemas(true))
              AND table_name = $1
              AND column_name = $2
        )",
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_one(&db.pool)
    .await?;

    if exists_in_search_path {
        return Ok(true);
    }

    let exists_in_public: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
              AND column_name = $2
        )",
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_one(&db.pool)
    .await?;

    if exists_in_public {
        return Ok(true);
    }

    // Last resort: any non-system schema.
    let exists_any: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_name = $1
              AND column_name = $2
        )",
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_one(&db.pool)
    .await?;

    Ok(exists_any)
}

async fn detect_title_schema(db: &Db) -> Result<TitleSchemaFlags> {
    // Schema detection: some DBs use a source-registry schema where `video_game_titles` is keyed by
    // (video_game_source_id, video_game_source_id), and may require video_game_id (NOT NULL) or product_id.
    let titles_has_source_id = column_exists(db, "video_game_titles", "video_game_source_id")
        .await
        .unwrap_or(false);
    let titles_has_video_game_source_id =
        column_exists(db, "video_game_titles", "video_game_source_id")
            .await
            .unwrap_or(false);
    let sources_table_exists = table_exists(db, "video_game_sources")
        .await
        .unwrap_or(false);
    if titles_has_source_id && titles_has_video_game_source_id && !sources_table_exists {
        warn!(
            target = "igdb",
            "video_game_titles has (video_game_source_id, video_game_source_id) but video_game_sources table is missing; falling back to legacy title schema"
        );
    }
    let keyed_by_source_item =
        titles_has_source_id && titles_has_video_game_source_id && sources_table_exists;
    let has_video_game_id = if keyed_by_source_item {
        column_exists(db, "video_game_titles", "video_game_id")
            .await
            .unwrap_or(false)
    } else {
        false
    };
    let has_product_id = if keyed_by_source_item {
        column_exists(db, "video_game_titles", "product_id")
            .await
            .unwrap_or(false)
    } else {
        false
    };
    let video_games_is_laravel = if has_video_game_id {
        column_exists(db, "video_games", "product_id")
            .await
            .unwrap_or(false)
            && column_exists(db, "video_games", "title")
                .await
                .unwrap_or(false)
    } else {
        false
    };
    let source_id = if keyed_by_source_item {
        Some(ensure_video_game_source(db, IGDB_PROVIDER_SLUG, IGDB_PROVIDER_NAME).await?)
    } else {
        None
    };
    Ok(TitleSchemaFlags {
        keyed_by_source_item,
        has_video_game_id,
        has_product_id,
        video_games_is_laravel,
        source_id,
    })
}

#[instrument(skip(db))]
async fn log_provider_snapshot(
    db: &Db,
    provider_id: i64,
    provider_items_exists: bool,
    provider_media_links_exists: bool,
) -> Result<()> {
    let provider_items = if provider_items_exists {
        fetch_provider_count(
            db,
            "SELECT COUNT(*)::bigint FROM provider_items WHERE provider_id=$1",
            provider_id,
        )
        .await?
    } else {
        0
    };
    let provider_media_links = if provider_media_links_exists {
        fetch_provider_count(
            db,
            "SELECT COUNT(*)::bigint FROM vg_source_media_links WHERE provider_id=$1",
            provider_id,
        )
        .await?
    } else {
        0
    };
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

async fn table_exists(db: &Db, table: &str) -> Result<bool> {
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (\
            SELECT 1 FROM information_schema.tables\
            WHERE table_schema='public' AND table_name=$1\
        )",
    )
    .persistent(false)
    .bind(table)
    .fetch_one(&db.pool)
    .await?;
    Ok(exists.0)
}

#[instrument(skip(db))]
pub async fn run_from_env(db: &Db) -> Result<()> {
    // Check for required schema tables (legacy-safe)
    let required_tables = [
        "platforms",
        "providers",
        "provider_items",
        "video_game_sources",
        "video_game_titles",
        "video_games",
    ];
    let mut missing: Vec<&str> = Vec::new();
    for t in required_tables {
        if !table_exists(db, t).await.unwrap_or(false) {
            missing.push(t);
        }
    }
    if !missing.is_empty() {
        use crate::database_ops::ingest_providers::php_compat_schema;
        let compat = php_compat_schema(db).await.unwrap_or(false);
        tracing::warn!(
            missing_tables = ?missing,
            php_compat = compat,
            "igdb run_from_env: required schema missing; skipping IGDB ingestion to preserve backward compatibility"
        );
        return Ok(());
    }

    let service = IgdbService::new_from_env()?;
    let mode = std::env::var("IGDB_MODE").unwrap_or_else(|_| "backfill".to_string());

    let current_year = Utc::now().year();
    let mut from_year = service
        .cfg
        .from_year
        .unwrap_or(current_year.saturating_sub(1));
    let mut to_year = service.cfg.to_year.unwrap_or(current_year);
    if from_year > to_year {
        std::mem::swap(&mut from_year, &mut to_year);
    }

    // Smoke-test sanity: make it obvious which env-derived limits are in effect.
    info!(
        target = "igdb",
        reqs_per_min = service.cfg.reqs_per_min,
        rps = service.cfg.rps,
        concurrency = service.cfg.concurrency,
        page_size = service.cfg.page_size,
        max_pages = service.cfg.max_pages,
        max_retries = service.cfg.max_retries,
        backoff_ms = service.cfg.backoff_ms,
        from_year,
        to_year,
        platform_ids = ?service.cfg.platform_ids,
        window_start = ?service.cfg.window_start,
        window_end = ?service.cfg.window_end,
        characters_per_game = service.cfg.characters_per_game,
        "IGDB run config"
    );

    let processed = if matches!(
        mode.as_str(),
        "top-50"
            | "top_50"
            | "top50"
            | "top-monthly"
            | "top_monthly"
            | "topmonthly"
            | "monthly-top"
            | "monthly"
    ) {
        if matches!(mode.as_str(), "top-50" | "top_50" | "top50") {
            info!(target = "igdb", mode = %mode, "IGDB running top-50 mode (alias of top-monthly)");
        } else {
            info!(target = "igdb", mode = %mode, "IGDB running top-monthly mode");
        }
        service.ingest_top_monthly_from_env(db).await?
    } else {
        info!(target = "igdb", mode = %mode, "IGDB running backfill mode");
        service
            .backfill_range(db, from_year, to_year, &service.cfg.platform_ids, 0, 0)
            .await?
    };
    info!(
        target = "igdb",
        processed,
        from_year,
        to_year,
        mode = %mode,
        "IGDB ingestion completed"
    );
    Ok(())
}
