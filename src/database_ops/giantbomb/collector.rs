use anyhow::{anyhow, Result};
use futures::future::join_all;
use indexmap::map::Entry;
use indexmap::IndexMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    env,
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

// Public entrypoint to run GiantBomb collection with current CLI/env behavior
pub async fn run_from_env() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if let Some(idx) = args
        .iter()
        .position(|arg| arg == "--merge-details" || arg.starts_with("--merge-details="))
    {
        let (output, sources) = resolve_merge_details_args(&args, idx)?;
        return merge_detail_files(&output, &sources);
    }

    if let Some(target) = extract_parse_videos_target(&args) {
        if target.is_empty() {
            return Err(anyhow!("--parse-videos requires a file path argument"));
        }
        return parse_videos_from_file(&target);
    }

    install_shutdown_handler(vec![
        "games_detailed_partial.json".to_string(),
        "games_detailed.json".to_string(),
        "merged_games.json".to_string(),
    ]);

    if args.iter().any(|arg| arg == "--parse-partial-videos") {
        return parse_videos_from_file("games_detailed_partial.json");
    }

    let client = Arc::new(Client::builder().user_agent("GBCollector/1.0").build()?);
    let semaphore = Arc::new(Semaphore::new(1)); // limit concurrency to 1 request
    let limiter = Arc::new(RequestLimiter::new(200, Duration::from_secs(3600)));

    let game_summary = Some("games_summary.json");
    let mut detailed: IndexMap<String, GameDetail> =
        match fs::read_to_string("games_detailed_partial.json") {
            Ok(raw) => match parse_partial_map(&raw) {
                Ok(existing) => {
                    println!(
                        "Loaded {} previously stored game details from partial file.",
                        existing.len()
                    );
                    existing
                }
                Err(err) => {
                    println!(
                        "⚠️ Failed to parse games_detailed_partial.json ({}). Starting fresh.",
                        err
                    );
                    IndexMap::new()
                }
            },
            Err(_) => IndexMap::new(),
        };

    if game_summary.is_some() && fs::metadata("games_summary.json").is_ok() {
        println!("Loading existing game summaries from file.");
        let data = fs::read_to_string("games_summary.json")?;
        let all_games: Vec<GameSummary> = serde_json::from_str(&data)?;
        println!("Loaded {} games from summary file.", all_games.len());

        if !detailed.is_empty() {
            let processed_count = detailed.len();
            if let Some(last_guid) = detailed.keys().last().cloned() {
                if let Some(pos) = all_games.iter().position(|g| g.guid == last_guid) {
                    println!(
                        "[resume] Partial file contains {} entries. Last GUID {} at summary index {}.",
                        processed_count, last_guid, pos
                    );
                } else {
                    println!(
                        "[resume] Partial file contains {} entries. Last GUID {} not found in summaries.",
                        processed_count, last_guid
                    );
                }
            }
        } else {
            println!(
                "[resume] No prior entries in games_detailed_partial.json. Starting from scratch."
            );
        }

        for (idx, summary) in all_games.iter().enumerate() {
            if detailed.contains_key(&summary.guid) {
                println!(
                    "[detail] ({}/{}) Skipping GUID {} ({:?}) already in partial file.",
                    idx + 3930,
                    all_games.len(),
                    summary.guid,
                    summary.name
                );
                continue;
            }
            tokio::time::sleep(Duration::from_millis(1800)).await;
            println!(
                "[detail] ({}/{}) Fetching detail for GUID {} ({:?}).",
                idx + 3930,
                all_games.len(),
                summary.guid,
                summary.name
            );
            tokio::time::sleep(Duration::from_millis(1800)).await;
            let mut new_detail_added = false;
            match fetch_game_detail(client.clone(), limiter.clone(), &semaphore, summary).await {
                Ok(detail) => {
                    sleep(Duration::from_millis(3600)).await;
                    println!(
                        "[detail] ✓ Stored detail for GUID {} ({:?}).",
                        summary.guid, summary.name
                    );
                    tokio::time::sleep(Duration::from_millis(1800)).await;

                    let mut appended = false;
                    if let Err(err) =
                        append_partial_detail("games_detailed_partial.json", &summary.guid, &detail)
                    {
                        println!(
                            "⚠️ Failed to append partial detail for {}: {err:?}. Falling back to rewrite.",
                            summary.guid
                        );
                    } else {
                        appended = true;
                    }

                    detailed.insert(summary.guid.clone(), detail);
                    new_detail_added = true;

                    if !appended {
                        if let Err(err) =
                            write_full_detail_map("games_detailed_partial.json", &detailed)
                        {
                            println!(
                                "⚠️ Fallback rewrite of partial file failed after {} entries: {err:?}",
                                detailed.len()
                            );
                        }
                    }
                }
                Err(err) => {
                    sleep(Duration::from_millis(3600)).await;
                    println!(
                        "⚠️ Failed to fetch detail for {} ({:?}): {err:?}",
                        summary.guid, summary.name
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(1800)).await;
            if new_detail_added {
                if let Err(err) = write_full_detail_map("games_detailed.json", &detailed) {
                    println!(
                        "⚠️ Failed to update full detailed file after {} entries: {err:?}",
                        detailed.len()
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(1800)).await;
        }
    } else {
        println!("No existing game summary file found. Fetching new data.");
    }

    let platforms = vec![
        ("Xbox360", 20),
        ("Wii", 36),
        ("XboxOne", 145),
        ("PS4", 146),
        ("Switch", 157),
        ("PS5", 176),
    ];

    let mut all_games: Vec<GameSummary> = Vec::new();

    for (name, id) in &platforms {
        println!("[summary] Sleeping briefly before {name} request batch.");
        sleep(Duration::from_millis(10000)).await;
        println!("Fetching platform: {name}");
        match fetch_games(client.clone(), limiter.clone(), *id, name.to_string()).await {
            Ok(games) => {
                println!("→ {name}: {} games", games.len());
                all_games.extend(games);
            }
            Err(err) => {
                println!("⚠️ Skipping platform {name} due to error: {err:?}");
            }
        }
        println!("[summary] Completed platform {name}, waiting before next platform.");
        sleep(Duration::from_millis(100000)).await;
    }

    println!(
        "[summary] Finished fetching summaries for {} platforms. Writing to disk.",
        platforms.len()
    );
    fs::write(
        "games_summary.json",
        serde_json::to_string_pretty(&all_games)?,
    )?;
    println!("Starting detail fetch for {} games.", all_games.len());

    for (idx, summary) in all_games.iter().enumerate() {
        if detailed.contains_key(&summary.guid) {
            println!(
                "[detail] ({}/{}) Skipping GUID {} ({:?}) already in partial file.",
                idx + 1,
                all_games.len(),
                summary.guid,
                summary.name
            );
            continue;
        }
        println!(
            "[detail] ({}/{}) Fetching detail for GUID {} ({:?}).",
            idx + 1,
            all_games.len() / 2,
            summary.guid,
            summary.name
        );
        let mut new_detail_added = false;
        match fetch_game_detail(client.clone(), limiter.clone(), &semaphore, summary).await {
            Ok(detail) => {
                println!(
                    "[detail] ✓ Stored detail for GUID {} ({:?}).",
                    summary.guid, summary.name
                );
                let mut appended = false;
                if let Err(err) =
                    append_partial_detail("games_detailed_partial.json", &summary.guid, &detail)
                {
                    println!(
                        "⚠️ Failed to append partial detail for {}: {err:?}. Falling back to rewrite.",
                        summary.guid
                    );
                } else {
                    appended = true;
                }

                detailed.insert(summary.guid.clone(), detail);
                new_detail_added = true;

                if !appended {
                    if let Err(err) =
                        write_full_detail_map("games_detailed_partial.json", &detailed)
                    {
                        println!(
                            "⚠️ Fallback rewrite of partial file failed after {} entries: {err:?}",
                            detailed.len()
                        );
                    }
                }
            }
            Err(err) => {
                println!(
                    "⚠️ Failed to fetch detail for {} ({:?}): {err:?}",
                    summary.guid, summary.name
                );
            }
        }

        if new_detail_added {
            if let Err(err) = write_full_detail_map("games_detailed.json", &detailed) {
                println!(
                    "⚠️ Failed to update full detailed file after {} entries: {err:?}",
                    detailed.len()
                );
            }
        }
    }

    write_full_detail_map("games_detailed.json", &detailed)?;
    println!("✅ Saved games summary with {} entries.", all_games.len());
    println!("✅ Saved {} detailed games.", detailed.len());

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct GameSummary {
    guid: String,
    name: Option<String>,
    site_detail_url: Option<String>,
    platform: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GameDetail {
    #[serde(default)]
    guid: String,
    name: Option<String>,
    description: Option<String>,
    image: Option<Image>,
    images: Option<Vec<Image>>,
    video_shows: Option<Vec<VideoShow>>,
    themes: Option<Vec<Theme>>,
    videos: Option<Vec<Video>>,
    video_api_payloads: Option<Vec<VideoApiPayload>>,
    original_game_rating: Option<Vec<Rating>>,
    #[serde(default)]
    raw_results: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct Image {
    original_url: Option<String>,
    super_url: Option<String>,
    small_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Rating {
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Theme {
    id: Option<i64>,
    name: Option<String>,
    api_detail_url: Option<String>,
    site_detail_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct VideoImage {
    icon_url: Option<String>,
    small_url: Option<String>,
    medium_url: Option<String>,
    super_url: Option<String>,
    thumb_url: Option<String>,
    tiny_url: Option<String>,
    screen_url: Option<String>,
    screen_large_url: Option<String>,
    original_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct VideoLight {
    api_detail_url: Option<String>,
    deck: Option<String>,
    guid: Option<String>,
    id: Option<String>,
    image: Option<VideoImage>,
    length_seconds: Option<i64>,
    name: Option<String>,
    publish_date: Option<String>,
    site_detail_url: Option<String>,
    url: Option<String>,
    user: Option<String>,
    video_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct VideoShow {
    api_detail_url: Option<String>,
    deck: Option<String>,
    title: Option<String>,
    name: Option<String>,
    id: Option<String>,
    guid: Option<String>,
    site_detail_url: Option<String>,
    image: Option<VideoImage>,
    logo: Option<VideoImage>,
    position: Option<i64>,
    active: Option<bool>,
    latest: Option<VideoLight>,
    playable_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Video {
    id: Option<String>,
    guid: Option<String>,
    name: Option<String>,
    deck: Option<String>,
    hd_url: Option<String>,
    high_url: Option<String>,
    low_url: Option<String>,
    embed_player: Option<String>,
    length_seconds: Option<i64>,
    publish_date: Option<String>,
    site_detail_url: Option<String>,
    url: Option<String>,
    api_detail_url: Option<String>,
    user: Option<String>,
    video_type: Option<String>,
    video_show: Option<VideoShow>,
    image: Option<VideoImage>,
    playable_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct VideoApiPayload {
    video_id: Option<String>,
    api_detail_url: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug)]
struct RequestWindow {
    window_start: Instant,
    count: usize,
    active_key: usize,
}

#[derive(Debug)]
struct RequestLimiter {
    limit: usize,
    window: Duration,
    inner: Mutex<RequestWindow>,
}

const API_KEYS: [&str; 2] = [
    "74f31006d7fbadb6842b84916b553ddeced05967",
    "ad28929ad9fa2f2245720573598f14b1fc50c34e",
];

impl RequestLimiter {
    pub fn new(limit: usize, window: Duration) -> Self {
        Self {
            limit,
            window,
            inner: Mutex::new(RequestWindow {
                window_start: Instant::now(),
                count: 0,
                active_key: 0,
            }),
        }
    }

    pub async fn acquire(&self, reason: &str) -> usize {
        loop {
            let mut guard = self.inner.lock().await;
            let now = Instant::now();
            let elapsed = now.duration_since(guard.window_start);

            if elapsed >= self.window {
                println!(
                    "[rate-limit] Window expired after {:?}. Resetting (prev count {}).",
                    elapsed, guard.count
                );
                guard.window_start = now;
                guard.count = 0;
                guard.active_key = 0;
            }

            if guard.count < self.limit {
                guard.count += 1;
                let key_index = guard.active_key;
                println!(
                    "[rate-limit] #{}/{} using key {} ({}) - {}",
                    guard.count, self.limit, key_index, API_KEYS[key_index], reason
                );
                return key_index;
            }

            if guard.active_key == 0 && elapsed < self.window {
                println!(
                    "[rate-limit] Limit {} hit on key0 within window; switching to key1.",
                    self.limit
                );
                guard.active_key = 1;
                guard.count = 0;
                continue;
            }

            if guard.active_key == 1 && elapsed < self.window {
                let wait = self.window - elapsed;
                println!(
                    "[rate-limit] Limit {} hit on key1. Sleeping {:?} until reset.",
                    self.limit, wait
                );
                drop(guard);
                sleep(wait).await;
                continue;
            }
        }
    }
}

const BASE: &str = "https://www.giantbomb.com/api";

async fn fetch_games(
    client: Arc<Client>,
    limiter: Arc<RequestLimiter>,
    platform_id: i32,
    platform_name: String,
) -> Result<Vec<GameSummary>> {
    let mut results = Vec::new();
    let mut offset = 2295;
    let api_key_index = 0;
    loop {
        let url = format!(
            "{BASE}/games/?api_key={}&format=json&filter=platforms:{},original_release_date:2020-01-01|2025-12-31&field_list=guid,name,site_detail_url&limit=100&offset={}",
            API_KEYS[api_key_index], platform_id, offset
        );

        let context = format!("summary:{platform_name}:offset:{offset}");
        limiter.acquire(&context).await;
        println!(
            "Fetching summary page for platform {} at offset {}: {}
        using key {}",
            platform_name,
            offset,
            url.clone(),
            api_key_index + 1
        );
        let response = client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "Non-success status {} for platform {} at offset {}",
                response.status(),
                platform_name,
                offset
            ));
        }
        let res = response.json::<serde_json::Value>().await?;
        println!(
            "Fetched {} games for {} at offset {}",
            res["number_of_total_results"], platform_name, offset
        );
        if res["error"] != "OK" {
            println!(
                "[summary] API reported non-OK status ({}) for {platform_name}.",
                res["error"]
            );
            break;
        }

        if let Some(arr) = res["results"].as_array() {
            for g in arr {
                results.push(GameSummary {
                    guid: g["guid"].as_str().unwrap_or_default().to_string(),
                    name: g["name"].as_str().map(|s| s.to_string()),
                    site_detail_url: g["site_detail_url"].as_str().map(|s| s.to_string()),
                    platform: platform_name.clone(),
                });
            }
            if arr.len() < 100 {
                println!(
                    "[summary] Received {} results (<100), finishing platform {}.",
                    arr.len(),
                    platform_name
                );
                break;
            }
            offset += 100;
        } else {
            println!(
                "[summary] No result array returned for platform {} at offset {}.",
                platform_name, offset
            );
            break;
        }
        println!(
            "[summary] Sleeping 1s after platform {} page at offset {}.",
            platform_name, offset
        );
        sleep(Duration::from_secs(1)).await;
    }
    Ok(results)
}

async fn fetch_game_detail(
    client: Arc<Client>,
    limiter: Arc<RequestLimiter>,
    sem: &Semaphore,
    summary: &GameSummary,
) -> Result<GameDetail> {
    let _permit = sem.acquire().await?;
    let context = format!("detail:{}", summary.guid);
    let api_key_index = limiter.acquire(&context).await;
    limiter.acquire(&context).await;

    let url = format!(
        "{BASE}/game/{}/?api_key={}&format=json",
        summary.guid, API_KEYS[api_key_index]
    );
    println!("Fetching detail for game: {}", url.clone());

    let response = client.get(&url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Non-success status {} for detail {} ({:?})",
            response.status(),
            summary.guid,
            summary.name
        ));
    }
    let data = response.json::<serde_json::Value>().await?;
    println!(
        "Snoozing for 1s before processing Fetched detail for game: {:?}",
        data
    );
    sleep(Duration::from_millis(1800)).await;
    println!(
        "Snoozing for 1 before processing detail for game: {:?}",
        summary.name.as_deref().unwrap_or("Unknown")
    );
    sleep(Duration::from_millis(1800)).await;
    let results = &data["results"];
    let active_api_key = API_KEYS[api_key_index];

    let videos = if let Some(arr) = results["videos"].as_array() {
        let tasks = arr
            .iter()
            .cloned()
            .map(|entry| parse_video_async(entry, active_api_key))
            .collect::<Vec<_>>();
        let parsed = join_all(tasks).await;
        let collected: Vec<_> = parsed.into_iter().flatten().collect();
        (!collected.is_empty()).then_some(collected)
    } else {
        None
    };

    let images = results["images"]
        .as_array()
        .map(|arr| arr.iter().filter_map(parse_image).collect());
    let video_shows = if let Some(arr) = results["video_shows"].as_array() {
        let tasks = arr
            .iter()
            .cloned()
            .map(|entry| parse_video_show_async(entry, active_api_key))
            .collect::<Vec<_>>();
        let parsed = join_all(tasks).await;
        let collected: Vec<_> = parsed.into_iter().flatten().collect();
        (!collected.is_empty()).then_some(collected)
    } else {
        None
    };
    let ratings = results["original_game_rating"]
        .as_array()
        .map(|arr| arr.iter().filter_map(parse_rating).collect());
    let themes = results["themes"]
        .as_array()
        .map(|arr| arr.iter().filter_map(parse_theme).collect::<Vec<_>>());
    let raw_results = results.clone();

    let mut video_api_payloads: Option<Vec<VideoApiPayload>> = None;
    if let Some(video_list) = videos.as_ref() {
        let mut collected_payloads = Vec::new();
        for video in video_list {
            let raw_url = match video.api_detail_url.as_deref() {
                Some(url) => url,
                None => continue,
            };
            let Some(request_url) = build_api_detail_request_url(raw_url, active_api_key) else {
                println!(
                    "[video-api] Skipping video {:?}: invalid api_detail_url.",
                    video.id
                );
                continue;
            };

            match fetch_video_api_payload(
                client.clone(),
                limiter.clone(),
                &request_url,
                video.id.as_deref().unwrap_or("unknown"),
            )
            .await
            {
                Ok(payload) => {
                    if let Some(high_url) = payload
                        .pointer("/results/high_url")
                        .and_then(|v| v.as_str())
                    {
                        println!(
                            "[video-api] high_url discovered for video {:?}: {}",
                            video.id, high_url
                        );
                    }
                    collected_payloads.push(VideoApiPayload {
                        video_id: video.id.clone(),
                        api_detail_url: video.api_detail_url.clone(),
                        payload,
                    });
                }
                Err(err) => {
                    println!(
                        "⚠️ Failed to fetch video API payload for video {:?}: {err:?}",
                        video.id
                    );
                }
            }

            sleep(Duration::from_millis(1200)).await;
        }

        if !collected_payloads.is_empty() {
            video_api_payloads = Some(collected_payloads);
        }
    }

    println!(
        "Processed detail for game: {}",
        summary.name.as_deref().unwrap_or("Unknown")
    );
    sleep(Duration::from_secs(1)).await;

    Ok(GameDetail {
        guid: summary.guid.clone(),
        name: results["name"].as_str().map(|s| s.to_string()),
        description: results["description"].as_str().map(|s| s.to_string()),
        image: parse_image(&results["image"]),
        themes,
        images,
        video_shows,
        videos,
        video_api_payloads,
        original_game_rating: ratings,
        raw_results,
    })
}

async fn fetch_video_api_payload(
    client: Arc<Client>,
    limiter: Arc<RequestLimiter>,
    api_url: &str,
    context: &str,
) -> Result<serde_json::Value> {
    let reason = format!("video_api:{context}");
    limiter.acquire(&reason).await;
    println!("[video-api] Fetching payload for {context} via {api_url}");
    let response = client.get(api_url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Non-success status {} when fetching video API for {context}",
            response.status()
        ));
    }
    let payload = response.json::<serde_json::Value>().await?;
    println!("[video-api] Retrieved payload for {context}.");
    Ok(payload)
}

fn append_partial_detail(path: &str, guid: &str, detail: &GameDetail) -> Result<()> {
    let entry = format_detail_entry(guid, detail)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    let metadata = file.metadata()?;

    if metadata.len() == 0 {
        let content = format!("{{\n{}\n}}\n", entry);
        file.write_all(content.as_bytes())?;
        file.flush()?;
        return Ok(());
    }

    let mut position = metadata.len() as i64 - 1;
    let mut buf = [0u8; 1];
    let mut last_char = None;
    while position >= 0 {
        file.seek(SeekFrom::Start(position as u64))?;
        file.read_exact(&mut buf)?;
        let ch = buf[0] as char;
        if !ch.is_ascii_whitespace() {
            last_char = Some(ch);
            break;
        }
        position -= 1;
    }

    let Some('}') = last_char else {
        return Err(anyhow!(
            "Malformed partial detail file: expected closing '}}'"
        ));
    };

    let mut prev_position = position - 1;
    let mut prev_char = None;
    while prev_position >= 0 {
        file.seek(SeekFrom::Start(prev_position as u64))?;
        file.read_exact(&mut buf)?;
        let ch = buf[0] as char;
        if !ch.is_ascii_whitespace() {
            prev_char = Some(ch);
            break;
        }
        prev_position -= 1;
    }

    let has_entries = match prev_char {
        Some('{') | None => false,
        _ => true,
    };

    file.seek(SeekFrom::Start(position as u64))?;
    let mut insertion = String::new();
    if has_entries {
        insertion.push_str(",\n");
    } else {
        insertion.push('\n');
    }
    insertion.push_str(&entry);
    insertion.push('\n');
    insertion.push('}');
    insertion.push('\n');

    file.write_all(insertion.as_bytes())?;
    let len = file.stream_position()?;
    file.set_len(len)?;
    file.flush()?;
    Ok(())
}

fn write_full_detail_map(path: &str, data: &IndexMap<String, GameDetail>) -> Result<()> {
    let json = serde_json::to_string_pretty(data)?;
    fs::write(path, json)?;
    Ok(())
}

fn format_detail_entry(guid: &str, detail: &GameDetail) -> Result<String> {
    let pretty = serde_json::to_string_pretty(detail)?;
    let mut lines = pretty.lines();
    let Some(first_line) = lines.next() else {
        return Ok(format!("  \"{}\": {}", guid, pretty));
    };

    let mut entry = format!("  \"{}\": {}", guid, first_line);
    for line in lines {
        entry.push('\n');
        entry.push_str("  ");
        entry.push_str(line);
    }
    Ok(entry)
}

fn parse_image(v: &serde_json::Value) -> Option<Image> {
    if v.is_null() {
        return None;
    }
    Some(Image {
        original_url: v["original_url"].as_str().map(|s| s.to_string()),
        super_url: v["super_url"].as_str().map(|s| s.to_string()),
        small_url: v["small_url"].as_str().map(|s| s.to_string()),
    })
}

async fn parse_video_async(entry: serde_json::Value, api_key: &'static str) -> Option<Video> {
    if entry.is_null() {
        return None;
    }

    let site_detail_url = entry
        .get("site_detail_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let video_show = if let Some(show) = entry.get("video_show") {
        parse_video_show_from_value(show, api_key)
    } else {
        None
    };

    Some(Video {
        id: entry.get("id").and_then(|v| value_to_string(v)),
        guid: entry
            .get("guid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        name: entry
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        deck: entry
            .get("deck")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        hd_url: entry
            .get("hd_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        high_url: entry
            .get("high_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        low_url: entry
            .get("low_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        embed_player: entry
            .get("embed_player")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        length_seconds: entry.get("length_seconds").and_then(|v| parse_i64_value(v)),
        publish_date: entry
            .get("publish_date")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        site_detail_url: site_detail_url.clone(),
        url: entry
            .get("url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        api_detail_url: entry
            .get("api_detail_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        user: entry
            .get("user")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        video_type: entry
            .get("video_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        video_show,
        image: entry.get("image").and_then(parse_video_image_value),
        playable_url: site_detail_url
            .as_deref()
            .and_then(|url| build_playable_url(url, api_key)),
    })
}

async fn parse_video_show_async(
    entry: serde_json::Value,
    api_key: &'static str,
) -> Option<VideoShow> {
    if entry.is_null() {
        return None;
    }
    parse_video_show_from_value(&entry, api_key)
}

fn parse_video_show_from_value(
    value: &serde_json::Value,
    api_key: &'static str,
) -> Option<VideoShow> {
    if value.is_null() {
        return None;
    }

    let site_detail_url = value
        .get("site_detail_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(VideoShow {
        api_detail_url: value
            .get("api_detail_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        deck: value
            .get("deck")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        title: value
            .get("title")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        name: value
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        id: value.get("id").and_then(|v| value_to_string(v)),
        guid: value
            .get("guid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        site_detail_url: site_detail_url.clone(),
        image: value.get("image").and_then(parse_video_image_value),
        logo: value.get("logo").and_then(parse_video_image_value),
        position: value.get("position").and_then(|v| parse_i64_value(v)),
        active: value.get("active").and_then(|v| v.as_bool()),
        latest: value.get("latest").and_then(parse_video_light_value),
        playable_url: site_detail_url
            .as_deref()
            .and_then(|url| build_playable_url(url, api_key)),
    })
}

fn parse_video_light_value(value: &serde_json::Value) -> Option<VideoLight> {
    if value.is_null() {
        return None;
    }

    Some(VideoLight {
        api_detail_url: value
            .get("api_detail_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        deck: value
            .get("deck")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        guid: value
            .get("guid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        id: value.get("id").and_then(|v| value_to_string(v)),
        image: value.get("image").and_then(parse_video_image_value),
        length_seconds: value.get("length_seconds").and_then(|v| parse_i64_value(v)),
        name: value
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        publish_date: value
            .get("publish_date")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        site_detail_url: value
            .get("site_detail_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        url: value
            .get("url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        user: value
            .get("user")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        video_type: value
            .get("video_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

fn parse_video_image_value(value: &serde_json::Value) -> Option<VideoImage> {
    if value.is_null() {
        return None;
    }
    Some(VideoImage {
        icon_url: value
            .get("icon_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        small_url: value
            .get("small_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        medium_url: value
            .get("medium_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        super_url: value
            .get("super_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        thumb_url: value
            .get("thumb_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        tiny_url: value
            .get("tiny_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        screen_url: value
            .get("screen_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        screen_large_url: value
            .get("screen_large_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        original_url: value
            .get("original_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

fn parse_i64_value(value: &serde_json::Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        Some(n)
    } else if let Some(s) = value.as_str() {
        s.parse().ok()
    } else {
        None
    }
}

fn value_to_string(value: &serde_json::Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        Some(s.to_string())
    } else if let Some(n) = value.as_i64() {
        Some(n.to_string())
    } else {
        None
    }
}

fn parse_theme(v: &serde_json::Value) -> Option<Theme> {
    if v.is_null() {
        return None;
    }
    Some(Theme {
        id: v["id"].as_i64(),
        name: v["name"].as_str().map(|s| s.to_string()),
        api_detail_url: v["api_detail_url"].as_str().map(|s| s.to_string()),
        site_detail_url: v["site_detail_url"].as_str().map(|s| s.to_string()),
    })
}

fn parse_rating(v: &serde_json::Value) -> Option<Rating> {
    Some(Rating {
        name: v["name"].as_str().map(|s| s.to_string()),
    })
}

fn build_playable_url(url: &str, api_key: &str) -> Option<String> {
    if url.is_empty() {
        return None;
    }

    let mut normalized = url.to_string();
    if !normalized.ends_with('/') {
        normalized.push('/');
    }

    Some(format!("{normalized}?api={api_key}"))
}

fn build_api_detail_request_url(url: &str, api_key: &str) -> Option<String> {
    if url.is_empty() {
        return None;
    }

    let separator = if url.contains('?') { '&' } else { '?' };
    Some(format!("{url}{separator}api_key={api_key}&format=json"))
}

fn extract_parse_videos_target(args: &[String]) -> Option<String> {
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if let Some(rest) = arg.strip_prefix("--parse-videos=") {
            return Some(rest.to_string());
        }

        if arg == "--parse-videos" {
            let next = args.get(idx + 1);
            return match next {
                Some(val) if !val.starts_with("--") => Some(val.clone()),
                _ => Some(String::new()),
            };
        }

        idx += 1;
    }

    None
}

fn parse_videos_from_file(path: &str) -> Result<()> {
    println!("[parse] Loading video data from {path}.");
    let raw = fs::read_to_string(path)?;
    let map = parse_partial_map(&raw)?;

    let mut games_with_videos = 0usize;
    let mut total_videos = 0usize;

    for (guid, detail) in map.iter() {
        let Some(videos) = detail.videos.as_ref() else {
            continue;
        };
        if videos.is_empty() {
            continue;
        }

        games_with_videos += 1;
        total_videos += videos.len();

        println!(
            "[parse] {} ({}) has {} videos.",
            guid,
            detail.name.as_deref().unwrap_or("Unknown"),
            videos.len()
        );

        for video in videos {
            let playable = video
                .playable_url
                .as_deref()
                .or(video.high_url.as_deref())
                .or(video.hd_url.as_deref())
                .or(video.low_url.as_deref())
                .unwrap_or("<no playable url>");

            println!(
                "  - {} | guid={} | playable={}",
                video.name.as_deref().unwrap_or("<unnamed>"),
                video.guid.as_deref().unwrap_or("<no-guid>"),
                playable
            );

            if let Some(api_detail) = video.api_detail_url.as_deref() {
                println!("    api_detail_url: {api_detail}");
            }
        }
    }

    println!(
        "[parse] Completed. Games with videos: {} of {}. Total videos: {}.",
        games_with_videos,
        map.len(),
        total_videos
    );

    Ok(())
}

fn parse_partial_map(raw: &str) -> Result<IndexMap<String, GameDetail>> {
    match serde_json::from_str(raw) {
        Ok(map) => Ok(map),
        Err(primary_err) => {
            println!(
                "[recover] Primary deserialize failed ({}). Attempting multi-document recovery.",
                primary_err
            );

            let mut combined = IndexMap::new();
            let mut parsed_any = false;
            let mut stream =
                serde_json::Deserializer::from_str(raw).into_iter::<serde_json::Value>();

            while let Some(next) = stream.next() {
                match next {
                    Ok(value) => {
                        if !value.is_object() {
                            println!(
                                "[recover] Encountered non-object fragment while recovering partial file."
                            );
                            continue;
                        }
                        let fragment: IndexMap<String, GameDetail> = serde_json::from_value(value)?;
                        combined.extend(fragment);
                        parsed_any = true;
                    }
                    Err(err) if err.is_eof() => {
                        println!(
                            "[recover] Reached EOF mid-fragment while recovering partial file ({}). Ignoring trailing data.",
                            err
                        );
                        break;
                    }
                    Err(err) => return Err(err.into()),
                }
            }

            if parsed_any {
                Ok(combined)
            } else {
                Err(primary_err.into())
            }
        }
    }
}

fn resolve_merge_details_args(args: &[String], index: usize) -> Result<(String, Vec<String>)> {
    let current = &args[index];
    if let Some(rest) = current.strip_prefix("--merge-details=") {
        let tokens: Vec<_> = rest
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if tokens.len() < 2 {
            return Err(anyhow!(
                "--merge-details=OUTPUT,SOURCE1,... requires at least one source file"
            ));
        }
        let output = tokens[0].clone();
        let sources = tokens[1..].to_vec();
        return Ok((output, sources));
    }

    let output = args.get(index + 1).cloned().ok_or_else(|| {
        anyhow!("--merge-details requires an output path followed by one or more source files")
    })?;

    let mut sources = Vec::new();
    let mut cursor = index + 2;
    while cursor < args.len() {
        let value = &args[cursor];
        if value.starts_with("--") {
            break;
        }
        sources.push(value.clone());
        cursor += 1;
    }

    if sources.is_empty() {
        return Err(anyhow!("--merge-details requires at least one source file"));
    }

    Ok((output, sources))
}

fn merge_detail_files(output: &str, sources: &[String]) -> Result<()> {
    if sources.is_empty() {
        return Err(anyhow!("--merge-details requires at least one source file"));
    }

    let mut merged: IndexMap<String, GameDetail> = IndexMap::new();

    for source in sources {
        println!("[merge] Loading details from {source}.");
        let raw =
            fs::read_to_string(source).map_err(|err| anyhow!("Failed to read {source}: {err}"))?;
        let map =
            parse_partial_map(&raw).map_err(|err| anyhow!("Failed to parse {source}: {err}"))?;
        println!("[merge] {} entries read from {source}.", map.len());

        for (guid, mut detail) in map {
            if detail.guid.is_empty() {
                detail.guid = guid.clone();
            }

            normalize_game_detail(&mut detail);

            match merged.entry(guid.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(detail);
                }
                Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    normalize_game_detail(existing);
                    merge_game_detail(existing, detail);
                }
            }
        }
    }

    println!(
        "[merge] Writing {} merged entries to {}.",
        merged.len(),
        output
    );
    write_full_detail_map(output, &merged)?;

    Ok(())
}

fn merge_game_detail(existing: &mut GameDetail, incoming: GameDetail) {
    let GameDetail {
        guid: _,
        name,
        description,
        image,
        images,
        video_shows,
        themes,
        videos,
        video_api_payloads,
        original_game_rating,
        raw_results,
    } = incoming;

    merge_string_field(&mut existing.name, name);
    merge_string_field(&mut existing.description, description);
    merge_image_option(&mut existing.image, image);
    merge_vec_option(&mut existing.images, images);
    merge_vec_option(&mut existing.video_shows, video_shows);
    merge_vec_option(&mut existing.themes, themes);
    merge_vec_option(&mut existing.videos, videos);
    merge_vec_option(&mut existing.video_api_payloads, video_api_payloads);
    merge_vec_option(&mut existing.original_game_rating, original_game_rating);

    if should_replace_raw_results(&existing.raw_results, &raw_results) {
        existing.raw_results = raw_results;
    }
}

fn merge_string_field(target: &mut Option<String>, incoming: Option<String>) {
    let needs_replacement = match target {
        None => true,
        Some(current) => current.trim().is_empty(),
    };

    if needs_replacement {
        if let Some(value) = incoming {
            if !value.trim().is_empty() {
                *target = Some(value);
            }
        }
    }
}

fn merge_image_option(target: &mut Option<Image>, incoming: Option<Image>) {
    match incoming {
        None => {}
        Some(mut img) => match target {
            None => *target = Some(img),
            Some(existing) => {
                if existing.original_url.is_none() {
                    existing.original_url = img.original_url.take();
                }
                if existing.super_url.is_none() {
                    existing.super_url = img.super_url.take();
                }
                if existing.small_url.is_none() {
                    existing.small_url = img.small_url.take();
                }
            }
        },
    }
}

fn merge_vec_option<T>(target: &mut Option<Vec<T>>, incoming: Option<Vec<T>>)
where
    T: Serialize,
{
    if let Some(vec) = incoming {
        if let Some(existing) = target.as_mut() {
            extend_dedupe_serialized(existing, vec);
        } else {
            let mut unique = vec;
            dedupe_serialized_in_place(&mut unique);
            *target = Some(unique);
        }
    }
}

fn extend_dedupe_serialized<T: Serialize>(existing: &mut Vec<T>, mut incoming: Vec<T>) {
    let mut seen = HashSet::new();
    for item in existing.iter() {
        if let Ok(key) = serde_json::to_string(item) {
            seen.insert(key);
        }
    }

    for item in incoming.drain(..) {
        match serde_json::to_string(&item) {
            Ok(key) => {
                if seen.insert(key) {
                    existing.push(item);
                }
            }
            Err(_) => existing.push(item),
        }
    }
}

fn dedupe_serialized_in_place<T: Serialize>(items: &mut Vec<T>) {
    let mut seen = HashSet::new();
    items.retain(|item| match serde_json::to_string(item) {
        Ok(key) => seen.insert(key),
        Err(_) => true,
    });
}

fn normalize_game_detail(detail: &mut GameDetail) {
    if let Some(images) = detail.images.as_mut() {
        dedupe_serialized_in_place(images);
    }
    if let Some(video_shows) = detail.video_shows.as_mut() {
        dedupe_serialized_in_place(video_shows);
    }
    if let Some(themes) = detail.themes.as_mut() {
        dedupe_serialized_in_place(themes);
    }
    if let Some(videos) = detail.videos.as_mut() {
        dedupe_serialized_in_place(videos);
    }
    if let Some(payloads) = detail.video_api_payloads.as_mut() {
        dedupe_serialized_in_place(payloads);
    }
    if let Some(ratings) = detail.original_game_rating.as_mut() {
        dedupe_serialized_in_place(ratings);
    }
}

fn should_replace_raw_results(current: &serde_json::Value, incoming: &serde_json::Value) -> bool {
    if incoming.is_null() {
        return false;
    }

    if current.is_null() {
        return true;
    }

    let current_empty = current
        .as_object()
        .map(|map| map.is_empty())
        .unwrap_or(false);
    let incoming_empty = incoming
        .as_object()
        .map(|map| map.is_empty())
        .unwrap_or(false);

    current_empty && !incoming_empty
}

fn install_shutdown_handler(paths: Vec<String>) {
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("[shutdown] Ctrl+C detected; deduplicating detail caches.");
            }
            Err(err) => {
                println!(
                    "[shutdown] Failed to install Ctrl+C handler: {err:?}. Deduplication skipped."
                );
                return;
            }
        }

        for path in paths {
            if !Path::new(&path).exists() {
                println!("[shutdown] Skipping {path}: file does not exist.");
                continue;
            }

            match dedupe_detail_file(&path) {
                Ok(count) => {
                    println!(
                        "[shutdown] Completed deduplication for {path}; {} entries written.",
                        count
                    );
                }
                Err(err) => {
                    println!("[shutdown] Failed deduplication for {path}: {err:?}");
                }
            }
        }

        println!("[shutdown] Deduplication finished; exiting.");
        std::process::exit(0);
    });
}

fn dedupe_detail_file(path: &str) -> Result<usize> {
    println!("[shutdown] Loading {path} for deduplication.");
    let raw = fs::read_to_string(path)?;
    let mut map = parse_partial_map(&raw)?;

    for (guid, detail) in map.iter_mut() {
        if detail.guid.is_empty() {
            detail.guid = guid.clone();
        }
        normalize_game_detail(detail);
    }

    write_full_detail_map(path, &map)?;
    Ok(map.len())
}
