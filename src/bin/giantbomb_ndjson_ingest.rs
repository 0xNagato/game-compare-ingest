//! High-throughput GiantBomb NDJSON bulk ingester (multi-pass, minimal ensure_* usage)
//!
//! Strategy (graph known in advance):
//! 1. Pass 1: Scan NDJSON; collect unique platforms, products (games), and retain lightweight records.
//! 2. Bulk upsert platforms (name/code) -> id map.
//! 3. Bulk upsert products + software rows -> id map.
//! 4. Bulk insert video_game_titles (1:1 with software products) using RETURNING.
//! 5. Bulk create video_game instances per platform (title_id, platform_id).
//! 6. (Optional) Bulk media/video ingestion placeholders.
//! 7. Later passes (prices, ratings) can stream using existing ids.
//!
//! This avoids per-row ensure_* round trips and favors set-oriented SQL with ON CONFLICT.
//! For extreme dataset sizes, replace in-memory Vec with an on-disk staging table (COPY then SQL joins).

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env;
use serde::Deserialize;
use sqlx::{QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
};
use tracing::{info, warn};

#[derive(Clone, Copy, Debug)]
enum SoftwareKeyColumn {
    ProductId,
    VideoGameId,
}

impl SoftwareKeyColumn {
    fn as_str(self) -> &'static str {
        match self {
            SoftwareKeyColumn::ProductId => "product_id",
            SoftwareKeyColumn::VideoGameId => "video_game_id",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TitleLinkColumn {
    ProductId,
    VideoGameId,
}

impl TitleLinkColumn {
    fn as_str(self) -> &'static str {
        match self {
            TitleLinkColumn::ProductId => "product_id",
            TitleLinkColumn::VideoGameId => "video_game_id",
        }
    }
}

#[derive(Debug)]
struct SchemaHints {
    software_column: SoftwareKeyColumn,
    title_link_column: TitleLinkColumn,
}

async fn detect_schema_hints(db: &Db) -> Result<SchemaHints> {
    let software_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = ANY(current_schemas(true)) \
               AND table_name = 'software'",
    )
    .persistent(false)
    .fetch_all(&db.pool)
    .await?;
    let mut software_set: HashSet<String> = HashSet::new();
    for col in software_columns {
        software_set.insert(col.to_ascii_lowercase());
    }
    let software_column = if software_set.contains("product_id") {
        SoftwareKeyColumn::ProductId
    } else if software_set.contains("video_game_id") {
        SoftwareKeyColumn::VideoGameId
    } else {
        bail!("software table missing product/video_game id column");
    };

    let title_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = ANY(current_schemas(true)) \
               AND table_name = 'video_game_titles'",
    )
    .persistent(false)
    .fetch_all(&db.pool)
    .await?;
    let mut title_set: HashSet<String> = HashSet::new();
    for col in title_columns {
        title_set.insert(col.to_ascii_lowercase());
    }
    let title_link_column = if title_set.contains("product_id") {
        TitleLinkColumn::ProductId
    } else if title_set.contains("video_game_id") {
        TitleLinkColumn::VideoGameId
    } else {
        bail!("video_game_titles table missing product/video_game id column");
    };

    Ok(SchemaHints {
        software_column,
        title_link_column,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("giantbomb_ndjson_ingest");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let ndjson_path =
        std::env::var("GB_NDJSON").unwrap_or_else(|_| "games_detailed_partial.ndjson".into());
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SUPABASE_DB_URL"))
        .context("DATABASE_URL or SUPABASE_DB_URL required")?;
    let max_conns = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&db_url, max_conns).await?;

    let schema_hints = detect_schema_hints(&db).await?;
    info!(?schema_hints, "detected schema hints for giantbomb ingest");

    let records = load_giantbomb(&ndjson_path)?;
    info!(
        count = records.len(),
        "loaded NDJSON records into memory (first pass)"
    );

    // Collect unique platforms & products
    let mut platform_names: HashSet<String> = HashSet::new();
    for r in &records {
        for p in r.platforms.iter() {
            platform_names.insert(p.name.clone());
        }
    }
    info!(
        platforms = platform_names.len(),
        "unique platforms detected"
    );

    // Bulk upsert platforms -> id map
    let platform_id_map = bulk_upsert_platforms(&db, &platform_names).await?;
    info!(mapped = platform_id_map.len(), "platform ids mapped");

    // Bulk upsert products & software rows (batched)
    let product_id_map =
        bulk_upsert_products_and_software(&db, &records, schema_hints.software_column).await?;
    info!(products = product_id_map.len(), "product ids mapped");

    // Bulk insert titles (1:1 product→title) using original display name
    let title_id_map = bulk_upsert_titles(
        &db,
        &records,
        &product_id_map,
        schema_hints.title_link_column,
    )
    .await?;
    info!(titles = title_id_map.len(), "title ids mapped");

    // Bulk create video_game instances per platform
    let vg_count = bulk_upsert_video_games(
        &db,
        &records,
        &product_id_map,
        &title_id_map,
        &platform_id_map,
    )
    .await?;
    info!(video_games = vg_count, "video game instances written");

    // Placeholder media ingestion (images only) – extend later
    let media_rows =
        collect_media_placeholders(&records, &product_id_map, &title_id_map, &platform_id_map);
    info!(
        media_candidates = media_rows.len(),
        "media placeholder rows prepared (not persisted)"
    );
    // TODO: Implement bulk media upsert using provider/media tables if required.

    info!("giantbomb ndjson ingest complete");
    Ok(())
}

// ---------- Data Shapes ----------

#[derive(Debug, Deserialize)]
struct GbPlatform {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GbImage {
    original_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GbVideo {
    hd_url: Option<String>,
    high_url: Option<String>,
    low_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GbRecord {
    name: String,

    platforms: Vec<GbPlatform>,
    images: Option<Vec<GbImage>>,
    videos: Option<Vec<GbVideo>>,
}

fn load_giantbomb(path: &str) -> Result<Vec<GbRecord>> {
    let file = File::open(path).context("open ndjson source")?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<GbRecord>(&line) {
            Ok(rec) => out.push(rec),
            Err(e) => warn!(error=%e, "skipping malformed record"),
        }
    }
    Ok(out)
}

// ---------- Helpers ----------

fn simple_slug(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if matches!(ch, '-' | '_' | ' ' | '/' | ':' | '.' | '\'' | '"') {
            if !last_dash && !slug.is_empty() {
                slug.push('-');
                last_dash = true;
            }
        }
    }
    slug.trim_matches('-').to_string()
}

// Derive a coarse platform family for grouping related generations/variants.
// This is intentionally conservative: only well-known console lineages + PC.
fn classify_family(code: &str, name: &str) -> Option<String> {
    let lc = code.to_ascii_lowercase();
    // Prefer code; fallback to name slug when code is too generic.
    let basis = if lc.is_empty() {
        simple_slug(name)
    } else {
        lc.clone()
    };
    // Ordered checks – match more specific prefixes first where needed.
    if basis.starts_with("playstation") || basis.starts_with("ps") {
        return Some("playstation".into());
    }
    if basis.starts_with("xbox") {
        return Some("xbox".into());
    }
    if basis.starts_with("nintendo") || basis.starts_with("supernintendo") {
        return Some("nintendo".into());
    }
    if basis == "pc" || basis.contains("windows") {
        // broad PC bucket
        return Some("pc".into());
    }
    None
}

fn parse_date(raw: &str) -> Option<NaiveDate> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    NaiveDate::parse_from_str(t, "%Y-%m-%d").ok()
}

// ---------- Bulk Upserts ----------

async fn bulk_upsert_platforms(db: &Db, names: &HashSet<String>) -> Result<HashMap<String, i64>> {
    if names.is_empty() {
        return Ok(HashMap::new());
    }
    let batch_size: usize = std::env::var("GB_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let mut name_vec: Vec<String> = names.iter().cloned().collect();
    name_vec.sort();
    let mut map = HashMap::new();
    for chunk in name_vec.chunks(batch_size) {
        let mut qb = QueryBuilder::<sqlx::Postgres>::new(
            "WITH incoming(name, code, family, canonical_code) AS (VALUES ",
        );
        let mut first = true;
        for n in chunk {
            let mut code = simple_slug(n);
            // Normalize PlayStation generations to full form to avoid reintroducing short codes.
            // Accept variants like ps4, playstation4, ps-4 etc.
            if code.starts_with("ps4") || code == "playstation4" || code == "ps-4" {
                code = "playstation-4".into();
            } else if code.starts_with("ps5") || code == "playstation5" || code == "ps-5" {
                code = "playstation-5".into();
            }
            // Ensure name aligns with normalized code for canonical rows (non-destructive if already normalized).
            let norm_name = match code.as_str() {
                "playstation-4" => "PlayStation 4",
                "playstation-5" => "PlayStation 5",
                _ => n,
            };
            let family = classify_family(&code, n);
            // canonical_code strips non-alnum to allow stricter uniqueness in later stages.
            let canonical_code = code
                .chars()
                .filter(|c| c.is_ascii_alphanumeric())
                .collect::<String>();
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push("(")
                .push_bind(norm_name.to_string()) // normalized name if mapped
                .push(", ")
                .push_bind(code.clone()) // code (owned)
                .push(", ")
                .push_bind(family.clone()) // family Option<String> (owned)
                .push(", ")
                .push_bind(canonical_code.clone()) // canonical_code (owned)
                .push(")");
        }
        qb.push(
            ") INSERT INTO platforms(name, code, family, canonical_code) SELECT name, code, family, canonical_code FROM incoming ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, family=COALESCE(EXCLUDED.family, platforms.family), canonical_code=EXCLUDED.canonical_code, updated_at=now() RETURNING id, name"
        );
        let rows = qb.build().fetch_all(&db.pool).await?;
        for r in rows {
            let id: i64 = r.get("id");
            let name: String = r.get("name");
            map.insert(name, id);
        }
        info!(
            batch = chunk.len(),
            accumulated = map.len(),
            "platform batch upsert complete"
        );
    }
    Ok(map)
}

async fn bulk_upsert_products_and_software(
    db: &Db,
    records: &[GbRecord],
    software_column: SoftwareKeyColumn,
) -> Result<HashMap<String, i64>> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut unique: Vec<&GbRecord> = Vec::new();
    for r in records {
        let slug = simple_slug(&r.name);
        if seen.insert(slug.clone()) {
            unique.push(r);
        }
    }
    let batch_size: usize = std::env::var("GB_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let mut map: HashMap<String, i64> = HashMap::new();
    for chunk in unique.chunks(batch_size) {
        let mut qb = QueryBuilder::<sqlx::Postgres>::new(
            "WITH incoming(slug,name,platform,category) AS (VALUES ",
        );
        let mut first = true;
        for rec in chunk {
            let slug = simple_slug(&rec.name);
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push("(")
                // Use owned values so they live long enough for QueryBuilder
                .push_bind(slug)
                .push(", ")
                .push_bind(rec.name.clone())
                .push(", ")
                // Laravel schema requires products.platform (NOT NULL). GB is catalog-only: use a safe default.
                .push_bind("unknown")
                .push(", ")
                .push_bind("software")
                .push(")");
        }
        qb.push(
            ") INSERT INTO products(slug,name,platform,category) SELECT slug,name,platform,category FROM incoming ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, updated_at=now() RETURNING id, slug"
        );
        let rows = qb.build().fetch_all(&db.pool).await?;
        for r in rows {
            let id: i64 = r.get("id");
            let slug: String = r.get("slug");
            map.insert(slug, id);
        }
        info!(
            batch = chunk.len(),
            accumulated = map.len(),
            "product batch upsert complete"
        );
    }
    // Insert software rows (ignore existing)
    if !map.is_empty() {
        let column_name = software_column.as_str();
        let mut qb_sw = QueryBuilder::<sqlx::Postgres>::new(format!(
            "INSERT INTO software({column_name}) VALUES "
        ));
        let mut first_sw = true;
        for (_slug, id) in &map {
            if !first_sw {
                qb_sw.push(", ");
            }
            first_sw = false;
            qb_sw.push("(").push_bind(id).push(")");
        }
        qb_sw.push(" ON CONFLICT DO NOTHING");
        qb_sw.build().execute(&db.pool).await?;
    }
    Ok(map)
}

async fn bulk_upsert_titles(
    db: &Db,
    records: &[GbRecord],
    product_id_map: &HashMap<String, i64>,
    title_link_column: TitleLinkColumn,
) -> Result<HashMap<i64, i64>> {
    if product_id_map.is_empty() {
        return Ok(HashMap::new());
    }
    let batch_size: usize = std::env::var("GB_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let mut map: HashMap<i64, i64> = HashMap::new();
    let mut pairs: Vec<(i64, String, String)> = Vec::new();
    for r in records {
        let slug = simple_slug(&r.name);
        if let Some(pid) = product_id_map.get(&slug) {
            let normalized = simple_slug(&r.name);
            pairs.push((*pid, r.name.clone(), normalized));
        }
    }
    let fk_column = title_link_column.as_str();
    let conflict_column = fk_column;
    for chunk in pairs.chunks(batch_size) {
        let mut qb = QueryBuilder::<sqlx::Postgres>::new(format!(
            "WITH incoming({fk_column},title,normalized_title) AS (VALUES "
        ));
        let mut first = true;
        for (pid, title, norm) in chunk {
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push("(")
                .push_bind(pid)
                .push(", ")
                .push_bind(title)
                .push(", ")
                .push_bind(norm)
                .push(")");
        }
        qb.push(format!(
            ") INSERT INTO video_game_titles({fk_column},title,normalized_title) \
             SELECT {fk_column},title,normalized_title FROM incoming \
             ON CONFLICT ({conflict_column}) DO UPDATE SET \
               title=EXCLUDED.title, \
               normalized_title=EXCLUDED.normalized_title, \
               updated_at=now() \
             RETURNING id, {fk_column} AS link_value"
        ));
        let rows = qb.build().fetch_all(&db.pool).await?;
        for r in rows {
            let id: i64 = r.get("id");
            let fk: i64 = r.get("link_value");
            map.insert(fk, id);
        }
        info!(
            batch = chunk.len(),
            accumulated = map.len(),
            "title batch upsert complete"
        );
    }
    Ok(map)
}

async fn bulk_upsert_video_games(
    db: &Db,
    records: &[GbRecord],
    product_id_map: &HashMap<String, i64>,
    title_id_map: &HashMap<i64, i64>,
    platform_id_map: &HashMap<String, i64>,
) -> Result<usize> {
    // Build distinct (title_id, platform_id) pairs
    let mut pairs: HashSet<(i64, i64)> = HashSet::new();
    for r in records {
        let prod_slug = simple_slug(&r.name);
        let Some(prod_id) = product_id_map.get(&prod_slug) else {
            continue;
        };
        let Some(title_id) = title_id_map.get(prod_id) else {
            continue;
        };
        for p in &r.platforms {
            if let Some(platform_id) = platform_id_map.get(&p.name) {
                pairs.insert((*title_id, *platform_id));
            }
        }
    }
    if pairs.is_empty() {
        return Ok(0);
    }
    let mut qb =
        QueryBuilder::<sqlx::Postgres>::new("WITH incoming(title_id,platform_id) AS (VALUES ");
    let mut first = true;
    for (t, p) in &pairs {
        if !first {
            qb.push(", ");
        }
        first = false;
        qb.push("(").push_bind(t).push(", ").push_bind(p).push(")");
    }
    qb.push(
        ") INSERT INTO video_games(title_id,platform_id) SELECT title_id,platform_id FROM incoming ON CONFLICT (title_id,platform_id,COALESCE(edition,'')) DO NOTHING RETURNING id"
    );
    let rows = qb.build().fetch_all(&db.pool).await?;
    Ok(rows.len())
}

#[derive(Debug)]
struct MediaPlaceholder {
    kind: String,
    url: String,
}

fn collect_media_placeholders(
    records: &[GbRecord],
    _product_map: &HashMap<String, i64>,
    _title_map: &HashMap<i64, i64>,
    _platform_map: &HashMap<String, i64>,
) -> Vec<MediaPlaceholder> {
    let mut out = Vec::new();
    for r in records {
        if let Some(images) = &r.images {
            for img in images {
                if let Some(url) = img.original_url.as_ref() {
                    out.push(MediaPlaceholder {
                        kind: "image".into(),
                        url: url.clone(),
                    });
                }
            }
        }
        if let Some(videos) = &r.videos {
            for v in videos {
                if let Some(url) = v
                    .hd_url
                    .as_ref()
                    .or(v.high_url.as_ref())
                    .or(v.low_url.as_ref())
                {
                    out.push(MediaPlaceholder {
                        kind: "video".into(),
                        url: url.clone(),
                    });
                }
            }
        }
    }
    out
}
// (Legacy ingestion path removed – superseded by bulk multi-pass ingester above)
