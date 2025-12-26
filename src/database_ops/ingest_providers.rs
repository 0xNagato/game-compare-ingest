use crate::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use crate::database_ops::exchange::ExchangeService;
use anyhow::{anyhow, Result};
use serde_json::Value;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use tokio::sync::OnceCell;
use std::sync::{Mutex};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

static PROVIDER_KIND_SUPPORTED: OnceCell<bool> = OnceCell::const_new();
static PHP_COMPAT_MODE: OnceCell<bool> = OnceCell::const_new();
static OFFER_COMPAT: OnceCell<(std::sync::atomic::AtomicI64, Mutex<HashMap<i64, (i64, i64, Option<String>)>>)> = OnceCell::const_new();
static COUNTRY_SCHEMA: OnceCell<CountrySchema> = OnceCell::const_new();
static JURISDICTIONS_PRESENT: OnceCell<bool> = OnceCell::const_new();

/// Detect whether the legacy PHP schema is present (sku_regions/region_prices) and usable.
async fn table_exists(db: &Db, name: &str) -> Result<bool> {
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT TRUE FROM information_schema.tables WHERE table_schema = ANY (current_schemas(true)) AND table_name = $1 LIMIT 1",
    )
    .persistent(false)
    .bind(name)
    .fetch_optional(&db.pool)
    .await?;
    Ok(exists.unwrap_or(false))
}

async fn php_compat_schema(db: &Db) -> Result<bool> {
    let compat = PHP_COMPAT_MODE
        .get_or_try_init(|| async {
            let sku_regions = table_exists(db, "sku_regions").await.unwrap_or(false);
            if !sku_regions {
                return Ok::<bool, anyhow::Error>(false);
            }
            // Supporting tables are optional in legacy-only environments; warn if absent but keep compat on so callers can
            // take a best-effort path against sku_regions alone.
            let countries = table_exists(db, "countries").await.unwrap_or(false);
            let jurisdictions = table_exists(db, "jurisdictions").await.unwrap_or(false);
            if !countries || !jurisdictions {
                warn!(countries, jurisdictions, "php compat: supporting tables partially missing; continuing with sku_regions-only mapping");
            }
            Ok(true)
        })
        .await?;
    if *compat {
        debug!("php compatibility mode enabled (sku_regions detected)");
    }
    Ok(*compat)
}

async fn provider_kind_supported(db: &Db) -> Result<bool> {
    let supported = PROVIDER_KIND_SUPPORTED
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar::<_, bool>(
                "SELECT TRUE FROM information_schema.columns \
                     WHERE table_name = 'video_game_sources' AND column_name = 'category' \
                       AND table_schema = ANY (current_schemas(true)) LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            let supported = exists.unwrap_or(false);
            if supported {
                debug!("video_game_sources.category column detected");
            } else {
                warn!(
                    "video_game_sources.category column missing — falling back to legacy schema compatibility"
                );
            }
            Ok::<bool, anyhow::Error>(supported)
        })
        .await?;
    Ok(*supported)
}

pub struct IngestResult {
    pub offer_jurisdiction_ids: Vec<i64>,
    pub current_updates: Vec<CurrentPriceRow>,
}

#[derive(Debug, Default)]
pub struct PostIngestSummary {
    pub provider_item_ids: HashSet<i64>,
    pub offer_jurisdiction_ids: HashSet<i64>,
    pub total_price_rows_written: usize,
    pub total_current_updates: usize,
    pub bundle_rows_ingested: usize,
    pub bundle_rows_skipped: usize,
    pub bundle_offer_jurisdictions_ingested: HashSet<i64>,
    pub bundle_offer_jurisdictions_skipped: HashSet<i64>,
}

impl PostIngestSummary {
    pub fn record_provider_item(&mut self, provider_item_id: i64) {
        self.provider_item_ids.insert(provider_item_id);
    }

    pub fn record_provider_items<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = i64>,
    {
        self.provider_item_ids.extend(iter);
    }

    pub fn record_batch(&mut self, batch_len: usize, ingest_result: &IngestResult) {
        self.total_price_rows_written += batch_len;
        self.total_current_updates += ingest_result.current_updates.len();
        self.offer_jurisdiction_ids
            .extend(ingest_result.offer_jurisdiction_ids.iter().copied());
    }

    pub fn record_bundle_ingest(&mut self, offer_jurisdiction_id: i64) {
        self.bundle_rows_ingested += 1;
        self.bundle_offer_jurisdictions_ingested
            .insert(offer_jurisdiction_id);
    }

    pub fn record_bundle_skip(&mut self, offer_jurisdiction_id: i64) {
        self.bundle_rows_skipped += 1;
        self.bundle_offer_jurisdictions_skipped
            .insert(offer_jurisdiction_id);
    }

    pub async fn verify(&self, db: &Db, provider_id: i64) -> Result<()> {
        verify_post_ingest(
            db,
            provider_id,
            &self.provider_item_ids,
            &self.offer_jurisdiction_ids,
            self.total_price_rows_written,
            self.total_current_updates,
        )
        .await?;

        info!(
            provider_id,
            bundle_rows_ingested = self.bundle_rows_ingested,
            bundle_rows_skipped = self.bundle_rows_skipped,
            bundle_offer_jurisdictions_ingested = self.bundle_offer_jurisdictions_ingested.len(),
            bundle_offer_jurisdictions_skipped = self.bundle_offer_jurisdictions_skipped.len(),
            "bundle price coverage summary"
        );
        Ok(())
    }
}

pub async fn verify_post_ingest(
    db: &Db,
    provider_id: i64,
    provider_item_ids: &HashSet<i64>,
    offer_jurisdiction_ids: &HashSet<i64>,
    total_price_rows_written: usize,
    total_current_updates: usize,
) -> Result<()> {
    if total_price_rows_written == 0 {
        warn!(provider_id, "no price rows were written during ingest");
    } else {
        info!(
            provider_id,
            price_rows = total_price_rows_written,
            current_updates = total_current_updates,
            "price rows flushed and current_price upserts attempted"
        );
    }

    if !offer_jurisdiction_ids.is_empty() {
        let oj_vec: Vec<i64> = offer_jurisdiction_ids.iter().copied().collect();
        let current_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM current_price WHERE offer_jurisdiction_id = ANY($1)",
        )
        .persistent(false)
        .bind(&oj_vec)
        .fetch_one(&db.pool)
        .await?;
        if current_count < (offer_jurisdiction_ids.len() as i64) {
            warn!(
                provider_id,
                expected = offer_jurisdiction_ids.len(),
                actual = current_count,
                "current_price propagation incomplete"
            );
        } else {
            info!(
                provider_id,
                current_entries = current_count,
                expected = offer_jurisdiction_ids.len(),
                "current_price propagation verified"
            );
        }
    } else {
        info!(
            provider_id,
            "no offer jurisdictions touched; skipping price propagation verification"
        );
    }

    if !provider_item_ids.is_empty() {
        let items_vec: Vec<i64> = provider_item_ids.iter().copied().collect();
        let media_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_media_links WHERE provider_item_id = ANY($1)",
        )
        .persistent(false)
        .bind(&items_vec)
        .fetch_one(&db.pool)
        .await?;
        let media_nulls: i64 = sqlx
            ::query_scalar(
                "SELECT COUNT(*) FROM provider_media_links WHERE provider_item_id = ANY($1) AND url IS NULL"
            )
            .persistent(false)
            .bind(&items_vec)
            .fetch_one(&db.pool).await?;
        if media_total == 0 {
            info!(
                provider_id,
                "no provider media links associated with processed items; upstream feed may lack media"
            );
        } else if media_nulls > 0 {
            warn!(
                provider_id,
                missing = media_nulls,
                total = media_total,
                "provider media links contain NULL urls"
            );
        } else {
            info!(
                provider_id,
                media_links = media_total,
                "provider media propagation verified"
            );
        }
    } else {
        info!(
            provider_id,
            "no provider items processed; skipping media verification"
        );
    }

    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_provider(db: &Db, name: &str, kind: &str, slug: Option<&str>) -> Result<i64> {
    if php_compat_schema(db).await.unwrap_or(false) {
        // PHP schema: game_providers(provider_key unique)
        let key = slug.unwrap_or(name);
        if let Some(rec) = sqlx::query("SELECT id FROM game_providers WHERE provider_key = $1")
            .persistent(false)
            .bind(key)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
        // Upsert on the unique composite (provider_key, providable_type, providable_id)
        // to avoid race-induced 23505 errors and return the existing id in one round-trip.
        // First attempt: insert with ON CONFLICT DO NOTHING (handles duplicate PK/unique quietly)
        if let Some(row) = sqlx::query(
            "INSERT INTO game_providers (provider_key, name, slug, providable_type, providable_id) \
             VALUES ($1,$2,$3,$4,$5) \
             ON CONFLICT DO NOTHING \
             RETURNING id",
        )
        .persistent(false)
        .bind(key)
        .bind(name)
        .bind(slug)
        .bind("App\\\\Models\\\\GamesProvider")
        .bind(0_i64)
        .fetch_optional(&db.pool)
        .await?
        {
            return Ok(row.get("id"));
        }

        // If the insert did nothing (common when the sequence is behind existing IDs), bump the sequence once and retry.
        let _ = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT setval(pg_get_serial_sequence('game_providers','id'), (SELECT COALESCE(MAX(id),0) FROM game_providers))"
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?;

        if let Some(row) = sqlx::query(
            "INSERT INTO game_providers (provider_key, name, slug, providable_type, providable_id) \
             VALUES ($1,$2,$3,$4,$5) \
             ON CONFLICT DO NOTHING \
             RETURNING id",
        )
        .persistent(false)
        .bind(key)
        .bind(name)
        .bind(slug)
        .bind("App\\\\Models\\\\GamesProvider")
        .bind(0_i64)
        .fetch_optional(&db.pool)
        .await?
        {
            return Ok(row.get("id"));
        }

        // Final fallback: fetch the existing row by provider_key (covers RLS-visible rows or concurrent insertions).
        let rec = sqlx::query("SELECT id FROM game_providers WHERE provider_key = $1")
            .persistent(false)
            .bind(key)
            .fetch_optional(&db.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("game_providers exists but no row found for provider_key {}", key))?;
        return Ok(rec.get("id"));
    }

    let has_kind_col = provider_kind_supported(db).await.unwrap_or(false);

    // Try by display_name first
    let rec = sqlx::query("SELECT id FROM video_game_sources WHERE display_name = $1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?;
    if let Some(r) = rec {
        debug!(provider_name=%name, provider_id=r.get::<i64, _>("id"), "video_game_source exists via display_name");
        return Ok(r.get::<i64, _>("id"));
    }

    // Try by slug if provided
    if let Some(s) = slug {
        if let Some(r) = sqlx::query("SELECT id FROM video_game_sources WHERE slug = $1")
            .persistent(false)
            .bind(s)
            .fetch_optional(&db.pool)
            .await?
        {
            debug!(provider_slug=%s, provider_id=r.get::<i64, _>("id"), "video_game_source exists via slug");
            return Ok(r.get::<i64, _>("id"));
        }
    }

    // Insert fresh row (no ON CONFLICT because target schema lacks unique constraint on slug)
    let inserted = if has_kind_col {
        sqlx::query(
            "INSERT INTO video_game_sources (display_name, category, slug, provider_key) VALUES ($1,$2,$3,$4) RETURNING id",
        )
        .persistent(false)
        .bind(name)
        .bind(kind)
        .bind(slug)
        .bind(slug.unwrap_or(name))
        .fetch_one(&db.pool)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO video_game_sources (display_name, slug, provider_key) VALUES ($1,$2,$3) RETURNING id",
        )
        .persistent(false)
        .bind(name)
        .bind(slug)
        .bind(slug.unwrap_or(name))
        .fetch_one(&db.pool)
        .await?
    };

    debug!(provider_name=%name, provider_slug=?slug, provider_kind=%kind, provider_id=inserted.get::<i64,_>("id"), has_kind_col, "video_game_source inserted");
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_retailer(db: &Db, name: &str, slug: Option<&str>) -> Result<i64> {
    if php_compat_schema(db).await.unwrap_or(false) {
        let key = slug.unwrap_or(name);
        // Legacy schema expects game_provider_id NOT NULL; reuse/create a matching game_provider first.
        let game_provider_id = ensure_provider(db, name, "storefront", slug).await?;
        if let Some(rec) = sqlx::query("SELECT id FROM game_retailers WHERE retailer_key = $1")
            .persistent(false)
            .bind(key)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
        let rec = sqlx::query(
            "INSERT INTO game_retailers (game_provider_id, retailer_key, name, slug) VALUES ($1,$2,$3,$4) RETURNING id",
        )
        .persistent(false)
        .bind(game_provider_id)
        .bind(key)
        .bind(name)
        .bind(slug)
        .fetch_one(&db.pool)
        .await?;
        return Ok(rec.get("id"));
    }

    if let Some(rec) = sqlx::query("SELECT id FROM retailers WHERE name = $1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }

    // Try by slug if provided
    if let Some(s) = slug {
        if let Some(r) = sqlx::query("SELECT id FROM retailers WHERE slug = $1")
            .persistent(false)
            .bind(s)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(r.get("id"));
        }
    }

    // Insert with ON CONFLICT to handle duplicate slug gracefully
    let inserted = sqlx::query(
        "INSERT INTO retailers (name, slug) VALUES ($1,$2) \
             ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name \
             WHERE retailers.slug IS NOT NULL \
             RETURNING id",
    )
    .persistent(false)
    .bind(name)
    .bind(slug)
    .fetch_optional(&db.pool)
    .await?;

    if let Some(row) = inserted {
        return Ok(row.get("id"));
    }

    // Fallback: insert without slug constraint conflict
    let fallback = sqlx::query("INSERT INTO retailers (name) VALUES ($1) RETURNING id")
        .persistent(false)
        .bind(name)
        .fetch_one(&db.pool)
        .await?;
    Ok(fallback.get("id"))
}

#[instrument(skip(db, price_rows))]
pub async fn ingest_prices(db: &Db, mut price_rows: Vec<PriceRow>) -> Result<IngestResult> {
    // Legacy PHP schema shortcut: insert into region_prices (sku_regions-based) and skip the
    // modern prices/current_price tables if the compat schema is detected.
    if php_compat_schema(db).await.unwrap_or(false) {
        if price_rows.is_empty() {
            return Ok(IngestResult {
                offer_jurisdiction_ids: Vec::new(),
                current_updates: Vec::new(),
            });
        }

        // Map sku_region_id -> currency_id, country_id, minor_unit using dynamic country code detection when available.
        let mut sku_region_ids: Vec<i64> = price_rows.iter().map(|r| r.offer_jurisdiction_id).collect();
        let has_countries = table_exists(db, "countries").await.unwrap_or(false);
        let mapping_sql = if has_countries {
            let country_schema = country_schema(db).await?;
            let code_col = country_schema
                .code_column()
                .ok_or_else(|| anyhow::anyhow!("countries table missing code/iso2 column for php compat mapping"))?;
            format!(
                "SELECT sr.id AS sku_region_id, sr.region_code, sr.currency AS currency_code, c.id AS country_id, curr.id AS currency_id, COALESCE(curr.minor_unit, 2) AS minor_unit \
                 FROM sku_regions sr \
                 LEFT JOIN countries c ON UPPER(c.{code_col}) = UPPER(split_part(sr.region_code,'-',1)) \
                 LEFT JOIN currencies curr ON UPPER(curr.code) = UPPER(sr.currency) \
                 WHERE sr.id = ANY($1)",
                code_col = code_col
            )
        } else {
            // Countries table absent: best-effort mapping using sku_regions + currencies only.
            "SELECT sr.id AS sku_region_id, sr.region_code, sr.currency AS currency_code, NULL::bigint AS country_id, curr.id AS currency_id, COALESCE(curr.minor_unit, 2) AS minor_unit \
             FROM sku_regions sr \
             LEFT JOIN currencies curr ON UPPER(curr.code) = UPPER(sr.currency) \
             WHERE sr.id = ANY($1)".to_string()
        };
        let rows = sqlx::query(&mapping_sql)
            .persistent(false)
            .bind(&sku_region_ids)
            .fetch_all(&db.pool)
            .await?;
        let mut map: HashMap<i64, (i64, Option<i64>, i16)> = HashMap::new();
        for row in rows {
            let sku_region_id: i64 = row.get("sku_region_id");
            let currency_id: Option<i64> = row.get("currency_id");
            let country_id: Option<i64> = row.get("country_id");
            let minor_unit: i16 = row.get("minor_unit");
            if let Some(cur) = currency_id {
                map.insert(sku_region_id, (cur, country_id, minor_unit));
            } else {
                warn!(sku_region_id, "skipping price row because currency lookup failed for sku_region");
            }
        }

        // Fallback: ids that are not sku_regions yet may actually be jurisdiction ids. Map by region_code -> sku_regions if jurisdictions table exists.
        if has_countries && table_exists(db, "jurisdictions").await.unwrap_or(false) {
            let missing: Vec<i64> = sku_region_ids
                .iter()
                .copied()
                .filter(|id| !map.contains_key(id))
                .collect();
            if !missing.is_empty() {
                let country_schema = country_schema(db).await?;
                let code_expr = country_schema
                    .code_expr()
                    .ok_or_else(|| anyhow!("countries table missing code expression for jurisdiction mapping"))?;
                let j_sql = format!(
                    "SELECT j.id AS jurisdiction_id, j.country_id, COALESCE(j.region_code, {code_expr}) AS region_code FROM jurisdictions j JOIN countries c ON c.id=j.country_id WHERE j.id = ANY($1)",
                    code_expr = code_expr
                );
                let j_rows = sqlx::query(&j_sql)
                    .persistent(false)
                    .bind(&missing)
                    .fetch_all(&db.pool)
                    .await?;
                let mut region_codes: HashMap<i64, (String, Option<i64>)> = HashMap::new();
                for r in j_rows {
                    let jid: i64 = r.get("jurisdiction_id");
                    let rc: String = r.get::<Option<String>, _>("region_code").unwrap_or_default();
                    let country_id: Option<i64> = r.get("country_id");
                    if !rc.is_empty() {
                        region_codes.insert(jid, (rc.to_ascii_uppercase(), country_id));
                    }
                }
                if !region_codes.is_empty() {
                    let codes: Vec<String> = region_codes.values().map(|(c, _)| c.clone()).collect();
                    let sr_rows = sqlx::query(
                        "SELECT id, region_code, currency AS currency_code, curr.id AS currency_id, COALESCE(curr.minor_unit,2) AS minor_unit \
                         FROM sku_regions sr \
                         LEFT JOIN currencies curr ON UPPER(curr.code)=UPPER(sr.currency) \
                         WHERE UPPER(sr.region_code) = ANY($1)",
                    )
                    .persistent(false)
                    .bind(&codes)
                    .fetch_all(&db.pool)
                    .await?;
                    // Build lookup by region_code
                    let mut by_code: HashMap<String, (i64, Option<i64>, i16)> = HashMap::new();
                    for r in sr_rows {
                        let sid: i64 = r.get("id");
                        let rc: String = r.get::<Option<String>, _>("region_code").unwrap_or_default().to_ascii_uppercase();
                        let currency_id: Option<i64> = r.get("currency_id");
                        let minor_unit: i16 = r.get("minor_unit");
                        by_code.insert(rc, (sid, currency_id, minor_unit));
                    }
                    for (jid, (rc, country_id)) in region_codes {
                        if let Some((sid, currency_id, minor_unit)) = by_code.get(&rc) {
                            if let Some(cur) = currency_id {
                                map.insert(jid, (*cur, country_id, *minor_unit));
                                sku_region_ids.push(*sid);
                            } else {
                                warn!(jurisdiction_id=jid, region_code=%rc, "php compat: currency missing for sku_region by region_code mapping");
                            }
                        } else {
                            warn!(jurisdiction_id=jid, region_code=%rc, "php compat: no sku_region found for jurisdiction region_code");
                        }
                    }
                }
            }
        }

        use sqlx::QueryBuilder;
        let now = chrono::Utc::now();
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO region_prices (sku_region_id, currency_id, country_id, recorded_at, fiat_amount, local_amount, btc_value, tax_inclusive, fx_rate_snapshot, btc_rate_snapshot, raw_payload, created_at, updated_at) ",
        );

        let mut rows_pushed = 0usize;
        qb.push_values(price_rows.iter().filter_map(|r| {
            map.get(&r.offer_jurisdiction_id).map(|(currency_id, country_id, minor_unit)| {
                rows_pushed += 1;
                let scale = 10f64.powi((*minor_unit).max(0) as i32);
                let amount_major = (r.amount_minor as f64) / scale;
                let btc_value = r.btc_sats_per_unit.and_then(|sats| {
                    if sats > 0 {
                        Some(amount_major * 100_000_000f64 / (sats as f64))
                    } else {
                        None
                    }
                });
                (
                    r.offer_jurisdiction_id,
                    *currency_id,
                    *country_id,
                    r.recorded_at,
                    amount_major,
                    amount_major,
                    btc_value,
                    r.tax_inclusive,
                    r.fx_minor_per_unit,
                    r.btc_sats_per_unit,
                    r.meta.clone(),
                    now,
                    now,
                )
            })
        }), |mut b, row| {
            let (
                sku_region_id,
                currency_id,
                country_id,
                recorded_at,
                fiat_amount,
                local_amount,
                btc_value,
                tax_inclusive,
                fx_minor_per_unit,
                btc_sats_per_unit,
                raw_payload,
                created_at,
                updated_at,
            ) = row;
            b.push_bind(sku_region_id)
                .push_bind(currency_id)
                .push_bind(country_id)
                .push_bind(recorded_at)
                .push_bind(fiat_amount)
                .push_bind(local_amount)
                .push_bind(btc_value)
                .push_bind(tax_inclusive)
                .push_bind(fx_minor_per_unit)
                .push_bind(btc_sats_per_unit)
                .push_bind(raw_payload)
                .push_bind(created_at)
                .push_bind(updated_at);
        });

        if rows_pushed > 0 {
            qb.build().persistent(false).execute(&db.pool).await?;
            info!(rows = rows_pushed, "php compat: inserted region_prices rows");
        } else {
            warn!("php compat: no region_prices rows inserted (missing currency mappings)");
        }

        return Ok(IngestResult {
            offer_jurisdiction_ids: Vec::new(),
            current_updates: Vec::new(),
        });
    }
    // Optional price-guard: compare incoming prices to a reference provider (default: 'itad')
    // We annotate meta with guard information and warn on large deviations; we do not block writes.
    let guard_enabled = std::env::var("PRICE_GUARD_ENABLED")
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true);
    let guard_slug = std::env::var("PRICE_GUARD_PROVIDER").unwrap_or_else(|_| "itad".to_string());
    let guard_max_pct_diff: f64 = std::env::var("PRICE_GUARD_MAX_PCT_DIFF")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.5); // 50% default

    if guard_enabled && !price_rows.is_empty() {
        // Skip guard if legacy providers/provider_items tables are absent
        let providers_exists: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE FROM information_schema.tables WHERE table_name='providers' AND table_schema = ANY (current_schemas(true)) LIMIT 1",
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?;
        let provider_items_exists: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE FROM information_schema.tables WHERE table_name='provider_items' AND table_schema = ANY (current_schemas(true)) LIMIT 1",
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?;
        if providers_exists.unwrap_or(false) && provider_items_exists.unwrap_or(false) {
                let oj_set: HashSet<i64> = price_rows.iter().map(|r| r.offer_jurisdiction_id).collect();
                let oj_list: Vec<i64> = oj_set.into_iter().collect();
        // Fetch latest guard price per offer_jurisdiction_id using DISTINCT ON
        let rows = sqlx
            ::query(
                "SELECT DISTINCT ON (p.offer_jurisdiction_id) p.offer_jurisdiction_id, p.amount_minor, p.recorded_at \
             FROM prices p \
             JOIN provider_items pi ON pi.id = p.provider_item_id \
             JOIN providers pr ON pr.id = pi.provider_id \
             WHERE pr.slug = $1 AND p.offer_jurisdiction_id = ANY($2) \
             ORDER BY p.offer_jurisdiction_id, p.recorded_at DESC"
            )
            .persistent(false)
            .bind(&guard_slug)
            .bind(&oj_list)
            .fetch_all(&db.pool).await?;
        let mut guard_map: HashMap<i64, (i64, chrono::DateTime<chrono::Utc>)> = HashMap::new();
        for row in rows {
            let oj: i64 = row.get("offer_jurisdiction_id");
            let amt: i64 = row.get("amount_minor");
            let ts: chrono::DateTime<chrono::Utc> = row.get("recorded_at");
            guard_map.insert(oj, (amt, ts));
        }
        for r in &mut price_rows {
            if let Some((g_amt, g_ts)) = guard_map.get(&r.offer_jurisdiction_id) {
                // relative diff vs guard
                let rel = if *g_amt > 0 {
                    ((r.amount_minor - *g_amt).abs() as f64) / (*g_amt as f64)
                } else {
                    0.0
                };
                let status = if rel > guard_max_pct_diff {
                    warn!(
                        oj = r.offer_jurisdiction_id,
                        amount_minor = r.amount_minor,
                        guard_amount_minor = *g_amt,
                        guard_provider = %guard_slug,
                        guard_recorded_at = %g_ts,
                        rel_diff = rel,
                        "price-guard deviation exceeds threshold"
                    );
                    "warn_large_deviation"
                } else {
                    "ok"
                };
                // Merge guard info into meta
                let mut meta_obj = match r.meta {
                    Value::Null => serde_json::Map::new(),
                    Value::Object(ref m) => m.clone(),
                    _ => serde_json::Map::new(),
                };
                meta_obj.insert(
                    "guard".to_string(),
                    serde_json::json!({
                        "provider": guard_slug,
                        "amount_minor": g_amt,
                        "recorded_at": g_ts,
                        "relative_diff": rel,
                        "status": status,
                    }),
                );
                r.meta = Value::Object(meta_obj);
            }
        }
        } else {
            info!("price guard disabled because providers/provider_items tables are missing in target schema");
        }
    }

    // FX enrichment (optional): populate fx_minor_per_unit if None, using latest rates relative to FX_BASE_CURRENCY (default USD)
    if !price_rows.is_empty() {
        let base_ccy = std::env::var("FX_BASE_CURRENCY").unwrap_or_else(|_| "USD".to_string());
        let mut oj_set: HashSet<i64> = HashSet::new();
        for r in &price_rows {
            oj_set.insert(r.offer_jurisdiction_id);
        }
        let oj_list: Vec<i64> = oj_set.into_iter().collect();
        let rows = sqlx::query(
            "SELECT oj.id, c.code, c.minor_unit \
             FROM offer_jurisdictions oj \
             JOIN currencies c ON c.id=oj.currency_id \
             WHERE oj.id = ANY($1)",
        )
        .bind(&oj_list)
        .fetch_all(&db.pool)
        .await?;
        let mut oj_currency: HashMap<i64, (String, i16)> = HashMap::new();
        for r in rows {
            let id: i64 = r.get("id");
            let code: String = r.get::<Option<String>, _>("code").unwrap_or_default();
            let mu: i16 = r.get::<Option<i16>, _>("minor_unit").unwrap_or(2);
            oj_currency.insert(id, (code, mu));
        }
        let fx_svc = ExchangeService::new(db.clone());
        // Build rate cache per distinct currency
        let mut targets: HashSet<String> = HashSet::new();
        for (_oj, (ccy, _mu)) in oj_currency.iter() {
            targets.insert(ccy.clone());
        }
        let mut rate_cache: HashMap<String, Option<f64>> = HashMap::new();
        for ccy in targets {
            let key = format!("{}->{}", base_ccy, ccy);
            let rate_opt = if base_ccy.eq_ignore_ascii_case(&ccy) {
                Some(1.0)
            } else if let Ok(Some(r)) = fx_svc.latest_rate(&base_ccy, &ccy).await {
                Some(r)
            } else if let Ok(Some(rinv)) = fx_svc.latest_rate(&ccy, &base_ccy).await {
                if rinv > 0.0 { Some(1.0 / rinv) } else { None }
            } else {
                None
            };
            rate_cache.insert(key, rate_opt);
        }
        for r in &mut price_rows {
            if r.fx_minor_per_unit.is_some() {
                continue;
            }
            if let Some((ccy, mu)) = oj_currency.get(&r.offer_jurisdiction_id) {
                let key = format!("{}->{}", base_ccy, ccy);
                if let Some(Some(rate)) = rate_cache.get(&key) {
                    let scale = (10i64).pow((*mu).max(0) as u32);
                    let minor_per_base = (rate * (scale as f64)).round() as i64;
                    r.fx_minor_per_unit = Some(minor_per_base);
                }
            }
        }
    }
    db.bulk_insert_prices(&price_rows).await?;
    let mut current_updates = Vec::with_capacity(price_rows.len());
    // Default agent for generic ingestion path
    const DEFAULT_AGENT: &str = "pipeline";
    const DEFAULT_PRIORITY: i16 = 0;
    for r in &price_rows {
        current_updates.push(CurrentPriceRow {
            offer_jurisdiction_id: r.offer_jurisdiction_id,
            amount_minor: r.amount_minor,
            recorded_at: r.recorded_at,
            agent: DEFAULT_AGENT.to_string(),
            agent_priority: DEFAULT_PRIORITY,
        });
    }
    db.upsert_current_prices(&current_updates).await?;
    let offer_jurisdiction_ids = price_rows.iter().map(|r| r.offer_jurisdiction_id).collect();
    Ok(IngestResult {
        offer_jurisdiction_ids,
        current_updates,
    })
}

pub struct ProviderEntityCache {
    db: Db,
    product_ids: HashMap<String, i64>,
    software_rows: HashSet<i64>,
    title_ids: HashMap<i64, i64>,
    video_game_ids: HashMap<(i64, i64, Option<String>), i64>,
    sellable_ids: HashMap<(String, i64), i64>,
    offer_ids: HashMap<(i64, i64, Option<String>), i64>,
    offer_jurisdictions: HashMap<(i64, i64), i64>,
    provider_items: HashMap<(i64, String), i64>,
}

impl ProviderEntityCache {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            product_ids: HashMap::new(),
            software_rows: HashSet::new(),
            title_ids: HashMap::new(),
            video_game_ids: HashMap::new(),
            sellable_ids: HashMap::new(),
            offer_ids: HashMap::new(),
            offer_jurisdictions: HashMap::new(),
            provider_items: HashMap::new(),
        }
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub async fn ensure_product_named(
        &mut self,
        kind: &str,
        slug: &str,
        name: &str,
    ) -> Result<i64> {
        if let Some(id) = self.product_ids.get(slug) {
            return Ok(*id);
        }
        let id = ensure_product_named(&self.db, kind, slug, name).await?;
        self.product_ids.insert(slug.to_string(), id);
        Ok(id)
    }

    pub async fn ensure_software_row(&mut self, product_id: i64) -> Result<()> {
        if self.software_rows.insert(product_id) {
            ensure_software_row(&self.db, product_id).await?;
        }
        Ok(())
    }

    pub async fn ensure_video_game_title(
        &mut self,
        video_game_id: i64,
        title: &str,
        slug_opt: Option<&str>,
    ) -> Result<i64> {
        if let Some(id) = self.title_ids.get(&video_game_id) {
            return Ok(*id);
        }
        let id = ensure_video_game_title(&self.db, video_game_id, title, slug_opt).await?;
        self.title_ids.insert(video_game_id, id);
        Ok(id)
    }

    pub async fn ensure_video_game(
        &mut self,
        title_id: i64,
        platform_id: i64,
        edition: Option<&str>,
    ) -> Result<i64> {
        let key = (title_id, platform_id, edition.map(|s| s.to_string()));
        if let Some(id) = self.video_game_ids.get(&key) {
            return Ok(*id);
        }
        let id = ensure_video_game(&self.db, title_id, platform_id, edition).await?;
        self.video_game_ids.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_sellable(&mut self, kind: &str, product_id: i64) -> Result<i64> {
        let key = (kind.to_string(), product_id);
        if let Some(id) = self.sellable_ids.get(&key) {
            return Ok(*id);
        }
        let id = ensure_sellable(&self.db, kind, product_id).await?;
        self.sellable_ids.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_offer(
        &mut self,
        sellable_id: i64,
        retailer_id: i64,
        sku: Option<&str>,
    ) -> Result<i64> {
        let key = (sellable_id, retailer_id, sku.map(|s| s.to_string()));
        if let Some(id) = self.offer_ids.get(&key) {
            return Ok(*id);
        }
        let id = ensure_offer(&self.db, sellable_id, retailer_id, sku).await?;
        self.offer_ids.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_offer_jurisdiction(
        &mut self,
        offer_id: i64,
        jurisdiction_id: i64,
        currency_id: i64,
    ) -> Result<i64> {
        let key = (offer_id, jurisdiction_id);
        if let Some(id) = self.offer_jurisdictions.get(&key) {
            return Ok(*id);
        }
        let id =
            ensure_offer_jurisdiction(&self.db, offer_id, jurisdiction_id, currency_id).await?;
        self.offer_jurisdictions.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_provider_item(
        &mut self,
        provider_id: i64,
        external_id: &str,
        payload: Option<Value>,
        refresh_metadata: bool,
    ) -> Result<i64> {
        let key = (provider_id, external_id.to_string());
        if let Some(id) = self.provider_items.get(&key) {
            if refresh_metadata {
                let _ = sqlx::query(
                    "UPDATE provider_items SET metadata=$1, updated_at=now() WHERE id=$2",
                )
                .persistent(false)
                .bind(&payload)
                .bind(id)
                .execute(&self.db.pool)
                .await?;
            }
            return Ok(*id);
        }
        let id = ensure_provider_item(&self.db, provider_id, external_id, payload.clone()).await?;
        self.provider_items.insert(key, id);
        Ok(id)
    }
}

#[derive(Clone, Copy, Debug)]
struct VideoGameTitleSchema {
    has_product_id: bool,
    has_video_game_id: bool,
}

static VIDEO_GAME_TITLE_SCHEMA: OnceCell<VideoGameTitleSchema> = OnceCell::const_new();

#[derive(Clone, Copy, Debug)]
enum SoftwareIdColumn {
    ProductId,
    VideoGameId,
}

static SOFTWARE_ID_COLUMN: OnceCell<SoftwareIdColumn> = OnceCell::const_new();
#[derive(Clone, Copy, Debug)]
struct SellableSchema {
    has_product_id: bool,
    has_software_title_id: bool,
    has_console_id: bool,
}

static SELLABLE_SCHEMA: OnceCell<SellableSchema> = OnceCell::const_new();

async fn get_video_game_title_schema(db: &Db) -> Result<VideoGameTitleSchema> {
    let schema = VIDEO_GAME_TITLE_SCHEMA
        .get_or_try_init(|| async { detect_video_game_title_schema(db).await })
        .await?;
    Ok(*schema)
}

async fn detect_video_game_title_schema(db: &Db) -> Result<VideoGameTitleSchema> {
    let has_video_game_id = video_game_titles_column_exists(db, "video_game_id").await?;
    let has_product_id = video_game_titles_column_exists(db, "product_id").await?;
    if !has_video_game_id && !has_product_id {
        anyhow::bail!("video_game_titles lacks both video_game_id and product_id columns");
    }
    Ok(VideoGameTitleSchema {
        has_product_id,
        has_video_game_id,
    })
}

async fn video_game_titles_column_exists(db: &Db, column: &str) -> Result<bool> {
    table_column_exists(db, "video_game_titles", column).await
}

async fn select_title_id_by_column(db: &Db, column: &str, product_id: i64) -> Result<Option<i64>> {
    let sql = format!("SELECT id FROM video_game_titles WHERE {}=$1", column);
    let rec = sqlx::query(&sql)
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?;
    Ok(rec.map(|row| row.get::<i64, _>("id")))
}

fn build_video_game_title_insert_sql(schema: VideoGameTitleSchema) -> String {
    let mut columns: Vec<&str> = Vec::new();
    if schema.has_video_game_id {
        columns.push("video_game_id");
    }
    if schema.has_product_id {
        columns.push("product_id");
    }
    columns.push("title");
    columns.push("normalized_title");

    let values: Vec<String> = (1..=columns.len()).map(|idx| format!("${}", idx)).collect();

    format!(
        "INSERT INTO video_game_titles ({}) VALUES ({}) RETURNING id",
        columns.join(", "),
        values.join(", ")
    )
}

async fn get_software_id_column(db: &Db) -> Result<SoftwareIdColumn> {
    let column = SOFTWARE_ID_COLUMN
        .get_or_try_init(|| async { detect_software_id_column(db).await })
        .await?;
    Ok(*column)
}

async fn detect_software_id_column(db: &Db) -> Result<SoftwareIdColumn> {
    if table_column_exists(db, "software", "video_game_id").await? {
        return Ok(SoftwareIdColumn::VideoGameId);
    }
    if table_column_exists(db, "software", "product_id").await? {
        return Ok(SoftwareIdColumn::ProductId);
    }
    anyhow::bail!("software table missing expected product/video_game id columns")
}

async fn table_column_exists(db: &Db, table: &str, column: &str) -> Result<bool> {
    let exists = sqlx
        ::query(
            "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name=$2"
        )
        .persistent(false)
        .bind(table)
        .bind(column)
        .fetch_optional(&db.pool).await?
        .is_some();
    Ok(exists)
}

async fn get_sellable_schema(db: &Db) -> Result<SellableSchema> {
    let schema = SELLABLE_SCHEMA
        .get_or_try_init(|| async { detect_sellable_schema(db).await })
        .await?;
    Ok(*schema)
}

async fn detect_sellable_schema(db: &Db) -> Result<SellableSchema> {
    Ok(SellableSchema {
        has_product_id: table_column_exists(db, "sellables", "product_id").await?,
        has_software_title_id: table_column_exists(db, "sellables", "software_title_id").await?,
        has_console_id: table_column_exists(db, "sellables", "console_id").await?,
    })
}

async fn fetch_title_id_for_product(db: &Db, product_id: i64) -> Result<Option<i64>> {
    let schema = get_video_game_title_schema(db).await?;
    let mut clauses: Vec<&str> = Vec::new();
    if schema.has_video_game_id {
        clauses.push("video_game_id = $1");
    }
    if schema.has_product_id {
        clauses.push("product_id = $1");
    }
    if clauses.is_empty() {
        return Ok(None);
    }
    let sql = format!(
        "SELECT id FROM video_game_titles WHERE {} ORDER BY updated_at DESC LIMIT 1",
        clauses.join(" OR ")
    );
    let rec = sqlx::query(&sql)
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?;
    Ok(rec.map(|row| row.get::<i64, _>("id")))
}

// --------- Ingest run observability helpers ---------

#[instrument(skip(db, meta))]
pub async fn ingest_run_start(
    db: &Db,
    provider_id: i64,
    region_code: Option<&str>,
    meta: Option<Value>,
) -> Result<i64> {
    // If the tracking table doesn't exist (e.g., legacy PHP schema), skip run bookkeeping gracefully.
    let table_exists: bool = sqlx
        ::query_scalar::<_, bool>(
            "SELECT TRUE FROM information_schema.tables WHERE table_schema='public' AND table_name='provider_ingest_runs' LIMIT 1"
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?
        .unwrap_or(false);
    if !table_exists {
        return Ok(0);
    }
    // Backward compatibility: ensure region_code column exists (older DBs may lack it)
    let _ = sqlx::query(
        "ALTER TABLE public.provider_ingest_runs ADD COLUMN IF NOT EXISTS region_code text",
    )
    .persistent(false)
    .execute(&db.pool)
    .await;
    // Detect optional 'meta' column presence
    let meta_exists: bool = sqlx
        ::query_scalar::<_, Option<String>>(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='meta'"
        )
        .persistent(false)
        .fetch_optional(&db.pool).await?
        .is_some();
    if meta_exists {
        let _ = sqlx
            ::query(
                "INSERT INTO public.provider_ingest_runs (provider_id, region_code, meta) VALUES ($1,$2,$3)"
            )
            .persistent(false)
            .bind(provider_id)
            .bind(region_code)
            .bind(meta)
            .execute(&db.pool).await?;
    } else {
        let _ = sqlx::query(
            "INSERT INTO public.provider_ingest_runs (provider_id, region_code) VALUES ($1,$2)",
        )
        .persistent(false)
        .bind(provider_id)
        .bind(region_code)
        .execute(&db.pool)
        .await?;
    }
    // Fallback: fetch latest run id for this provider
    let maybe = sqlx
        ::query(
            "SELECT id FROM public.provider_ingest_runs WHERE provider_id=$1 ORDER BY started_at DESC LIMIT 1"
        )
        .persistent(false)
        .bind(provider_id)
        .fetch_optional(&db.pool).await?;
    Ok(maybe.map(|r| r.get::<i64, _>("id")).unwrap_or(0))
}

#[instrument(skip(db, errors))]
pub async fn ingest_run_finish(
    db: &Db,
    run_id: i64,
    status: &str,
    items_processed: i64,
    prices_written: i64,
    errors: Option<Value>,
) -> Result<()> {
    if run_id == 0 {
        // No-op if run tracking table/row not available
        return Ok(());
    }
    sqlx
        ::query(
            "UPDATE public.provider_ingest_runs SET ended_at=now(), status=$1, items_processed=$2, prices_written=$3, errors=COALESCE($4, errors) WHERE id=$5"
        )
        .persistent(false)
        .bind(status)
        .bind(items_processed)
        .bind(prices_written)
        .bind(errors)
        .bind(run_id)
        .execute(&db.pool).await?;
    Ok(())
}

#[instrument(skip(db))]
pub async fn sample_ingest_flow(db: &Db) -> Result<IngestResult> {
    // Demonstration: create minimal entities then ingest one price
    let _provider_id = ensure_provider(db, "steam", "retailer_api", Some("steam")).await?;
    let retailer_id = ensure_retailer(db, "Steam Store", Some("steam")).await?;
    let product_id = ensure_product(db, "software", Some("sample-game")).await?;
    let sellable_id = ensure_sellable(db, "software", product_id).await?;
    let _offer_id = ensure_offer(db, sellable_id, retailer_id, None).await?;

    // Ensure a basic USD / US national jurisdiction
    let usd_id = ensure_currency(db, "USD", "United States Dollar", 2).await?;
    let us_id = ensure_country(db, "US", "United States", usd_id).await?;

    // For simplicity, ensure jurisdiction and return empty ingest result placeholder
    let _jurisdiction_id = ensure_national_jurisdiction(db, us_id).await?;
    Ok(IngestResult {
        offer_jurisdiction_ids: Vec::new(),
        current_updates: Vec::new(),
    })
}

// --------- Provider items and metadata ---------

#[instrument(skip(db, metadata))]
pub async fn ensure_provider_item(
    db: &Db,
    provider_id: i64,
    external_id: &str,
    metadata: Option<Value>,
) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM provider_items WHERE provider_id=$1 AND external_id=$2")
            .persistent(false)
            .bind(provider_id)
            .bind(external_id)
            .fetch_optional(&db.pool)
            .await?
    {
        let existing_id: i64 = rec.get("id");
        // If metadata provided, refresh it (idempotent update)
        if metadata.is_some() {
            let _ =
                sqlx::query("UPDATE provider_items SET metadata=$1, updated_at=now() WHERE id=$2")
                    .persistent(false)
                    .bind(&metadata)
                    .bind(existing_id)
                    .execute(&db.pool)
                    .await?;
        }
        return Ok(existing_id);
    }
    let rec = sqlx
        ::query(
            "INSERT INTO provider_items (provider_id, external_id, metadata) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(provider_id)
        .bind(external_id)
        .bind(&metadata)
        .fetch_one(&db.pool).await?;
    Ok(rec.get("id"))
}

// --------- Provider media links ---------

#[instrument(skip(db))]
pub async fn ensure_provider_media_link(db: &Db, provider_item_id: i64, url: &str) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM provider_media_links WHERE provider_item_id=$1 AND url=$2")
            .persistent(false)
            .bind(provider_item_id)
            .bind(url)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx::query(
        "INSERT INTO provider_media_links (provider_item_id, url) VALUES ($1,$2) RETURNING id",
    )
    .persistent(false)
    .bind(provider_item_id)
    .bind(url)
    .fetch_one(&db.pool)
    .await?;
    Ok(inserted.get("id"))
}

/// Batch upsert variant to reduce per-row round trips.
/// Accepts a slice of tuples representing game media rows.
/// Each tuple: (video_game_id, source, external_id, media_type, url, provider_data_json)
/// Performs ON CONFLICT upsert mirroring single-row behavior.
#[instrument(skip(db, rows))]
pub async fn upsert_game_media_batch(
    db: &Db,
    rows: &[(i64, &str, &str, &str, &str, &Value)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    use sqlx::QueryBuilder;
    use sqlx::types::Json;

    const MAX_CHUNK_SIZE: usize = 64;

    // Deduplicate within the batch to avoid hitting the same target row twice in a single
    // INSERT ... ON CONFLICT statement, which PostgreSQL rejects. Prefer the first occurrence
    // (these rows are typically identical apart from metadata ordering).
    let mut deduped: Vec<(i64, &str, &str, &str, &str, &Value)> = Vec::with_capacity(rows.len());
    let mut seen: HashSet<(i64, &str, &str)> = HashSet::with_capacity(rows.len());
    for row in rows {
        let key = (row.0, row.1, row.2);
        if seen.insert(key) {
            deduped.push(*row);
        } else {
            debug!(
                video_game_id = row.0,
                source = row.1,
                external_id = row.2,
                "skipping duplicate media row in batch"
            );
        }
    }

    if deduped.is_empty() {
        return Ok(());
    }

    for chunk in deduped.chunks(MAX_CHUNK_SIZE) {
        let mut qb = QueryBuilder::new(
            "INSERT INTO game_media (video_game_id, source, external_id, media_type, title, url, original_url, thumbnail_url, stream_url, poster_url, provider_data) VALUES ",
        );

        for (idx, (vg_id, source, external_id, media_type, url, pdata)) in chunk.iter().enumerate()
        {
            if idx > 0 {
                qb.push(", ");
            }

            let title = *media_type;

            let (original_url, thumbnail_url, stream_url, poster_url) = {
                fn looks_like_url(s: &str) -> bool {
                    let ls = s.to_ascii_lowercase();
                    (ls.starts_with("http://") || ls.starts_with("https://"))
                        && ls.len() > 8
                        && ls.contains('.')
                }
                fn grab<'a>(
                    obj: &'a serde_json::Map<String, Value>,
                    keys: &[&str],
                ) -> Option<&'a str> {
                    for k in keys {
                        if let Some(v) = obj.get(*k) {
                            if let Some(s) = v.as_str() {
                                if looks_like_url(s) {
                                    return Some(s);
                                }
                            }
                        }
                    }
                    None
                }
                if let Some(obj) = pdata.as_object() {
                    let orig =
                        grab(obj, &["original_url", "url", "high_url", "hd_url"]).unwrap_or(*url);
                    let thumb = grab(
                        obj,
                        &[
                            "thumbnail_url",
                            "thumb",
                            "thumbnail",
                            "small_url",
                            "image_thumb",
                        ],
                    );
                    let stream = grab(obj, &["stream_url", "video_url", "m3u8", "playable_url"]);
                    let poster = grab(
                        obj,
                        &["poster_url", "preview_url", "image", "hero", "cover"],
                    );
                    (Some(orig), thumb, stream, poster)
                } else {
                    (Some(*url), None, None, None)
                }
            };

            qb.push("(")
                .push_bind(*vg_id)
                .push(", ")
                .push_bind(*source)
                .push("::media_source, ")
                .push_bind(*external_id)
                .push(", ")
                .push_bind(*media_type)
                .push("::media_type, ")
                .push_bind(title)
                .push(", ")
                .push_bind(*url)
                .push(", ")
                .push_bind(original_url)
                .push(", ")
                .push_bind(thumbnail_url)
                .push(", ")
                .push_bind(stream_url)
                .push(", ")
                .push_bind(poster_url)
                .push(", ")
                .push_bind(Json((*pdata).clone()))
                .push(")");
        }

        qb.push(
            " ON CONFLICT (video_game_id, source, external_id) DO UPDATE SET \
              url=EXCLUDED.url, \
              provider_data=EXCLUDED.provider_data, \
              title=EXCLUDED.title, \
              original_url=COALESCE(EXCLUDED.original_url, game_media.original_url), \
              thumbnail_url=COALESCE(EXCLUDED.thumbnail_url, game_media.thumbnail_url), \
              stream_url=COALESCE(EXCLUDED.stream_url, game_media.stream_url), \
              poster_url=COALESCE(EXCLUDED.poster_url, game_media.poster_url)",
        );

        qb.build().execute(&db.pool).await?;
    }
    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_software_row(db: &Db, product_id: i64) -> Result<()> {
    let column = get_software_id_column(db).await?;
    let column_name = match column {
        SoftwareIdColumn::VideoGameId => "video_game_id",
        SoftwareIdColumn::ProductId => "product_id",
    };
    let select_sql = format!(
        "SELECT EXISTS(SELECT 1 FROM software WHERE {}=$1) AS present",
        column_name
    );
    let exists: bool = sqlx::query_scalar(&select_sql)
        .persistent(false)
        .bind(product_id)
        .fetch_one(&db.pool)
        .await?;
    if exists {
        return Ok(());
    }
    let insert_sql = format!("INSERT INTO software ({}) VALUES ($1)", column_name);
    sqlx::query(&insert_sql)
        .persistent(false)
        .bind(product_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_video_game_title(
    db: &Db,
    product_id: i64,
    name: &str,
    slug: Option<&str>,
) -> Result<i64> {
    let schema = get_video_game_title_schema(db).await?;
    if schema.has_video_game_id {
        if let Some(id) = select_title_id_by_column(db, "video_game_id", product_id).await? {
            return Ok(id);
        }
    }
    if schema.has_product_id {
        if let Some(id) = select_title_id_by_column(db, "product_id", product_id).await? {
            return Ok(id);
        }
    }
    // Normalization: if slug provided, use it; else derive via DB function normalize_game_title
    // Prefer server-side; if function is missing (older schema), fall back to a local normalization.
    let normalized: Option<String> = match slug {
        Some(s) => Some(s.to_string()),
        None => {
            match sqlx::query("SELECT normalize_game_title($1) AS norm")
                .persistent(false)
                .bind(name)
                .fetch_one(&db.pool)
                .await
            {
                Ok(row) => {
                    let norm: String = row.get("norm");
                    Some(norm)
                }
                Err(sqlx::Error::Database(db_err)) => {
                    // 42883 = undefined_function
                    if db_err.code().map(|c| c.to_string()).as_deref() == Some("42883") {
                        warn!(
                            func = "normalize_game_title(text)",
                            "DB function missing; using local fallback normalization"
                        );
                        Some(local_normalize_title(name))
                    } else {
                        return Err(sqlx::Error::Database(db_err).into());
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    };
    let insert_sql = build_video_game_title_insert_sql(schema);
    let mut query = sqlx::query(&insert_sql).persistent(false);
    if schema.has_video_game_id {
        query = query.bind(product_id);
    }
    if schema.has_product_id {
        query = query.bind(product_id);
    }
    let rec = query
        .bind(name)
        .bind(&normalized)
        .fetch_one(&db.pool)
        .await?;
    Ok(rec.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_video_game_title_without_product(
    db: &Db,
    slug_hint: &str,
    name: &str,
) -> Result<i64> {
    let slug_candidate = if !slug_hint.trim().is_empty() {
        local_normalize_title(slug_hint)
    } else {
        local_normalize_title(name)
    };
    let slug_final = if slug_candidate.is_empty() {
        format!("title-{}", Uuid::new_v4().simple())
    } else {
        slug_candidate
    };
    let title_trimmed = name.trim();
    let title_final = if title_trimmed.is_empty() {
        if !slug_hint.trim().is_empty() {
            slug_hint.trim().replace(['-', '_'], " ")
        } else {
            format!("Untitled {}", Uuid::new_v4().simple())
        }
    } else {
        title_trimmed.to_string()
    };

    if
        let Some(row) = sqlx
            ::query(
                "SELECT id, title FROM video_game_titles WHERE normalized_title=$1 ORDER BY updated_at DESC LIMIT 1"
            )
            .persistent(false)
            .bind(&slug_final)
            .fetch_optional(&db.pool).await?
    {
        let id: i64 = row.get("id");
        let existing_title: String = row.get("title");
        if !title_final.is_empty() && existing_title.trim() != title_final {
            let _ = sqlx
                ::query("UPDATE video_game_titles SET title=$1 WHERE id=$2")
                .persistent(false)
                .bind(&title_final)
                .bind(id)
                .execute(&db.pool).await?;
        }
        return Ok(id);
    }

    let lower_name = title_final.to_lowercase();
    if !lower_name.is_empty() {
        if let Some(row) = sqlx::query(
            "SELECT id, normalized_title FROM video_game_titles WHERE lower(title)=$1 LIMIT 1",
        )
        .persistent(false)
        .bind(&lower_name)
        .fetch_optional(&db.pool)
        .await?
        {
            let id: i64 = row.get("id");
            let existing_norm: String = row.get("normalized_title");
            if existing_norm.trim().is_empty() || existing_norm != slug_final {
                let _ = sqlx::query("UPDATE video_game_titles SET normalized_title=$1 WHERE id=$2")
                    .persistent(false)
                    .bind(&slug_final)
                    .bind(id)
                    .execute(&db.pool)
                    .await?;
            }
            if !title_final.is_empty() {
                let _ = sqlx
                    ::query(
                        "UPDATE video_game_titles SET title=$1 WHERE id=$2 AND title IS DISTINCT FROM $1"
                    )
                    .persistent(false)
                    .bind(&title_final)
                    .bind(id)
                    .execute(&db.pool).await?;
            }
            return Ok(id);
        }
    }

    let inserted = sqlx::query(
        "INSERT INTO video_game_titles (title, normalized_title) VALUES ($1,$2) RETURNING id",
    )
    .persistent(false)
    .bind(&title_final)
    .bind(&slug_final)
    .fetch_one(&db.pool)
    .await?;
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_video_game(
    db: &Db,
    title_id: i64,
    platform_id: i64,
    edition: Option<&str>,
) -> Result<i64> {
    let rec = match edition {
        Some(ed) => {
            sqlx::query(
                "SELECT id FROM video_games WHERE title_id=$1 AND platform_id=$2 AND edition=$3",
            )
            .persistent(false)
            .bind(title_id)
            .bind(platform_id)
            .bind(ed)
            .fetch_optional(&db.pool)
            .await?
        }
        None => sqlx::query(
            "SELECT id FROM video_games WHERE title_id=$1 AND platform_id=$2 AND edition IS NULL",
        )
        .persistent(false)
        .bind(title_id)
        .bind(platform_id)
        .fetch_optional(&db.pool)
        .await?,
    };
    if let Some(r) = rec {
        return Ok(r.get("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO video_games (title_id, platform_id, edition) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(title_id)
    .bind(platform_id)
    .bind(edition)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_sellable_for_title(db: &Db, title_id: i64) -> Result<i64> {
    if let Some(row) = sqlx::query("SELECT id FROM sellables WHERE software_title_id=$1")
        .persistent(false)
        .bind(title_id)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(row.get("id"));
    }
    let row = sqlx::query(
        "INSERT INTO sellables (kind, software_title_id) VALUES ('software',$1) RETURNING id",
    )
    .persistent(false)
    .bind(title_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(row.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_sellable(db: &Db, kind: &str, product_id: i64) -> Result<i64> {
    let schema = get_sellable_schema(db).await?;
    match kind {
        "software" => {
            if schema.has_software_title_id {
                let title_id = fetch_title_id_for_product(db, product_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "no video_game_title found for product {} while creating sellable",
                            product_id
                        )
                    })?;
                if
                    let Some(rec) = sqlx
                        ::query(
                            "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND software_title_id=$2"
                        )
                        .persistent(false)
                        .bind(kind)
                        .bind(title_id)
                        .fetch_optional(&db.pool).await?
                {
                    let id: i64 = rec.get("id");
                    let _ = sqlx
                        ::query(
                            "UPDATE video_games SET sellable_id=$1 WHERE title_id=$2 AND (sellable_id IS DISTINCT FROM $1)"
                        )
                        .persistent(false)
                        .bind(id)
                        .bind(title_id)
                        .execute(&db.pool).await?;
                    return Ok(id);
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, software_title_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(title_id)
                    .fetch_one(&db.pool).await?;
                let sellable_id: i64 = inserted.get("id");
                let _ = sqlx
                    ::query(
                        "UPDATE video_games SET sellable_id=$1 WHERE title_id=$2 AND (sellable_id IS DISTINCT FROM $1)"
                    )
                    .persistent(false)
                    .bind(sellable_id)
                    .bind(title_id)
                    .execute(&db.pool).await?;
                Ok(sellable_id)
            } else if schema.has_product_id {
                if let Some(rec) = sqlx::query(
                    "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND product_id=$2",
                )
                .persistent(false)
                .bind(kind)
                .bind(product_id)
                .fetch_optional(&db.pool)
                .await?
                {
                    return Ok(rec.get("id"));
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, product_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(product_id)
                    .fetch_one(&db.pool).await?;
                Ok(inserted.get("id"))
            } else {
                anyhow::bail!("sellables table missing software linkage columns")
            }
        }
        "hardware" => {
            if schema.has_console_id {
                if let Some(rec) = sqlx::query(
                    "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND console_id=$2",
                )
                .persistent(false)
                .bind(kind)
                .bind(product_id)
                .fetch_optional(&db.pool)
                .await?
                {
                    return Ok(rec.get("id"));
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, console_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(product_id)
                    .fetch_one(&db.pool).await?;
                Ok(inserted.get("id"))
            } else if schema.has_product_id {
                if let Some(rec) = sqlx::query(
                    "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND product_id=$2",
                )
                .persistent(false)
                .bind(kind)
                .bind(product_id)
                .fetch_optional(&db.pool)
                .await?
                {
                    return Ok(rec.get("id"));
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, product_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(product_id)
                    .fetch_one(&db.pool).await?;
                Ok(inserted.get("id"))
            } else {
                anyhow::bail!("sellables table missing hardware linkage columns")
            }
        }
        _ => anyhow::bail!("unsupported sellable kind '{}'", kind),
    }
}

#[instrument(skip(db))]
pub async fn ensure_offer(
    db: &Db,
    sellable_id: i64,
    retailer_id: i64,
    sku: Option<&str>,
) -> Result<i64> {
    if php_compat_schema(db).await.unwrap_or(false) {
        let (counter, map) = OFFER_COMPAT
            .get_or_init(|| async {
                (
                    std::sync::atomic::AtomicI64::new(1),
                    Mutex::new(HashMap::new()),
                )
            })
            .await;
        let mut guard = map.lock().unwrap();
        if let Some((id, _)) = guard
            .iter()
            .find(|(_id, (s, r, sk))| *s == sellable_id && *r == retailer_id && sk.as_deref() == sku)
            .map(|(id, v)| (*id, v.clone()))
        {
            return Ok(id);
        }
        let new_id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        guard.insert(new_id, (sellable_id, retailer_id, sku.map(|s| s.to_string())));
        return Ok(new_id);
    }

    if
        let Some(rec) = sqlx
            ::query(
                "SELECT id FROM offers WHERE sellable_id=$1 AND retailer_id=$2 AND sku IS NOT DISTINCT FROM $3"
            )
            .persistent(false)
            .bind(sellable_id)
            .bind(retailer_id)
            .bind(sku)
            .fetch_optional(&db.pool).await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx::query(
        "INSERT INTO offers (sellable_id, retailer_id, sku) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(sellable_id)
    .bind(retailer_id)
    .bind(sku)
    .fetch_one(&db.pool)
    .await?;
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_offer_jurisdiction(
    db: &Db,
    offer_id: i64,
    jurisdiction_id: i64,
    currency_id: i64,
) -> Result<i64> {
    let compat = php_compat_schema(db).await.unwrap_or(false);
    let has_jurisdictions = *JURISDICTIONS_PRESENT
        .get_or_try_init(|| async { table_exists(db, "jurisdictions").await })
        .await?;

    if compat {
        let has_countries = table_exists(db, "countries").await.unwrap_or(false);
        // If legacy helper tables are missing, treat the incoming jurisdiction_id as an existing sku_region_id (best effort for Supabase-lite).
        if !has_jurisdictions || !has_countries {
            if let Some(rec) = sqlx::query("SELECT id FROM sku_regions WHERE id=$1")
                .persistent(false)
                .bind(jurisdiction_id)
                .fetch_optional(&db.pool)
                .await?
            {
                return Ok(rec.get("id"));
            } else {
                anyhow::bail!("sku_regions.id={} not found; legacy compat requires jurisdictions/countries tables or matching sku_regions rows", jurisdiction_id);
            }
        }

        // Map to sku_regions unique (product_id, region_code, retailer)
        // Derive product and retailer info from compat map (synthetic offers)
        let (sellable_id, retailer_id, _sku) = {
            let (_, map) = OFFER_COMPAT
                .get_or_init(|| async {
                    (
                        std::sync::atomic::AtomicI64::new(1),
                        Mutex::new(HashMap::new()),
                    )
                })
                .await;
            let guard = map.lock().unwrap();
            guard
                .get(&offer_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("offer {} missing for php compat", offer_id))?
        };
        // Map sellable -> product_id if present
        let product_id: i64 = sqlx::query_scalar(
            "SELECT COALESCE(product_id, software_title_id, console_id, id) FROM sellables WHERE id=$1",
        )
        .persistent(false)
        .bind(sellable_id)
        .fetch_optional(&db.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("sellable {} missing when creating sku_region (offer {})", sellable_id, offer_id))?;

        // region_code from jurisdictions / countries
        let country_schema = country_schema(db).await?;
        let code_expr = country_schema
            .code_expr()
            .ok_or_else(|| anyhow::anyhow!("countries table missing code column for region lookup"))?;
        let region_sql = format!(
            "SELECT COALESCE(j.region_code, {}) FROM jurisdictions j JOIN countries c ON c.id=j.country_id WHERE j.id=$1",
            code_expr
        );
        let region_code: String = sqlx::query_scalar(&region_sql)
            .persistent(false)
            .bind(jurisdiction_id)
            .fetch_optional(&db.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("jurisdiction {} missing (offer {})", jurisdiction_id, offer_id))?;
        let region_code = region_code.to_ascii_uppercase();

        // retailer string
        let retailer_key: String = sqlx::query_scalar(
            "SELECT COALESCE(slug, name, 'retailer') FROM retailers WHERE id=$1",
        )
        .persistent(false)
        .bind(retailer_id)
        .fetch_optional(&db.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("retailer {} missing (offer {})", retailer_id, offer_id))?;

        // currency code
        let currency: String = sqlx::query_scalar("SELECT code FROM currencies WHERE id=$1")
            .persistent(false)
            .bind(currency_id)
            .fetch_optional(&db.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("currency {} missing (offer {})", currency_id, offer_id))?;

        // Upsert sku_regions
        if let Some(existing) = sqlx::query(
            "SELECT id FROM sku_regions WHERE product_id=$1 AND region_code=$2 AND retailer=$3",
        )
        .persistent(false)
        .bind(product_id)
        .bind(&region_code)
        .bind(&retailer_key)
        .fetch_optional(&db.pool)
        .await?
        {
            return Ok(existing.get("id"));
        }
        let rec = sqlx::query(
            "INSERT INTO sku_regions (product_id, region_code, retailer, currency, sku, is_active, metadata) VALUES ($1,$2,$3,$4,$5,true, NULL) RETURNING id",
        )
        .persistent(false)
        .bind(product_id)
        .bind(&region_code)
        .bind(&retailer_key)
        .bind(&currency)
        .bind::<Option<String>>(None)
        .fetch_one(&db.pool)
        .await?;
        return Ok(rec.get("id"));
    }

    // Modern schema: require jurisdictions table so we don't fail with relation errors.
    if !has_jurisdictions {
        anyhow::bail!("jurisdictions table missing; cannot create offer_jurisdictions — ensure migrations are applied or enable php compat schema");
    }

    if let Some(rec) =
        sqlx::query("SELECT id FROM offer_jurisdictions WHERE offer_id=$1 AND jurisdiction_id=$2")
            .persistent(false)
            .bind(offer_id)
            .bind(jurisdiction_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO offer_jurisdictions (offer_id, jurisdiction_id, currency_id) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(offer_id)
        .bind(jurisdiction_id)
        .bind(currency_id)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_product(db: &Db, category: &str, slug: Option<&str>) -> Result<i64> {
    if let Some(slug_val) = slug {
        if let Some(rec) = sqlx::query("SELECT id FROM products WHERE slug=$1")
            .persistent(false)
            .bind(slug_val)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
        if let Some(rec) = sqlx::query(
            "INSERT INTO products (slug, category) VALUES ($1,$2) \
                     ON CONFLICT (slug) DO UPDATE SET category=EXCLUDED.category RETURNING id",
        )
        .persistent(false)
        .bind(slug_val)
        .bind(category)
        .fetch_optional(&db.pool)
        .await?
        {
            return Ok(rec.get("id"));
        }
    }
    let inserted = sqlx::query("INSERT INTO products (category) VALUES ($1) RETURNING id")
        .persistent(false)
        .bind(category)
        .fetch_one(&db.pool)
        .await?;
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_product_named(db: &Db, category: &str, slug: &str, name: &str) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM products WHERE slug=$1")
        .persistent(false)
        .bind(slug)
        .fetch_optional(&db.pool)
        .await?
    {
        // Keep name/category fresh if changed
        let existing_id: i64 = rec.get("id");
        let _ = sqlx::query("UPDATE products SET name=$1, category=$2 WHERE id=$3")
            .persistent(false)
            .bind(name)
            .bind(category)
            .bind(existing_id)
            .execute(&db.pool)
            .await?;
        return Ok(existing_id);
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO products (slug, name, category) VALUES ($1,$2,$3) \
             ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, category=EXCLUDED.category RETURNING id"
        )
        .persistent(false)
        .bind(slug)
        .bind(name)
        .bind(category)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

// --------- Jurisdictional helpers (currencies, countries, jurisdictions) ---------

#[instrument(skip(db))]
pub async fn ensure_currency(db: &Db, code: &str, name: &str, minor_unit: i16) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM currencies WHERE code=$1")
        .persistent(false)
        .bind(code)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec =
        sqlx::query("INSERT INTO currencies (code,name,minor_unit) VALUES ($1,$2,$3) RETURNING id")
            .persistent(false)
            .bind(code)
            .bind(name)
            .bind(minor_unit)
            .fetch_one(&db.pool)
            .await?;
    Ok(rec.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_country(db: &Db, iso2: &str, name: &str, currency_id: i64) -> Result<i64> {
    let schema = country_schema(db).await?;
    let iso2_clean = iso2.trim().to_ascii_uppercase();
    if iso2_clean.len() != 2 {
        anyhow::bail!(
            "invalid ISO-3166 alpha-2 code '{}': expected 2 characters",
            iso2
        );
    }

    let code_col = schema
        .code_col
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("countries table missing code column (iso2/country_code/code2/code)"))?;

    let select_sql = format!("SELECT id FROM countries WHERE {}=$1", code_col);
    if let Some(rec) = sqlx::query(&select_sql)
        .persistent(false)
        .bind(&iso2_clean)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }

    let iso3_val = if schema.has_iso3 {
        iso3_from_iso2(&iso2_clean)
    } else {
        None
    };

    use sqlx::QueryBuilder;
    let mut cols: Vec<&str> = Vec::new();
    cols.push(code_col);
    if schema.has_iso3 && iso3_val.is_some() && code_col != "iso3" {
        cols.push("iso3");
    }
    if schema.has_code2 && code_col != "code2" {
        cols.push("code2");
    }
    if schema.has_name {
        cols.push("name");
    }
    if schema.has_currency_id {
        cols.push("currency_id");
    }

    let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new("INSERT INTO countries (");
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            qb.push(", ");
        }
        qb.push(*c);
    }
    qb.push(") VALUES (");
    let iso3_string = iso3_val
        .map(|s| s.to_string())
        .unwrap_or_else(|| iso2_clean.clone());
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            qb.push(", ");
        }
        if *col == code_col {
            qb.push_bind(iso2_clean.clone());
        } else if *col == "iso3" {
            qb.push_bind(iso3_string.clone());
        } else if *col == "code2" {
            qb.push_bind(iso2_clean.clone());
        } else if *col == "name" {
            qb.push_bind(name.to_string());
        } else if *col == "currency_id" {
            qb.push_bind(currency_id);
        } else {
            qb.push_bind(iso2_clean.clone());
        }
    }
    qb.push(")");
    if let Some(col) = schema.code_col.as_ref() {
        qb.push(" ON CONFLICT (");
        qb.push(col.as_str());
        qb.push(") DO UPDATE SET ");
        if schema.has_name {
            qb.push("name = EXCLUDED.name");
        } else {
            qb.push(" ");
        }
    }
    qb.push(" RETURNING id");

    let rec = qb.build().persistent(false).fetch_one(&db.pool).await?;
    Ok(rec.get("id"))
}

fn iso3_from_iso2(code: &str) -> Option<&'static str> {
    match code {
        // Some providers may pass language code 'ZH' where a country code is expected;
        // treat it as China to avoid ingestion failures.
        "ZH" => Some("CHN"),
        "US" => Some("USA"),
        "CA" => Some("CAN"),
        "MX" => Some("MEX"),
        "BR" => Some("BRA"),
        "AR" => Some("ARG"),
        "CL" => Some("CHL"),
        "CO" => Some("COL"),
        "PE" => Some("PER"),
        "UY" => Some("URY"),
        "GB" => Some("GBR"),
        "IE" => Some("IRL"),
        "FR" => Some("FRA"),
        "DE" => Some("DEU"),
        "ES" => Some("ESP"),
        "IT" => Some("ITA"),
        "NL" => Some("NLD"),
        "BE" => Some("BEL"),
        "PT" => Some("PRT"),
        "LU" => Some("LUX"),
        "AT" => Some("AUT"),
        "CH" => Some("CHE"),
        "SE" => Some("SWE"),
        "NO" => Some("NOR"),
        "DK" => Some("DNK"),
        "FI" => Some("FIN"),
        "IS" => Some("ISL"),
        "GR" => Some("GRC"),
        "PL" => Some("POL"),
        "CZ" => Some("CZE"),
        "SK" => Some("SVK"),
        "HU" => Some("HUN"),
        "SI" => Some("SVN"),
        "HR" => Some("HRV"),
        "RO" => Some("ROU"),
        "BG" => Some("BGR"),
        "EE" => Some("EST"),
        "LV" => Some("LVA"),
        "LT" => Some("LTU"),
        "MT" => Some("MLT"),
        "CY" => Some("CYP"),
        "UA" => Some("UKR"),
        "RU" => Some("RUS"),
        "TR" => Some("TUR"),
        "SA" => Some("SAU"),
        "AE" => Some("ARE"),
        "IL" => Some("ISR"),
        "EG" => Some("EGY"),
        "ZA" => Some("ZAF"),
        "NG" => Some("NGA"),
        "KE" => Some("KEN"),
        "JP" => Some("JPN"),
        "KR" => Some("KOR"),
        "HK" => Some("HKG"),
        "TW" => Some("TWN"),
        "SG" => Some("SGP"),
        "MY" => Some("MYS"),
        "TH" => Some("THA"),
        "ID" => Some("IDN"),
        "PH" => Some("PHL"),
        "VN" => Some("VNM"),
        "AU" => Some("AUS"),
        "NZ" => Some("NZL"),
        "KZ" => Some("KAZ"),
        "IN" => Some("IND"),
        "PK" => Some("PAK"),
        "BD" => Some("BGD"),
        "LK" => Some("LKA"),
        "QA" => Some("QAT"),
        "KW" => Some("KWT"),
        "BH" => Some("BHR"),
        "OM" => Some("OMN"),
        "JO" => Some("JOR"),
        "DZ" => Some("DZA"),
        "MA" => Some("MAR"),
        "TN" => Some("TUN"),
        "GH" => Some("GHA"),
        "CM" => Some("CMR"),
        "SN" => Some("SEN"),
        _ => None,
    }
}

struct CountrySchema {
    has_iso3: bool,
    has_code2: bool,
    has_currency_id: bool,
    has_name: bool,
    code_col: Option<String>,
    code_expr: Option<String>,
}

impl CountrySchema {
    fn code_expr(&self) -> Option<&str> {
        self.code_expr.as_deref()
    }

    fn code_column(&self) -> Option<&str> {
        self.code_col.as_deref()
    }
}

async fn country_schema(db: &Db) -> Result<&'static CountrySchema> {
    COUNTRY_SCHEMA
        .get_or_try_init(|| async {
            let cols: Vec<String> = sqlx::query_scalar(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = ANY (current_schemas(true)) AND table_name = 'countries'",
            )
            .persistent(false)
            .fetch_all(&db.pool)
            .await?;

            let has_iso2 = cols.iter().any(|c| c.eq_ignore_ascii_case("iso2"));
            let has_iso3 = cols.iter().any(|c| c.eq_ignore_ascii_case("iso3"));
            let has_code2 = cols.iter().any(|c| c.eq_ignore_ascii_case("code2"));
            let has_country_code = cols.iter().any(|c| c.eq_ignore_ascii_case("country_code"));
            let has_code = cols.iter().any(|c| c.eq_ignore_ascii_case("code"));
            let has_currency_id = cols.iter().any(|c| c.eq_ignore_ascii_case("currency_id"));
            let has_name = cols.iter().any(|c| c.eq_ignore_ascii_case("name"));

            let code_col = if has_iso2 {
                Some("iso2".to_string())
            } else if has_country_code {
                Some("country_code".to_string())
            } else if has_code2 {
                Some("code2".to_string())
            } else if has_code {
                Some("code".to_string())
            } else {
                None
            };
            let code_expr = code_col
                .as_ref()
                .map(|c| format!("c.{}", c));

            Ok::<CountrySchema, anyhow::Error>(CountrySchema {
                has_iso3,
                has_code2,
                has_currency_id,
                has_name,
                code_col,
                code_expr,
            })
        })
        .await
}

#[instrument(skip(db))]
pub async fn ensure_national_jurisdiction(db: &Db, country_id: i64) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM jurisdictions WHERE country_id=$1 AND region_code IS NULL")
            .persistent(false)
            .bind(country_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO jurisdictions (country_id,region_code) VALUES ($1,NULL) RETURNING id",
    )
    .persistent(false)
    .bind(country_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get("id"))
}

fn local_normalize_title(title: &str) -> String {
    title
        .trim()
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

// --------- Platform helpers ---------

#[instrument(skip(db))]
pub async fn ensure_platform(db: &Db, name: &str, _slug: Option<&str>) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM platforms WHERE name=$1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }

    // Some schemas require a non-null `code` column. Detect it once and populate if present.
    static PLATFORM_CODE_PRESENT: OnceCell<bool> = OnceCell::const_new();
    static PLATFORM_FAMILY_PRESENT: OnceCell<bool> = OnceCell::const_new();
    let has_code = *PLATFORM_CODE_PRESENT
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE FROM information_schema.columns WHERE table_name='platforms' AND column_name='code' AND table_schema = ANY (current_schemas(true)) LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;
    let has_family = *PLATFORM_FAMILY_PRESENT
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE FROM information_schema.columns WHERE table_name='platforms' AND column_name='family' AND table_schema = ANY (current_schemas(true)) LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;

    // Insert without slug - let DB use defaults/constraints
    let inserted = if has_code && has_family {
        sqlx::query(
            "INSERT INTO platforms (name, code, family) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(name)
        .bind(name)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    } else if has_code {
        sqlx::query(
            "INSERT INTO platforms (name, code) VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(name)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    } else if has_family {
        sqlx::query(
            "INSERT INTO platforms (name, family) VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(name)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO platforms (name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    };

    // If insert returned nothing (conflict), fetch the existing ID
    if let Some(row) = inserted {
        return Ok(row.get("id"));
    }

    // Fetch existing ID after conflict (e.g., duplicate or RLS-hidden row)
    if let Some(rec) = sqlx::query("SELECT id FROM platforms WHERE name=$1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }

    // If still not found (likely RLS hiding the row), insert a compat row with a
    // distinct name to obtain an accessible id and keep ingest moving.
    let compat_name = format!("{} (compat)", name);
    let compat_insert = if has_code && has_family {
        sqlx::query(
            "INSERT INTO platforms (name, code, family) VALUES ($1,$2,$3) RETURNING id",
        )
        .persistent(false)
        .bind(&compat_name)
        .bind(&compat_name)
        .bind(&compat_name)
        .fetch_one(&db.pool)
        .await?
    } else if has_code {
        sqlx::query("INSERT INTO platforms (name, code) VALUES ($1,$2) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .bind(&compat_name)
            .fetch_one(&db.pool)
            .await?
    } else if has_family {
        sqlx::query("INSERT INTO platforms (name, family) VALUES ($1,$2) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .bind(&compat_name)
            .fetch_one(&db.pool)
            .await?
    } else {
        sqlx::query("INSERT INTO platforms (name) VALUES ($1) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .fetch_one(&db.pool)
            .await?
    };

    Ok(compat_insert.get("id"))
}

// --------- Provider offers ---------

#[instrument(skip(db))]
pub async fn link_provider_offer(
    db: &Db,
    provider_item_id: i64,
    offer_id: i64,
    confidence: Option<f32>,
) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM provider_offers WHERE provider_item_id=$1 AND offer_id=$2")
            .persistent(false)
            .bind(provider_item_id)
            .bind(offer_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO provider_offers (provider_item_id, offer_id, confidence) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(provider_item_id)
        .bind(offer_id)
        .bind(confidence)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

// --------- Provider media links with metadata ---------

#[instrument(skip(db, urls, meta))]
pub async fn ensure_provider_media_links_with_meta(
    db: &Db,
    provider_item_id: i64,
    video_game_id: Option<i64>,
    urls: &[(String, Option<String>, Option<String>, Option<String>)],
    source: &str,
    meta: Option<Value>,
) -> Result<usize> {
    let metadata_arg = meta.as_ref().filter(|v| !v.is_null()).cloned();
    let mut count = 0usize;
    let mut seen: HashSet<String> = HashSet::new();
    for (url, media_type_raw, role_raw, title_raw) in urls {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = trimmed.to_string();
        if !seen.insert(normalized.clone()) {
            continue;
        }
        let media_type = media_type_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase());
        let role = role_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let title = title_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let _ = sqlx
            ::query(
                "INSERT INTO provider_media_links (provider_item_id, video_game_id, url, source, media_type, role, title, metadata) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, '{}'::jsonb)) \
                 ON CONFLICT (provider_item_id, url) DO UPDATE \
                 SET video_game_id = COALESCE(EXCLUDED.video_game_id, provider_media_links.video_game_id), \
                     source = COALESCE(EXCLUDED.source, provider_media_links.source), \
                     media_type = COALESCE(EXCLUDED.media_type, provider_media_links.media_type), \
                     role = COALESCE(EXCLUDED.role, provider_media_links.role), \
                     title = COALESCE(EXCLUDED.title, provider_media_links.title), \
                     metadata = COALESCE($8, provider_media_links.metadata), \
                     updated_at = now()"
            )
            .persistent(false)
            .bind(provider_item_id)
            .bind(video_game_id)
            .bind(&normalized)
            .bind(source)
            .bind(&media_type)
            .bind(&role)
            .bind(&title)
            .bind(&metadata_arg)
            .execute(&db.pool).await?;
        count += 1;
    }
    Ok(count)
}

// --------- Provider media links (simple version) ---------

#[instrument(skip(db, urls))]
pub async fn ensure_provider_media_links(
    db: &Db,
    provider_item_id: i64,
    urls: &[String],
) -> Result<()> {
    for url in urls {
        let _ = ensure_provider_media_link(db, provider_item_id, url).await?;
    }
    Ok(())
}

// --------- Game media upsert (single row) ---------

#[instrument(skip(db, provider_data))]
pub async fn upsert_game_media(
    db: &Db,
    video_game_id: i64,
    source: &str,
    external_id: &str,
    media_type: &str,
    url: &str,
    provider_data: Value,
) -> Result<()> {
    sqlx
        ::query(
            "INSERT INTO game_media (video_game_id, source, external_id, media_type, title, url, provider_data) \
               VALUES ($1, $2::media_source, $3, $4::media_type, $5, $6, $7) \
             ON CONFLICT (video_game_id, source, external_id) DO UPDATE \
             SET url = EXCLUDED.url, provider_data = EXCLUDED.provider_data, title = EXCLUDED.title"
        )
        .persistent(false)
        .bind(video_game_id)
        .bind(source)
        .bind(external_id)
        .bind(media_type)
        .bind(media_type)
        .bind(url)
        .bind(provider_data)
        .execute(&db.pool).await?;
    Ok(())
}
