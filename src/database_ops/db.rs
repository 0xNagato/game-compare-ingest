use anyhow::Result;
use chrono::{Datelike, TimeZone, Utc};
use sqlx::{
    PgPool, QueryBuilder, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, instrument};

#[derive(Clone)]
pub struct Db {
    pub pool: PgPool,
}

impl Db {
    #[instrument]
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let use_prepared = std::env::var("USE_PREPARED")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")))
            .unwrap_or(false);
        let mut connect_options = PgConnectOptions::from_str(database_url)?;
        if !use_prepared {
            // PgBouncer txn mode safe
            connect_options = connect_options.statement_cache_capacity(0);
        }

        // Optional fast-ingest session tuning
        let fast_ingest = std::env::var("FAST_INGEST")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")))
            .unwrap_or(false);
        let work_mem_mb: u32 = std::env::var("FAST_INGEST_WORK_MEM_MB")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(64);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(600))
            .after_connect(move |conn, _meta| {
                let do_fast = fast_ingest;
                let wm = work_mem_mb;
                Box::pin(async move {
                    if do_fast {
                        // Best-effort; ignore errors to avoid blocking startup in restricted envs
                        let _ = sqlx::query("SET synchronous_commit = 'off'")
                            .execute(&mut *conn)
                            .await;
                        let _ = sqlx::query(&format!("SET work_mem = '{}MB'", wm))
                            .execute(&mut *conn)
                            .await;
                        // maintenance_work_mem can help when creating indexes in-session (rare here)
                        let _ = sqlx::query("SET maintenance_work_mem = '256MB'")
                            .execute(&mut *conn)
                            .await;
                    }
                    Ok(())
                })
            })
            .connect_with(connect_options)
            .await?;
        info!("connected to db");

        // Optional auto-migrate gate (default: true)
        let auto_migrate = std::env::var("AUTO_MIGRATE")
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true);
        if auto_migrate {
            info!("running migrations (AUTO_MIGRATE=on, custom runner)");
            Self::run_migrations(&pool).await?;
        } else {
            info!("AUTO_MIGRATE disabled; skipping migrations");
        }
        Ok(Self { pool })
    }

    // Variant that NEVER runs migrations regardless of env (for pure data import paths).
    #[instrument]
    pub async fn connect_no_migrate(database_url: &str, max_connections: u32) -> Result<Self> {
        let use_prepared = std::env::var("USE_PREPARED")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")))
            .unwrap_or(false);
        let mut connect_options = PgConnectOptions::from_str(database_url)?;
        if !use_prepared {
            connect_options = connect_options.statement_cache_capacity(0);
        }

        // Fast-ingest session wiring (applies to every acquired connection).
        let fast_ingest = std::env::var("FAST_INGEST")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")))
            .unwrap_or(false);
        let work_mem_mb: u32 = std::env::var("FAST_INGEST_WORK_MEM_MB")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(64);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(600))
            .after_connect(move |conn, _meta| {
                let do_fast = fast_ingest;
                let wm = work_mem_mb;
                Box::pin(async move {
                    if do_fast {
                        let _ = sqlx::query("SET synchronous_commit = 'off'")
                            .execute(&mut *conn)
                            .await;
                        let _ = sqlx::query(&format!("SET work_mem = '{}MB'", wm))
                            .execute(&mut *conn)
                            .await;
                        let _ = sqlx::query("SET maintenance_work_mem = '256MB'")
                            .execute(&mut *conn)
                            .await;
                    }
                    Ok(())
                })
            })
            .connect_with(connect_options)
            .await?;
        info!("connected to db (no-migrate)");
        Ok(Self { pool })
    }
}

impl Db {
    // Custom lightweight migration runner that ignores non-numeric filenames (e.g. database_settings.sql)
    async fn run_migrations(pool: &PgPool) -> Result<()> {
        use std::{fs, path::Path};
        let dir = Path::new("./migrations");
        if !dir.exists() {
            return Ok(());
        }
        // Ensure tracking table exists (use raw_sql to avoid prepared statements under PgBouncer)
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
                version BIGINT PRIMARY KEY,
                description TEXT,
                installed_at TIMESTAMPTZ DEFAULT now()
             )",
        )
        .execute(pool)
        .await?;
        // Fetch applied versions
        let applied_rows = sqlx::raw_sql("SELECT version FROM _sqlx_migrations")
            .fetch_all(pool)
            .await?;
        use std::collections::HashSet;
        let mut applied: HashSet<i64> = HashSet::new();
        for r in applied_rows {
            applied.insert(r.try_get::<i64, _>(0)?);
        }
        // Collect candidate migration files
        let mut candidates: Vec<(i64, String, std::path::PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                // pattern: digits '_' rest '.sql'
                let mut chars = fname.chars();
                let mut num_str = String::new();
                while let Some(c) = chars.next() {
                    if c.is_ascii_digit() {
                        num_str.push(c);
                    } else {
                        break;
                    }
                }
                if num_str.is_empty() || !fname.ends_with(".sql") {
                    continue;
                }
                if let Some(rest) = fname
                    .strip_prefix(&format!("{}", num_str))
                    .and_then(|s| s.strip_prefix("_"))
                {
                    if let Ok(version) = num_str.parse::<i64>() {
                        candidates.push((version, rest.trim_end_matches(".sql").to_string(), path));
                    }
                }
            }
        }
        candidates.sort_by_key(|(v, _, _)| *v);
        for (version, desc, path) in candidates {
            if applied.contains(&version) {
                continue;
            }
            let sql = fs::read_to_string(&path)?;
            info!(version, file=?path, "applying migration (two-phase)");

            // Phase 1: strip out CREATE INDEX CONCURRENTLY statements so they are not executed
            // inside an implicit transaction (which Postgres forbids). We do a lightweight
            // line-oriented scan; all such statements in our migrations start with that phrase
            // (possibly preceded by whitespace) and end at the first ';'. Multiline bodies are
            // collected until a terminating semicolon line is seen.
            let mut transactional = String::with_capacity(sql.len());
            let mut concurrent_indexes: Vec<String> = Vec::new();
            let mut capturing = false;
            let mut buf = String::new();
            for line in sql.lines() {
                let lt = line.trim_start().to_lowercase();
                if !capturing && lt.starts_with("create index concurrently") {
                    capturing = true;
                    buf.clear();
                    buf.push_str(line);
                    buf.push('\n');
                    if line.contains(';') {
                        // single-line statement
                        capturing = false;
                        concurrent_indexes.push(buf.clone());
                        buf.clear();
                    }
                    continue;
                }
                if capturing {
                    buf.push_str(line);
                    buf.push('\n');
                    if line.contains(';') {
                        capturing = false;
                        concurrent_indexes.push(buf.clone());
                        buf.clear();
                    }
                    continue;
                }
                // Normal line
                transactional.push_str(line);
                transactional.push('\n');
            }
            // Execute transactional portion (may be empty)
            let trimmed = transactional.trim();
            if !trimmed.is_empty() {
                sqlx::raw_sql(trimmed).execute(pool).await?;
            }
            // Phase 2: run each CREATE INDEX CONCURRENTLY individually in autocommit context
            for stmt in concurrent_indexes {
                let stmt_trim = stmt.trim();
                if stmt_trim.is_empty() {
                    continue;
                }
                info!(migration_version=version, index_stmt=%stmt_trim, "creating concurrent index");
                // Use raw_sql so statement goes as-is; errors bubble up but do not rollback prior work.
                if let Err(e) = sqlx::raw_sql(stmt_trim).execute(pool).await {
                    // Log and continue; since all are IF NOT EXISTS this is usually safe.
                    tracing::warn!(migration_version=version, error=%e, "concurrent index creation failed");
                    return Err(e.into());
                }
            }
            // Use raw_sql to avoid prepared statements; escape single quotes in description
            let desc_escaped = desc.replace('\'', "''");
            let insert_stmt = format!(
                "INSERT INTO _sqlx_migrations(version, description) VALUES ({}, '{}')",
                version, desc_escaped
            );
            sqlx::raw_sql(&insert_stmt).execute(pool).await?;
            // Update in-memory applied set to prevent duplicate-key errors if multiple files share the same version
            applied.insert(version);
        }
        // Log last applied
        if let Ok(r) = sqlx::raw_sql(
            "SELECT version, description FROM _sqlx_migrations ORDER BY version DESC LIMIT 1",
        )
        .fetch_one(pool)
        .await
        {
            let version: i64 = r.try_get(0).unwrap_or_default();
            let desc: String = r
                .try_get::<Option<String>, _>(1)
                .ok()
                .flatten()
                .unwrap_or_default();
            info!(version, desc, "migrations up-to-date (custom)");
        }
        Ok(())
    }
}

pub struct PriceRow {
    pub offer_jurisdiction_id: i64,
    pub provider_item_id: Option<i64>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    pub amount_minor: i64,
    pub tax_inclusive: bool,
    pub fx_minor_per_unit: Option<i64>,
    pub btc_sats_per_unit: Option<i64>,
    pub meta: serde_json::Value,
}

// (Optional future) validation helper removed for now to keep startup lean.

pub struct CurrentPriceRow {
    pub offer_jurisdiction_id: i64,
    pub amount_minor: i64,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    // Source agent hint (e.g., 'ps-store', 'steam', 'nexarda'). Used for tie-breaking on equal timestamps
    pub agent: String,
    // Higher priority wins when recorded_at ties (e.g., storefront=100, retailer_api=90, catalog=20, fallback=0)
    pub agent_priority: i16,
}

impl Db {
    #[instrument(skip(self, rows))]
    pub async fn bulk_insert_prices(&self, rows: &[PriceRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // Pre-ensure partitions for distinct months to avoid DDL-in-trigger conflicts
        {
            use std::collections::HashSet;
            let mut months: HashSet<(i32, u32)> = HashSet::new();
            for r in rows {
                let dt = r.recorded_at;
                months.insert((dt.year(), dt.month()));
            }
            // ensure for each representative timestamp (1st of month)
            for (y, m) in months {
                let first = Utc.with_ymd_and_hms(y, m, 1, 0, 0, 0).unwrap();
                // ignore errors if function missing; best effort; avoid prepared statements via raw_sql
                let iso = first.to_rfc3339();
                let stmt = format!("SELECT ensure_price_partition('{}'::timestamptz)", iso);
                let _ = sqlx::raw_sql(&stmt).execute(&self.pool).await;
            }
        }
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive, fx_minor_per_unit, btc_sats_per_unit, meta) ",
        );
        qb.push_values(rows, |mut b, r| {
            b.push_bind(r.offer_jurisdiction_id)
                .push_bind(r.provider_item_id)
                .push_bind(r.recorded_at)
                .push_bind(r.amount_minor)
                .push_bind(r.tax_inclusive)
                .push_bind(r.fx_minor_per_unit)
                .push_bind(r.btc_sats_per_unit)
                .push_bind(&r.meta);
        });
        qb.build().persistent(false).execute(&self.pool).await?;
        Ok(())
    }

    #[instrument(skip(self, rows))]
    pub async fn upsert_current_prices(&self, rows: &[CurrentPriceRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO current_price (offer_jurisdiction_id, amount_minor, recorded_at, agent, agent_priority) ",
        );
        use std::collections::HashMap;
        // Keep only the latest recorded_at per offer_jurisdiction_id
        let mut latest: HashMap<i64, &CurrentPriceRow> = HashMap::new();
        for r in rows {
            latest
                .entry(r.offer_jurisdiction_id)
                .and_modify(|cur| {
                    if r.recorded_at > cur.recorded_at
                        || (r.recorded_at == cur.recorded_at
                            && (r.agent_priority > cur.agent_priority
                                || (r.agent_priority == cur.agent_priority
                                    && r.amount_minor != cur.amount_minor)))
                    {
                        *cur = r;
                    }
                })
                .or_insert(r);
        }
        let uniques: Vec<&CurrentPriceRow> = latest.into_values().collect();
        qb.push_values(&uniques, |mut b, r| {
            b.push_bind(r.offer_jurisdiction_id)
                .push_bind(r.amount_minor)
                .push_bind(r.recorded_at)
                .push_bind(&r.agent)
                .push_bind(r.agent_priority);
        });
        qb.push(
            " ON CONFLICT (offer_jurisdiction_id)
              DO UPDATE SET amount_minor = EXCLUDED.amount_minor,
                             recorded_at = EXCLUDED.recorded_at,
                             agent = EXCLUDED.agent,
                             agent_priority = EXCLUDED.agent_priority
              WHERE (current_price.recorded_at < EXCLUDED.recorded_at)
                 OR (current_price.recorded_at = EXCLUDED.recorded_at AND current_price.agent_priority < EXCLUDED.agent_priority)
                 OR (current_price.recorded_at = EXCLUDED.recorded_at AND current_price.agent_priority = EXCLUDED.agent_priority AND current_price.amount_minor <> EXCLUDED.amount_minor)"
        );
        qb.build().persistent(false).execute(&self.pool).await?;

        let impacted_oj: Vec<i64> = uniques
            .iter()
            .map(|row| row.offer_jurisdiction_id)
            .collect();
        self.refresh_video_game_regional_prices(&impacted_oj)
            .await?;
        Ok(())
    }

    async fn refresh_video_game_regional_prices(
        &self,
        offer_jurisdiction_ids: &[i64],
    ) -> Result<()> {
        if offer_jurisdiction_ids.is_empty() {
            return Ok(());
        }

        let video_game_ids: Vec<i64> = sqlx::query_scalar(
            "SELECT DISTINCT vg.id
             FROM public.video_games vg
             JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
             JOIN public.sellables s ON s.software_title_id = vgt.id
             JOIN public.offers o ON o.sellable_id = s.id
             JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
             WHERE oj.id = ANY($1)",
        )
        .bind(offer_jurisdiction_ids)
        .fetch_all(&self.pool)
        .await?;

        if video_game_ids.is_empty() {
            return Ok(());
        }

        sqlx::query(
            "WITH price_data AS (
                SELECT
                    vg.id AS video_game_id,
                    COALESCE(
                        jsonb_agg(
                            jsonb_build_object(
                                'offer_jurisdiction_id', oj.id,
                                'region_code', COALESCE(j.region_code, co.iso2),
                                'region_label', CASE
                                    WHEN j.region_code IS NULL OR j.region_code = '' THEN co.name
                                    ELSE co.name || ' - ' || j.region_code
                                END,
                                'country_iso2', co.iso2,
                                'currency_code', curr.code,
                                'amount_minor', cp.amount_minor,
                                'recorded_at', cp.recorded_at,
                                'title', COALESCE(vg.display_title, vgt.title)
                            )
                            ORDER BY COALESCE(j.region_code, co.iso2), curr.code
                        ),
                        '[]'::jsonb
                    ) AS prices
                FROM public.video_games vg
                JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
                JOIN public.sellables s ON s.software_title_id = vgt.id
                JOIN public.offers o ON o.sellable_id = s.id
                JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
                JOIN public.current_price cp ON cp.offer_jurisdiction_id = oj.id
                JOIN public.jurisdictions j ON j.id = oj.jurisdiction_id
                JOIN public.countries co ON co.id = j.country_id
                JOIN public.currencies curr ON curr.id = oj.currency_id
                WHERE vg.id = ANY($1)
                GROUP BY vg.id
            )
            UPDATE public.video_games vg
            SET regional_prices = price_data.prices
            FROM price_data
            WHERE vg.id = price_data.video_game_id",
        )
        .bind(&video_game_ids)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
