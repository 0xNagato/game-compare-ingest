use anyhow::{Context, Result, anyhow, bail};
use clap::{CommandFactory, Parser, Subcommand};
use i_miss_rust::cli::db_counts::{DbCountsConfig, run as run_db_counts};
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::giantbomb::ingest::ingest_from_file as gb_ingest_from_file;
use i_miss_rust::database_ops::igdb::client as igdb_client;
use i_miss_rust::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
    ensure_offer_jurisdiction, ensure_retailer, ensure_sellable,
};
use i_miss_rust::database_ops::nexarda::provider::{NexardaOptions, NexardaProvider};
use i_miss_rust::database_ops::rawg;
use i_miss_rust::database_ops::steam::provider::SteamProvider;
use i_miss_rust::database_ops::xbox::provider as xbox_provider;
use i_miss_rust::psstore_seed_pipeline;
use i_miss_rust::util::env;
use rayon::{ThreadPoolBuilder, prelude::*};
use sqlx::{Executor, Row};
use std::cmp::min;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gc", version, about = "GameCompare admin CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
#[command(rename_all = "kebab-case")]
enum Commands {
    /// Run PlayStation catalog sync and emit price snapshots
    PsPrices {
        /// Comma-separated list of PS Store locales (overrides PS_REGIONS)
        #[arg(long)]
        regions: Option<String>,
        /// Max pages per platform (overrides PS_MAX_PAGES)
        #[arg(long)]
        max_pages: Option<u32>,
        /// Page size (overrides PS_PAGE_SIZE)
        #[arg(long)]
        page_size: Option<u32>,
    },
    /// Fetch and store PlayStation ratings
    PsRatings {
        /// Locale (overrides PS_LOCALE)
        #[arg(long)]
        locale: Option<String>,
        /// Max pages for PS4 (overrides MAX_PAGES_PS4)
        #[arg(long)]
        max_pages_ps4: Option<u32>,
        /// Max pages for PS5 (overrides MAX_PAGES_PS5)
        #[arg(long)]
        max_pages_ps5: Option<u32>,
        /// Page size (overrides PAGE_SIZE)
        #[arg(long)]
        page_size: Option<u32>,
        /// Dry run (no DB writes)
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Dump PlayStation category metadata to CSV
    PsDumpCategories,
    /// Fetch a PlayStation product detail payload
    PsDumpDetail {
        /// Product ID to fetch
        #[arg(long)]
        product_id: Option<String>,
        /// Locale override (default from env/config)
        #[arg(long)]
        locale: Option<String>,
        /// Optional path to write JSON payload
        #[arg(long = "out")]
        out: Option<PathBuf>,
    },
    /// Dump PlayStation price payloads for debugging
    PsDumpPrices,
    /// Dump raw PlayStation product payloads
    PsDumpRaw,
    /// Export PlayStation products catalogue snapshot
    PsExportProducts,
    /// Run PlayStation genre scan helper
    PsGenreScan,
    /// Run demo ingestion pipeline for PlayStation sample data
    PsIngestDemo,
    /// Run PlayStation prices debug helper
    PsPricesDebug,
    /// Search PlayStation categories via GraphQL helper
    PsSearchCategories,
    /// Count PlayStation catalog ranges across locales
    PsCountRange {
        #[arg(long)]
        regions: Option<String>,
        #[arg(long = "ps4-category")]
        ps4_category: Option<String>,
        #[arg(long = "ps5-category")]
        ps5_category: Option<String>,
        #[arg(long = "page-size")]
        page_size: Option<u32>,
        #[arg(long = "total-pages")]
        total_pages: Option<u32>,
        #[arg(long = "cutoff-year")]
        cutoff_year: Option<i32>,
        #[arg(long = "rps")]
        rps_per_locale: Option<u32>,
        #[arg(long = "retry-attempts")]
        retry_attempts: Option<u32>,
        #[arg(long = "retry-backoff-ms")]
        retry_backoff_ms: Option<u64>,
    },
    /// Probe a PlayStation product/concept for price info
    PsPriceProbe {
        #[arg(long)]
        locale: Option<String>,
        #[arg(long = "product-id")]
        product_id: Option<String>,
        #[arg(long = "concept-id")]
        concept_id: Option<String>,
    },
    /// Capture PlayStation Store Set-Cookie headers per locale and emit reusable cookie/header files
    PsCaptureHeaders {
        #[arg(long)]
        regions: Option<String>,
        #[arg(long = "out")]
        out: Option<PathBuf>,
        #[arg(long = "user-agent")]
        user_agent: Option<String>,
    },
    /// End-to-end portfolio ingest: prune demo offers, bootstrap PlayStation retailer, run all providers, and report coverage
    #[command(name = "ps-full-refresh")]
    PsFullRefresh {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
        /// Prune demo retailer offers and jurisdictions before bootstrapping
        #[arg(long, default_value_t = true)]
        prune_demo: bool,
        /// Dry run (log actions without mutating or ingesting)
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Skip the final db-counts report
        #[arg(long, default_value_t = false)]
        skip_counts: bool,
        /// Skip removal of existing PlayStation offers and provider data before ingesting
        #[arg(long, default_value_t = false)]
        skip_purge_playstation: bool,
        /// Override the minimum release year (default 2020)
        #[arg(long)]
        year_min: Option<i32>,
        /// Override the maximum release year (default 2025)
        #[arg(long)]
        year_max: Option<i32>,
    },
    /// Run raw SQL migrations using the simple runner
    DbMigrate {
        /// Optional migrations directory
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Include non-prefixed SQL files
        #[arg(long, default_value_t = false)]
        include_all: bool,
    },
    /// Apply a single migration file via simple runner
    DbMigrateOnce {
        /// Migration file path to run exactly once
        #[arg(long)]
        file: PathBuf,
    },
    DbCounts {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
        /// Force printing of recent games (otherwise follows env)
        #[arg(long, default_value_t = false)]
        recent_games: bool,
        /// Override RECENT_GAMES_LIMIT (defaults to env/20)
        #[arg(long)]
        recent_games_limit: Option<i64>,
    },
    /// Backfill missing sellables for canonical video game titles
    DbBackfillSellables {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
        /// Maximum number of titles to process (default: all)
        #[arg(long)]
        limit: Option<i64>,
        /// Batch size for each fetch (default: 250)
        #[arg(long)]
        chunk_size: Option<i64>,
        /// When set, only logs actions without mutating the database
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Ensure every sellable has a baseline offer and jurisdiction coverage
    DbBootstrapOffers {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
        /// ISO currency code to associate with the jurisdiction (default: USD)
        #[arg(long, default_value = "USD")]
        currency: String,
        /// Descriptive currency name (default: derived from code)
        #[arg(long)]
        currency_name: Option<String>,
        /// Minor unit for the currency (default: 2)
        #[arg(long, default_value_t = 2)]
        currency_minor_unit: i16,
        /// ISO-3166 alpha-2 country code for the jurisdiction (default: US)
        #[arg(long, default_value = "US")]
        country: String,
        /// Descriptive country name (default: derived from code)
        #[arg(long)]
        country_name: Option<String>,
        /// Additional coverage specs (e.g., "GB:GBP,CA:CAD:2")
        #[arg(long)]
        coverage: Option<String>,
        /// Retailer display name (default: PlayStation)
        #[arg(long, default_value = "PlayStation")]
        retailer_name: String,
        /// Retailer slug (default: playstation)
        #[arg(long, default_value = "playstation")]
        retailer_slug: String,
        /// Maximum number of sellables to consider (default: all)
        #[arg(long)]
        limit: Option<i64>,
        /// Batch size per iteration (default: 250)
        #[arg(long)]
        chunk_size: Option<i64>,
        /// When set, only logs actions without mutating the database
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Print missing entity statistics
    DbMissingStats {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
    },
    /// Emit schema audit for core tables
    DbSchemaAudit {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
        /// Optional comma-separated filter of tables
        #[arg(long, value_delimiter = ',')]
        tables: Option<Vec<String>>,
        /// Optional override for max pool connections
        #[arg(long)]
        max_connections: Option<u32>,
    },
    /// Run the unified ingest pipeline (backfill, bootstrap, PlayStation seed)
    UnifiedIngest {
        /// Optional override for the database URL
        #[arg(long)]
        db_url: Option<String>,
        /// Skip the sellable backfill step
        #[arg(long, default_value_t = false)]
        skip_backfill: bool,
        /// Skip the offer bootstrap step
        #[arg(long, default_value_t = false)]
        skip_bootstrap: bool,
        /// Skip the PlayStation seeding pipeline
        #[arg(long, default_value_t = false)]
        skip_ps_seed: bool,
        /// Maximum number of rows to process (shared across backfill/bootstrap)
        #[arg(long)]
        limit: Option<i64>,
        /// Chunk size for batched operations (shared across backfill/bootstrap)
        #[arg(long)]
        chunk_size: Option<i64>,
        /// Coverage string (e.g., "GB:GBP,CA:CAD:2")
        #[arg(long)]
        coverage: Option<String>,
        /// Base currency code for bootstrap offers
        #[arg(long, default_value = "USD")]
        currency: String,
        /// Optional friendly currency name
        #[arg(long)]
        currency_name: Option<String>,
        /// Currency minor unit
        #[arg(long, default_value_t = 2)]
        currency_minor_unit: i16,
        /// Base country code for bootstrap offers
        #[arg(long, default_value = "US")]
        country: String,
        /// Optional friendly country name
        #[arg(long)]
        country_name: Option<String>,
        /// Retailer display name
        #[arg(long, default_value = "PlayStation")]
        retailer_name: String,
        /// Retailer slug
        #[arg(long, default_value = "playstation")]
        retailer_slug: String,
        /// Only log actions without mutating the database (applies to backfill/bootstrap)
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Override PlayStation seed regions
        #[arg(long)]
        ps_regions: Option<String>,
        /// Override PlayStation page size
        #[arg(long)]
        ps_page_size: Option<u32>,
        /// Override PlayStation total pages per locale
        #[arg(long)]
        ps_total_pages: Option<u32>,
        /// Override PlayStation start page offset
        #[arg(long)]
        ps_start_page: Option<u32>,
        /// Override PlayStation minimum release year
        #[arg(long)]
        ps_year_min: Option<i32>,
        /// Override PlayStation maximum release year
        #[arg(long)]
        ps_year_max: Option<i32>,
        /// Override PlayStation requests-per-second setting
        #[arg(long)]
        ps_rps: Option<u32>,
        /// Override PlayStation retry attempts
        #[arg(long)]
        ps_retry_attempts: Option<u32>,
        /// Override PlayStation retry base backoff in ms
        #[arg(long)]
        ps_retry_backoff_ms: Option<u64>,
        /// Disable PlayStation backfill mode
        #[arg(long, default_value_t = false)]
        disable_ps_backfill: bool,
        /// Skip Nexarda ingestion step
        #[arg(long, default_value_t = false)]
        skip_nexarda: bool,
        /// Skip GiantBomb JSON ingest step
        #[arg(long, default_value_t = false)]
        skip_giantbomb: bool,
        /// Skip Steam storefront ingestion
        #[arg(long, default_value_t = false)]
        skip_steam: bool,
        /// Skip Xbox storefront ingestion
        #[arg(long, default_value_t = false)]
        skip_xbox: bool,
        /// Skip IGDB catalogue ingestion
        #[arg(long, default_value_t = false)]
        skip_igdb: bool,
        /// Skip RAWG metadata ingestion
        #[arg(long, default_value_t = false)]
        skip_rawg: bool,
    },
}

#[derive(Debug, Default, Clone)]
struct BootstrapStats {
    processed: i64,
    offers_created: i64,
    offers_reused: i64,
    offers_missing: i64,
    jurisdictions_created: i64,
    jurisdictions_reused: i64,
    jurisdictions_missing: i64,
    failures: i64,
}

impl BootstrapStats {
    fn absorb(&mut self, other: &BootstrapStats) {
        self.processed += other.processed;
        self.offers_created += other.offers_created;
        self.offers_reused += other.offers_reused;
        self.offers_missing += other.offers_missing;
        self.jurisdictions_created += other.jurisdictions_created;
        self.jurisdictions_reused += other.jurisdictions_reused;
        self.jurisdictions_missing += other.jurisdictions_missing;
        self.failures += other.failures;
    }
}

#[derive(Debug, Default, Clone)]
struct BackfillSellablesStats {
    processed: i64,
    created: i64,
    skipped: i64,
    failed: i64,
}

#[derive(Debug, Clone)]
struct BootstrapCoverageSpec {
    currency_code: String,
    currency_name: Option<String>,
    currency_minor_unit: i16,
    country_code: String,
    country_name: Option<String>,
}

#[derive(Debug, Default)]
struct PlaystationPurgeStats {
    provider_items: i64,
    provider_offers: i64,
    provider_media_links: i64,
    retailer_offers: i64,
    offer_jurisdictions: i64,
    prices: i64,
    current_price_rows: i64,
    ingest_runs: i64,
    retailer_provider_links: i64,
}

fn resolve_database_url(db_url: Option<String>) -> Result<String> {
    if let Some(url) = db_url {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    let env_url = env::db_url().with_context(|| "resolve_database_url: missing database URL")?;
    let trimmed = env_url.trim();
    if trimmed.is_empty() {
        bail!("database URL is empty; set SUPABASE_IPV6_DB / SUPABASE_DB_URL or pass --db-url");
    }
    Ok(trimmed.to_string())
}

async fn purge_playstation_data(db: &Db, dry_run: bool) -> Result<()> {
    let provider_id_opt = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM providers WHERE name = $1 OR slug = $2 ORDER BY id LIMIT 1",
    )
    .persistent(false)
    .bind("playstation_store")
    .bind("ps-store")
    .fetch_optional(&db.pool)
    .await?;

    let Some(provider_id) = provider_id_opt else {
        info!("ps-full-refresh: PlayStation provider not present; nothing to purge");
        return Ok(());
    };

    let retailer_id_opt = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM retailers WHERE slug = $1 OR name = $2 ORDER BY id LIMIT 1",
    )
    .persistent(false)
    .bind("playstation")
    .bind("PlayStation")
    .fetch_optional(&db.pool)
    .await?;

    let Some(retailer_id) = retailer_id_opt else {
        info!("ps-full-refresh: PlayStation retailer not present; nothing to purge");
        return Ok(());
    };

    let stats = PlaystationPurgeStats {
        provider_items: sqlx
            ::query_scalar::<_, i64>(
                "SELECT COALESCE(COUNT(*), 0) FROM provider_items WHERE provider_id = $1"
            )
            .persistent(false)
            .bind(provider_id)
            .fetch_one(&db.pool).await?,
        provider_offers: sqlx
            ::query_scalar::<_, i64>(
                r#"
                SELECT COALESCE(COUNT(*), 0)
                FROM provider_offers po
                JOIN provider_items pi ON pi.id = po.provider_item_id
                WHERE pi.provider_id = $1
                "#
            )
            .persistent(false)
            .bind(provider_id)
            .fetch_one(&db.pool).await?,
        provider_media_links: sqlx
            ::query_scalar::<_, i64>(
                r#"
                SELECT COALESCE(COUNT(*), 0)
                FROM provider_media_links pml
                JOIN provider_items pi ON pi.id = pml.provider_item_id
                WHERE pi.provider_id = $1
                "#
            )
            .persistent(false)
            .bind(provider_id)
            .fetch_one(&db.pool).await?,
        retailer_offers: sqlx
            ::query_scalar::<_, i64>(
                "SELECT COALESCE(COUNT(*), 0) FROM offers WHERE retailer_id = $1"
            )
            .persistent(false)
            .bind(retailer_id)
            .fetch_one(&db.pool).await?,
        offer_jurisdictions: sqlx
            ::query_scalar::<_, i64>(
                r#"
                SELECT COALESCE(COUNT(*), 0)
                FROM offer_jurisdictions oj
                JOIN offers o ON o.id = oj.offer_id
                WHERE o.retailer_id = $1
                "#
            )
            .persistent(false)
            .bind(retailer_id)
            .fetch_one(&db.pool).await?,
        prices: sqlx
            ::query_scalar::<_, i64>(
                r#"
                SELECT COALESCE(COUNT(*), 0)
                FROM prices p
                JOIN offer_jurisdictions oj ON oj.id = p.offer_jurisdiction_id
                JOIN offers o ON o.id = oj.offer_id
                WHERE o.retailer_id = $1
                "#
            )
            .persistent(false)
            .bind(retailer_id)
            .fetch_one(&db.pool).await?,
        current_price_rows: sqlx
            ::query_scalar::<_, i64>(
                r#"
                SELECT COALESCE(COUNT(*), 0)
                FROM current_price cp
                JOIN offer_jurisdictions oj ON oj.id = cp.offer_jurisdiction_id
                JOIN offers o ON o.id = oj.offer_id
                WHERE o.retailer_id = $1
                "#
            )
            .persistent(false)
            .bind(retailer_id)
            .fetch_one(&db.pool).await?,
        ingest_runs: sqlx
            ::query_scalar::<_, i64>(
                "SELECT COALESCE(COUNT(*), 0) FROM provider_ingest_runs WHERE provider_id = $1"
            )
            .persistent(false)
            .bind(provider_id)
            .fetch_one(&db.pool).await?,
        retailer_provider_links: sqlx
            ::query_scalar::<_, i64>(
                "SELECT COALESCE(COUNT(*), 0) FROM retailer_providers WHERE retailer_id = $1 AND provider_id = $2"
            )
            .persistent(false)
            .bind(retailer_id)
            .bind(provider_id)
            .fetch_one(&db.pool).await?,
    };

    info!(
        provider_id,
        retailer_id,
        provider_items = stats.provider_items,
        provider_offers = stats.provider_offers,
        provider_media_links = stats.provider_media_links,
        retailer_offers = stats.retailer_offers,
        offer_jurisdictions = stats.offer_jurisdictions,
        price_rows = stats.prices,
        current_price_rows = stats.current_price_rows,
        ingest_runs = stats.ingest_runs,
        retailer_provider_links = stats.retailer_provider_links,
        dry_run,
        "ps-full-refresh: PlayStation purge evaluation"
    );

    if dry_run {
        return Ok(());
    }

    let mut tx = db.pool.begin().await?;
    let deleted_ingest_runs = tx
        .execute(
            sqlx::query("DELETE FROM provider_ingest_runs WHERE provider_id = $1")
                .bind(provider_id),
        )
        .await?
        .rows_affected();

    let deleted_retailer_links = tx
        .execute(
            sqlx::query(
                "DELETE FROM retailer_providers WHERE retailer_id = $1 AND provider_id = $2",
            )
            .bind(retailer_id)
            .bind(provider_id),
        )
        .await?
        .rows_affected();

    let deleted_provider_items = tx
        .execute(sqlx::query("DELETE FROM provider_items WHERE provider_id = $1").bind(provider_id))
        .await?
        .rows_affected();

    let deleted_offers = tx
        .execute(sqlx::query("DELETE FROM offers WHERE retailer_id = $1").bind(retailer_id))
        .await?
        .rows_affected();

    tx.commit().await?;

    info!(
        provider_id,
        retailer_id,
        deleted_provider_items,
        deleted_offers,
        deleted_ingest_runs,
        deleted_retailer_links,
        "ps-full-refresh: PlayStation dataset purged (cascade removed offer jurisdictions, prices, current_price, provider_offers, provider_media_links)"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("gc");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init();

    if std::env::var_os("GC_LIST_SUBCOMMANDS").is_some() {
        let names: Vec<String> = Cli::command()
            .get_subcommands()
            .map(|cmd| cmd.get_name().to_string())
            .collect();
        eprintln!("available subcommands: {:?}", names);
        return Ok(());
    }

    let cli = Cli::parse();

    match cli.command {
        Commands::PsPrices {
            regions,
            max_pages,
            page_size,
        } => {
            use i_miss_rust::cli::playstation::{self, PricesCommandConfig};
            let cfg = PricesCommandConfig {
                regions,
                max_pages,
                page_size,
            };
            playstation::run_prices(cfg).await?;
        }
        Commands::PsRatings {
            locale,
            max_pages_ps4,
            max_pages_ps5,
            page_size,
            dry_run,
        } => {
            use i_miss_rust::cli::playstation::{self, RatingsCommandConfig};
            let cfg = RatingsCommandConfig {
                locale,
                max_pages_ps4,
                max_pages_ps5,
                page_size,
                dry_run,
            };
            playstation::run_ratings(cfg).await?;
        }
        Commands::PsDumpCategories => {
            use i_miss_rust::cli::playstation;
            playstation::run_dump_categories()?;
        }
        Commands::PsDumpDetail {
            product_id,
            locale,
            out,
        } => {
            use i_miss_rust::cli::playstation::{self, DumpDetailCommandConfig};
            let cfg = DumpDetailCommandConfig {
                product_id,
                locale,
                out_path: out,
            };
            playstation::run_dump_detail(cfg).await?;
        }
        Commands::PsDumpPrices => {
            use i_miss_rust::cli::playstation;
            playstation::run_dump_prices().await?;
        }
        Commands::PsDumpRaw => {
            use i_miss_rust::cli::playstation;
            playstation::run_dump_raw().await?;
        }
        Commands::PsExportProducts => {
            use i_miss_rust::cli::playstation;
            playstation::run_export_products().await?;
        }
        Commands::PsGenreScan => {
            use i_miss_rust::cli::playstation;
            playstation::run_genre_scan().await?;
        }
        Commands::PsIngestDemo => {
            use i_miss_rust::cli::playstation;
            playstation::run_ingest_demo().await?;
        }
        Commands::PsPricesDebug => {
            use i_miss_rust::cli::playstation;
            playstation::run_prices_debug().await?;
        }
        Commands::PsSearchCategories => {
            use i_miss_rust::cli::playstation;
            playstation::run_search_categories().await?;
        }
        Commands::PsCountRange {
            regions,
            ps4_category,
            ps5_category,
            page_size,
            total_pages,
            cutoff_year,
            rps_per_locale,
            retry_attempts,
            retry_backoff_ms,
        } => {
            use i_miss_rust::cli::playstation::{self, CountRangeCommandConfig};
            let cfg = CountRangeCommandConfig {
                regions,
                ps4_category,
                ps5_category,
                page_size,
                total_pages,
                cutoff_year,
                rps_per_locale,
                retry_attempts,
                retry_backoff_ms,
            };
            playstation::run_count_range(cfg).await?;
        }
        Commands::PsPriceProbe {
            locale,
            product_id,
            concept_id,
        } => {
            use i_miss_rust::cli::playstation::{self, PriceProbeCommandConfig};
            let cfg = PriceProbeCommandConfig {
                locale,
                product_id,
                concept_id,
            };
            playstation::run_price_probe(cfg).await?;
        }
        Commands::PsCaptureHeaders {
            regions,
            out,
            user_agent,
        } => {
            use i_miss_rust::cli::playstation::{self, CaptureHeadersCommandConfig};
            let cfg = CaptureHeadersCommandConfig {
                regions,
                out_path: out,
                user_agent,
            };
            playstation::run_capture_headers(cfg).await?;
        }
        Commands::PsFullRefresh {
            db_url,
            prune_demo: _prune_demo,
            dry_run,
            skip_counts,
            skip_purge_playstation,
            year_min,
            year_max,
        } => {
            let database_url = resolve_database_url(db_url)?;
            info!(url = %database_url, "ps-full-refresh: connecting");
            let db = Db::connect(&database_url, 5).await?;

            const DEFAULT_CHUNK: i64 = 250;
            const DEFAULT_LIMIT: i64 = i64::MAX;
            let chunk = DEFAULT_CHUNK;
            let limit_total = DEFAULT_LIMIT;

            let coverage_info = derive_retailer_coverage(&db, "playstation").await?;
            let mut base_country_code = "US".to_string();
            let mut base_country_name = default_country_name(&base_country_code);
            let mut base_currency_code = "USD".to_string();
            let mut base_currency_name = default_currency_name(&base_currency_code);
            let mut base_minor_unit: i16 = 2;
            let mut coverage_tokens: Vec<String> = Vec::new();

            if let Some(info) = coverage_info {
                base_country_code = info.base.country_code;
                base_country_name = if info.base.country_name.is_empty() {
                    default_country_name(&base_country_code)
                } else {
                    info.base.country_name
                };
                base_currency_code = info.base.currency_code;
                base_currency_name = if info.base.currency_name.is_empty() {
                    default_currency_name(&base_currency_code)
                } else {
                    info.base.currency_name
                };
                base_minor_unit = info.base.minor_unit;
                coverage_tokens.extend(info.additional_tokens);
                info!(
                    base_country = %base_country_code,
                    base_currency = %base_currency_code,
                    minor_unit = base_minor_unit,
                    additional_specs = coverage_tokens.len(),
                    "ps-full-refresh: derived coverage from existing PlayStation data"
                );
            } else {
                warn!(
                    "ps-full-refresh: no existing PlayStation coverage detected; falling back to default US/USD baseline"
                );
            }

            let purge_playstation = !skip_purge_playstation;
            if purge_playstation {
                purge_playstation_data(&db, dry_run).await?;
            } else {
                info!("ps-full-refresh: skipping PlayStation purge step");
            }

            const YEAR_MIN_DEFAULT: i32 = 2020;
            const YEAR_MAX_DEFAULT: i32 = 2025;
            let year_min_value = year_min.unwrap_or(YEAR_MIN_DEFAULT);
            let year_max_value = year_max.unwrap_or(YEAR_MAX_DEFAULT);
            if year_min_value > year_max_value {
                bail!(
                    "ps-full-refresh: year-min {} exceeds year-max {}",
                    year_min_value,
                    year_max_value
                );
            }

            let coverage_str = if coverage_tokens.is_empty() {
                None
            } else {
                Some(coverage_tokens.join(","))
            };

            if dry_run {
                info!(
                    year_min = year_min_value,
                    year_max = year_max_value,
                    "ps-full-refresh: dry-run would ingest PlayStation catalog within year window"
                );
                info!(
                    chunk_size = chunk,
                    limit_total = limit_total,
                    coverage = coverage_str.as_deref().unwrap_or("<none>"),
                    "ps-full-refresh: dry-run enabled; skipping bootstrap mutation"
                );
            } else {
                let totals = run_db_bootstrap_offers(
                    &db,
                    "PlayStation",
                    "playstation",
                    &base_currency_code,
                    Some(&base_currency_name),
                    base_minor_unit,
                    &base_country_code,
                    Some(&base_country_name),
                    coverage_str.as_deref(),
                    limit_total,
                    chunk,
                    false,
                )
                .await?;
                info!(
                    processed = totals.processed,
                    offers_created = totals.offers_created,
                    jurisdictions_created = totals.jurisdictions_created,
                    "ps-full-refresh: bootstrap completed"
                );
            }

            if dry_run {
                info!("ps-full-refresh: dry-run enabled; skipping provider ingests");
            } else {
                set_env_value("YEAR_MIN", year_min_value);
                set_env_value("YEAR_MAX", year_max_value);
                info!(
                    year_min = year_min_value,
                    year_max = year_max_value,
                    "ps-full-refresh: configured PlayStation year window"
                );
                let summary = psstore_seed_pipeline(&db).await?;
                info!(
                    provider_items = summary.provider_item_ids.len(),
                    offer_jurisdictions = summary.offer_jurisdiction_ids.len(),
                    price_rows = summary.total_price_rows_written,
                    "ps-full-refresh: PlayStation ingest completed"
                );

                let start = Instant::now();
                match run_nexarda_provider(&db).await {
                    Ok(total) => {
                        info!(
                            deals_ingested = total,
                            elapsed_ms = start.elapsed().as_millis() as u64,
                            "ps-full-refresh: Nexarda provider completed"
                        );
                    }
                    Err(err) => {
                        error!(error = %err, "ps-full-refresh: Nexarda provider failed");
                    }
                }

                let start = Instant::now();
                match run_giantbomb_ingest(&db).await {
                    Ok(Some(count)) => {
                        info!(
                            records = count,
                            elapsed_ms = start.elapsed().as_millis() as u64,
                            "ps-full-refresh: GiantBomb ingest completed"
                        );
                    }
                    Ok(None) => {
                        info!("ps-full-refresh: GiantBomb ingest skipped (payload missing)");
                    }
                    Err(err) => {
                        error!(error = %err, "ps-full-refresh: GiantBomb ingest failed");
                    }
                }

                let start = Instant::now();
                match SteamProvider::run_from_env(&db).await {
                    Ok(_) => {
                        info!(
                            elapsed_ms = start.elapsed().as_millis() as u64,
                            "ps-full-refresh: Steam provider completed"
                        );
                    }
                    Err(err) => {
                        error!(error = %err, "ps-full-refresh: Steam provider failed");
                    }
                }

                let start = Instant::now();
                match xbox_provider::run_from_env(&db).await {
                    Ok(_) => {
                        info!(
                            elapsed_ms = start.elapsed().as_millis() as u64,
                            "ps-full-refresh: Xbox provider completed"
                        );
                    }
                    Err(err) => {
                        error!(error = %err, "ps-full-refresh: Xbox provider failed");
                    }
                }

                if igdb_credentials_present() {
                    let start = Instant::now();
                    match igdb_client::run_from_env(&db).await {
                        Ok(_) => {
                            info!(
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "ps-full-refresh: IGDB provider completed"
                            );
                        }
                        Err(err) => {
                            warn!(error = %err, "ps-full-refresh: IGDB provider failed");
                        }
                    }
                } else {
                    info!("ps-full-refresh: IGDB provider skipped (credentials missing)");
                }

                let start = Instant::now();
                let rawg_api_key = std::env::var("RAWG_API_KEY").ok().filter(|v| !v.is_empty());
                match rawg::sync(&db, rawg_api_key).await {
                    Ok(_) => {
                        info!(
                            elapsed_ms = start.elapsed().as_millis() as u64,
                            "ps-full-refresh: RAWG provider completed"
                        );
                    }
                    Err(err) => {
                        error!(error = %err, "ps-full-refresh: RAWG provider failed");
                    }
                }
            }

            if skip_counts {
                info!("ps-full-refresh: skipping db-counts summary");
            } else {
                let cfg = DbCountsConfig {
                    database_url: Some(database_url.clone()),
                    show_recent_games: None,
                    recent_games_limit: None,
                };
                run_db_counts(cfg).await?;
            }
        }
        Commands::DbMigrate { .. } => {
            eprintln!("db-migrate command currently unavailable: missing cli module");
            bail!("db-migrate support temporarily disabled");
        }
        Commands::DbMigrateOnce { .. } => {
            eprintln!("db-migrate-once command currently unavailable: missing cli module");
            bail!("db-migrate support temporarily disabled");
        }
        Commands::DbCounts {
            db_url,
            recent_games,
            recent_games_limit,
        } => {
            use i_miss_rust::cli::db_counts::{DbCountsConfig, run};
            let cfg = DbCountsConfig {
                database_url: db_url,
                show_recent_games: if recent_games { Some(true) } else { None },
                recent_games_limit,
            };
            run(cfg).await?;
        }
        Commands::DbBackfillSellables {
            db_url,
            limit,
            chunk_size,
            dry_run,
        } => {
            let database_url = resolve_database_url(db_url)?;
            info!(url = %database_url, "db-backfill-sellables: connecting");
            let db = Db::connect(&database_url, 5).await?;

            let chunk = chunk_size.unwrap_or(250).max(1);
            let limit_total = limit.unwrap_or(i64::MAX);
            let stats = run_db_backfill_sellables(&db, limit_total, chunk, dry_run).await?;
            info!(
                processed = stats.processed,
                created = stats.created,
                skipped = stats.skipped,
                failed = stats.failed,
                dry_run = dry_run,
                "db-backfill-sellables: finished"
            );
        }
        Commands::DbBootstrapOffers {
            db_url,
            currency,
            currency_name,
            currency_minor_unit,
            country,
            country_name,
            coverage,
            retailer_name,
            retailer_slug,
            limit,
            chunk_size,
            dry_run,
        } => {
            let database_url = resolve_database_url(db_url)?;
            info!(url = %database_url, "db-bootstrap-offers: connecting");
            let db = Db::connect(&database_url, 5).await?;

            let chunk = chunk_size.unwrap_or(250).max(1);
            let limit_total = limit.unwrap_or(i64::MAX);
            let totals = run_db_bootstrap_offers(
                &db,
                &retailer_name,
                &retailer_slug,
                &currency,
                currency_name.as_deref(),
                currency_minor_unit,
                &country,
                country_name.as_deref(),
                coverage.as_deref(),
                limit_total,
                chunk,
                dry_run,
            )
            .await?;

            info!(
                processed = totals.processed,
                offers_created = totals.offers_created,
                offers_reused = totals.offers_reused,
                offers_missing = totals.offers_missing,
                jurisdictions_created = totals.jurisdictions_created,
                jurisdictions_reused = totals.jurisdictions_reused,
                jurisdictions_missing = totals.jurisdictions_missing,
                failures = totals.failures,
                dry_run = dry_run,
                "db-bootstrap-offers: finished"
            );
        }
        Commands::DbMissingStats { db_url } => {
            use i_miss_rust::cli::db_missing_stats::{DbMissingStatsConfig, run};
            let cfg = DbMissingStatsConfig {
                database_url: db_url,
            };
            run(cfg).await?;
        }
        Commands::DbSchemaAudit {
            db_url,
            tables,
            max_connections,
        } => {
            use i_miss_rust::database_ops::schema_audit::{DbSchemaAuditConfig, run};
            let table_filter = tables.map(|vals| {
                vals.into_iter()
                    .map(|t| t.trim().to_ascii_lowercase())
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
            });
            let cfg = DbSchemaAuditConfig {
                database_url: db_url,
                table_filter,
                max_connections,
            };
            run(cfg).await?;
        }
        Commands::UnifiedIngest {
            db_url,
            skip_backfill,
            skip_bootstrap,
            skip_ps_seed,
            limit,
            chunk_size,
            coverage,
            currency,
            currency_name,
            currency_minor_unit,
            country,
            country_name,
            retailer_name,
            retailer_slug,
            dry_run,
            ps_regions,
            ps_page_size,
            ps_total_pages,
            ps_start_page,
            ps_year_min,
            ps_year_max,
            ps_rps,
            ps_retry_attempts,
            ps_retry_backoff_ms,
            disable_ps_backfill,
            skip_nexarda,
            skip_giantbomb,
            skip_steam,
            skip_xbox,
            skip_igdb,
            skip_rawg,
        } => {
            let database_url = resolve_database_url(db_url)?;
            info!(url = %database_url, "unified-ingest: connecting");
            let db = Db::connect(&database_url, 5).await?;

            let chunk = chunk_size.unwrap_or(250).max(1);
            let limit_total = limit.unwrap_or(i64::MAX);

            let mut base_currency_code = currency;
            let mut base_currency_name = currency_name;
            let mut base_minor_unit = currency_minor_unit;
            let mut base_country_code = country;
            let mut base_country_name = country_name;
            let mut coverage_value = coverage;

            if coverage_value
                .as_ref()
                .map(|s| s.trim().is_empty())
                .unwrap_or(false)
            {
                coverage_value = None;
            }

            if coverage_value.is_none() {
                if let Some(derived) = derive_retailer_coverage(&db, &retailer_slug).await? {
                    let DerivedCoverage {
                        base,
                        additional_tokens,
                    } = derived;
                    let CoverageRow {
                        country_code,
                        country_name,
                        currency_code,
                        currency_name,
                        minor_unit,
                    } = base;

                    base_country_code = country_code;
                    base_country_name = Some(if country_name.is_empty() {
                        default_country_name(&base_country_code)
                    } else {
                        country_name
                    });

                    base_currency_code = currency_code;
                    base_currency_name = Some(if currency_name.is_empty() {
                        default_currency_name(&base_currency_code)
                    } else {
                        currency_name
                    });

                    base_minor_unit = minor_unit;

                    let additional_count = additional_tokens.len();
                    if additional_count > 0 {
                        let derived_str = additional_tokens.join(",");
                        coverage_value = Some(derived_str);
                    }

                    info!(
                        retailer = %retailer_slug,
                        base_country = %base_country_code,
                        base_currency = %base_currency_code,
                        additional_specs = additional_count,
                        "unified-ingest: derived coverage from existing retailer data"
                    );
                }
            }

            if skip_backfill {
                info!("unified-ingest: skipping sellable backfill step");
            } else {
                let stats = run_db_backfill_sellables(&db, limit_total, chunk, dry_run).await?;
                info!(
                    processed = stats.processed,
                    created = stats.created,
                    skipped = stats.skipped,
                    failed = stats.failed,
                    dry_run = dry_run,
                    "unified-ingest: sellable backfill completed"
                );
            }

            if skip_bootstrap {
                info!("unified-ingest: skipping offer bootstrap step");
            } else {
                let totals = run_db_bootstrap_offers(
                    &db,
                    &retailer_name,
                    &retailer_slug,
                    &base_currency_code,
                    base_currency_name.as_deref(),
                    base_minor_unit,
                    &base_country_code,
                    base_country_name.as_deref(),
                    coverage_value.as_deref(),
                    limit_total,
                    chunk,
                    dry_run,
                )
                .await?;
                info!(
                    processed = totals.processed,
                    offers_created = totals.offers_created,
                    offers_reused = totals.offers_reused,
                    offers_missing = totals.offers_missing,
                    jurisdictions_created = totals.jurisdictions_created,
                    jurisdictions_reused = totals.jurisdictions_reused,
                    jurisdictions_missing = totals.jurisdictions_missing,
                    failures = totals.failures,
                    dry_run = dry_run,
                    "unified-ingest: offer bootstrap completed"
                );
            }

            if skip_ps_seed {
                info!("unified-ingest: skipping PlayStation seed step");
            } else if dry_run {
                info!("unified-ingest: dry-run enabled; skipping PlayStation seed step");
            } else {
                info!("unified-ingest: starting PlayStation seed pipeline");
                set_env_from_option("PS_STORE_REGIONS", ps_regions);
                set_env_from_option("PS_PAGE_SIZE", ps_page_size);
                set_env_from_option("PS_TOTAL_PAGES", ps_total_pages);
                set_env_from_option("PS_PAGE_START", ps_start_page);
                set_env_from_option("YEAR_MIN", ps_year_min);
                set_env_from_option("YEAR_MAX", ps_year_max);
                set_env_from_option("PS_STORE_RPS", ps_rps);
                set_env_from_option("PS_STORE_MAX_RETRIES", ps_retry_attempts);
                set_env_from_option("PS_STORE_BACKOFF_MS", ps_retry_backoff_ms);
                if disable_ps_backfill {
                    set_env_value("PS_BACKFILL", "0");
                }

                let summary = psstore_seed_pipeline(&db).await?;
                info!(
                    provider_items = summary.provider_item_ids.len(),
                    offer_jurisdictions = summary.offer_jurisdiction_ids.len(),
                    price_rows = summary.total_price_rows_written,
                    "unified-ingest: PlayStation seed completed"
                );
            }

            let runtime = tokio::runtime::Handle::current();
            let mut provider_tasks: Vec<Arc<dyn Fn() + Send + Sync>> = Vec::new();

            if skip_nexarda {
                info!("unified-ingest: skipping Nexarda ingestion step");
            } else {
                let db_clone = db.clone();
                let handle = runtime.clone();
                provider_tasks.push(Arc::new(move || {
                    info!("unified-ingest: starting Nexarda ingestion");
                    let start = Instant::now();
                    match handle.block_on(run_nexarda_provider(&db_clone)) {
                        Ok(total) => {
                            info!(
                                deals_ingested = total,
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "unified-ingest: Nexarda ingestion completed"
                            );
                        }
                        Err(err) => {
                            error!(error = %err, "unified-ingest: Nexarda ingestion failed");
                        }
                    }
                }));
            }

            if skip_giantbomb {
                info!("unified-ingest: skipping GiantBomb ingest step");
            } else {
                let db_clone = db.clone();
                let handle = runtime.clone();
                provider_tasks.push(Arc::new(move || {
                    info!("unified-ingest: starting GiantBomb ingest");
                    let start = Instant::now();
                    match handle.block_on(run_giantbomb_ingest(&db_clone)) {
                        Ok(Some(count)) => {
                            info!(
                                records = count,
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "unified-ingest: GiantBomb ingest completed"
                            );
                        }
                        Ok(None) => {}
                        Err(err) => {
                            error!(error = %err, "unified-ingest: GiantBomb ingest failed");
                        }
                    }
                }));
            }

            if skip_steam {
                info!("unified-ingest: skipping Steam provider step");
            } else {
                let db_clone = db.clone();
                let handle = runtime.clone();
                provider_tasks.push(Arc::new(move || {
                    info!("unified-ingest: starting Steam provider run");
                    let start = Instant::now();
                    match handle.block_on(SteamProvider::run_from_env(&db_clone)) {
                        Ok(_) => {
                            info!(
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "unified-ingest: Steam provider completed"
                            );
                        }
                        Err(err) => {
                            error!(error = %err, "unified-ingest: Steam provider failed");
                        }
                    }
                }));
            }

            if skip_xbox {
                info!("unified-ingest: skipping Xbox provider step");
            } else {
                let db_clone = db.clone();
                let handle = runtime.clone();
                provider_tasks.push(Arc::new(move || {
                    info!("unified-ingest: starting Xbox provider run");
                    let start = Instant::now();
                    match handle.block_on(xbox_provider::run_from_env(&db_clone)) {
                        Ok(_) => {
                            info!(
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "unified-ingest: Xbox provider completed"
                            );
                        }
                        Err(err) => {
                            error!(error = %err, "unified-ingest: Xbox provider failed");
                        }
                    }
                }));
            }

            if skip_igdb {
                info!("unified-ingest: skipping IGDB provider step");
            } else if !igdb_credentials_present() {
                info!(
                    "unified-ingest: skipping IGDB provider (TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET not configured)"
                );
            } else {
                let db_clone = db.clone();
                let handle = runtime.clone();
                provider_tasks.push(Arc::new(move || {
                    info!("unified-ingest: starting IGDB provider run");
                    let start = Instant::now();
                    match handle.block_on(igdb_client::run_from_env(&db_clone)) {
                        Ok(_) => {
                            info!(
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "unified-ingest: IGDB provider completed"
                            );
                        }
                        Err(err) => {
                            error!(error = %err, "unified-ingest: IGDB provider failed");
                        }
                    }
                }));
            }

            if skip_rawg {
                info!("unified-ingest: skipping RAWG provider step");
            } else {
                let db_clone = db.clone();
                let handle = runtime.clone();
                provider_tasks.push(Arc::new(move || {
                    info!("unified-ingest: starting RAWG provider run");
                    let start = Instant::now();
                    let api_key = std::env::var("RAWG_API_KEY").ok().filter(|v| !v.is_empty());
                    match handle.block_on(rawg::sync(&db_clone, api_key)) {
                        Ok(_) => {
                            info!(
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                "unified-ingest: RAWG provider completed"
                            );
                        }
                        Err(err) => {
                            error!(error = %err, "unified-ingest: RAWG provider failed");
                        }
                    }
                }));
            }

            if !provider_tasks.is_empty() {
                let thread_env = std::env::var("UNIFIED_INGEST_THREADS")
                    .ok()
                    .and_then(|s| s.trim().parse::<usize>().ok())
                    .filter(|threads| *threads > 0);
                let mut builder = ThreadPoolBuilder::new()
                    .thread_name(|idx| format!("unified-ingest-provider-{idx}"));
                if let Some(threads) = thread_env {
                    info!(threads, "unified-ingest: configuring rayon provider pool");
                    builder = builder.num_threads(threads);
                }
                let pool = builder
                    .build()
                    .context("unified-ingest: failed to build rayon provider pool")?;
                pool.install(|| {
                    provider_tasks
                        .into_par_iter()
                        .for_each(|task| task.as_ref()());
                });
            }
        }
    }

    Ok(())
}

fn igdb_credentials_present() -> bool {
    fn non_empty(key: &str) -> bool {
        std::env::var(key)
            .ok()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    }
    non_empty("TWITCH_CLIENT_ID") && non_empty("TWITCH_CLIENT_SECRET")
}

fn build_nexarda_options_from_env() -> NexardaOptions {
    let products = std::env::var("NEXARDA_PRODUCTS")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let store_map = std::env::var("NEXARDA_STORE_MAP")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let default_regions = std::env::var("NEXARDA_DEFAULT_REGIONS")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let dynamic_overrides = std::env::var("NEXARDA_STORE_OVERRIDES")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let context = std::env::var("NEXARDA_CONTEXT")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    NexardaOptions {
        products,
        store_map,
        base_url: None,
        timeout: None,
        api_key: std::env::var("NEXARDA_API_KEY").ok(),
        auto_register_stores: Some(true),
        default_regions,
        dynamic_store_overrides: dynamic_overrides,
        default_tax_inclusive: Some(true),
        context,
    }
}

async fn run_nexarda_provider(db: &Db) -> Result<usize> {
    let base_url_opt = std::env::var("NEXARDA_BASE_URL").ok();
    let timeout_secs = std::env::var("NEXARDA_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok());
    let provider = NexardaProvider::new(base_url_opt.as_deref(), timeout_secs)?;
    let opts = build_nexarda_options_from_env();
    if opts.products.is_empty() {
        info!(
            "unified-ingest: Nexarda options contained no products; set NEXARDA_PRODUCTS or NEXARDA_PRODUCTS_FILE to enable this step"
        );
        return Ok(0);
    }
    provider.ingest_to_db(db, opts).await
}

async fn run_giantbomb_ingest(db: &Db) -> Result<Option<usize>> {
    let path = std::env::var("GIANT_BOMB_FILE")
        .unwrap_or_else(|_| "keep/giant_bomb_games_detailed.json".into());
    if !Path::new(&path).exists() {
        info!(path = %path, "GiantBomb ingest payload not found; skipping");
        return Ok(None);
    }
    let limit = std::env::var("GB_LIMIT").ok().and_then(|s| s.parse().ok());
    let count = gb_ingest_from_file(db, &path, limit).await?;
    Ok(Some(count))
}

fn default_currency_name(code: &str) -> String {
    (match code {
        "USD" => "US Dollar",
        "EUR" => "Euro",
        "GBP" => "British Pound",
        "CAD" => "Canadian Dollar",
        "AUD" => "Australian Dollar",
        "NZD" => "New Zealand Dollar",
        "JPY" => "Japanese Yen",
        "CNY" => "Chinese Yuan",
        "KRW" => "South Korean Won",
        "BRL" => "Brazilian Real",
        "MXN" => "Mexican Peso",
        "CHF" => "Swiss Franc",
        "SEK" => "Swedish Krona",
        "NOK" => "Norwegian Krone",
        "DKK" => "Danish Krone",
        "PLN" => "Polish Złoty",
        other => other,
    })
    .to_string()
}

fn default_country_name(code: &str) -> String {
    (match code {
        "US" => "United States",
        "GB" => "United Kingdom",
        "CA" => "Canada",
        "DE" => "Germany",
        "FR" => "France",
        "AU" => "Australia",
        "NZ" => "New Zealand",
        "JP" => "Japan",
        "CN" => "China",
        "KR" => "South Korea",
        "BR" => "Brazil",
        "MX" => "Mexico",
        "CH" => "Switzerland",
        "SE" => "Sweden",
        "NO" => "Norway",
        "DK" => "Denmark",
        "PL" => "Poland",
        other => other,
    })
    .to_string()
}

fn parse_additional_coverage(
    raw: &str,
    fallback_currency_code: &str,
    fallback_minor_unit: i16,
) -> Result<Vec<BootstrapCoverageSpec>> {
    let normalized = raw
        .replace(',', " ")
        .replace(';', " ")
        .replace('\n', " ")
        .replace('\t', " ");

    let mut specs = Vec::new();
    for token in normalized.split_whitespace() {
        let parts: Vec<&str> = token.split(':').collect();
        if parts.is_empty() {
            continue;
        }

        let country_code = parts[0].trim().to_ascii_uppercase();
        if country_code.len() != 2 {
            bail!(
                "invalid coverage country code '{}': expected 2 characters",
                parts[0]
            );
        }

        let currency_code = parts
            .get(1)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_else(|| fallback_currency_code.to_ascii_uppercase());

        let currency_minor_unit = parts
            .get(2)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<i16>())
            .transpose()
            .map_err(|err| anyhow!("invalid minor unit for coverage '{}': {}", token, err))?
            .unwrap_or(fallback_minor_unit);

        let country_name = parts
            .get(3)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let currency_name = parts
            .get(4)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        specs.push(BootstrapCoverageSpec {
            currency_code,
            currency_name,
            currency_minor_unit,
            country_code,
            country_name,
        });
    }

    Ok(specs)
}

async fn run_db_backfill_sellables(
    db: &Db,
    limit_total: i64,
    chunk: i64,
    dry_run: bool,
) -> Result<BackfillSellablesStats> {
    info!(
        chunk_size = chunk,
        limit_total = limit_total,
        dry_run = dry_run,
        "db-backfill-sellables: starting run"
    );

    let mut stats = BackfillSellablesStats::default();
    let mut last_id: i64 = 0;
    let started = Instant::now();

    loop {
        if stats.processed >= limit_total {
            info!(
                processed = stats.processed,
                limit_total = limit_total,
                "db-backfill-sellables: reached limit"
            );
            break;
        }

        let remaining = limit_total - stats.processed;
        let fetch = min(chunk, remaining);
        debug!(
            last_id = last_id,
            fetch = fetch,
            "db-backfill-sellables: fetching candidate chunk"
        );

        let rows = sqlx::query(
            r#"
            SELECT vgt.id, vgt.product_id
            FROM public.video_game_titles vgt
            LEFT JOIN public.sellables s
              ON s.software_title_id = vgt.id
            WHERE s.id IS NULL
              AND vgt.product_id IS NOT NULL
              AND vgt.id > $1
            ORDER BY vgt.id
            LIMIT $2
            "#,
        )
        .bind(last_id)
        .bind(fetch)
        .fetch_all(&db.pool)
        .await?;

        info!(
            chunk_size = rows.len(),
            last_id = last_id,
            fetch = fetch,
            "db-backfill-sellables: chunk loaded"
        );

        if rows.is_empty() {
            info!(
                last_id = last_id,
                processed = stats.processed,
                "db-backfill-sellables: no additional titles without sellables"
            );
            break;
        }

        for row in rows {
            let title_id: i64 = row.try_get("id")?;
            let product_id_opt: Option<i64> = row.try_get("product_id")?;
            debug!(
                title_id = title_id,
                product_id = ?product_id_opt,
                "db-backfill-sellables: processing title"
            );

            last_id = title_id;
            stats.processed += 1;

            let Some(product_id) = product_id_opt else {
                stats.skipped += 1;
                warn!(
                    title_id = title_id,
                    "db-backfill-sellables: skipped due to NULL product_id despite filter"
                );
                if stats.processed >= limit_total {
                    debug!(
                        processed = stats.processed,
                        limit_total = limit_total,
                        "db-backfill-sellables: limit reached while skipping"
                    );
                    break;
                }
                continue;
            };

            if dry_run {
                info!(
                    title_id = title_id,
                    product_id = product_id,
                    "db-backfill-sellables: dry-run would create sellable"
                );
                stats.skipped += 1;
                if stats.processed >= limit_total {
                    debug!(
                        processed = stats.processed,
                        limit_total = limit_total,
                        "db-backfill-sellables: limit reached during dry-run"
                    );
                    break;
                }
                continue;
            }

            match ensure_sellable(db, "software", product_id).await {
                Ok(_) => {
                    stats.created += 1;
                    info!(
                        title_id = title_id,
                        product_id = product_id,
                        "db-backfill-sellables: ensured sellable"
                    );
                }
                Err(err) => {
                    stats.failed += 1;
                    error!(
                        title_id = title_id,
                        product_id = product_id,
                        error = %err,
                        "db-backfill-sellables: failed to ensure sellable"
                    );
                }
            }

            if stats.processed >= limit_total {
                debug!(
                    processed = stats.processed,
                    limit_total = limit_total,
                    "db-backfill-sellables: limit reached after ensure"
                );
                break;
            }
        }
    }

    info!(
        processed = stats.processed,
        created = stats.created,
        skipped = stats.skipped,
        failed = stats.failed,
        dry_run = dry_run,
        elapsed_ms = started.elapsed().as_millis(),
        "db-backfill-sellables: completed"
    );

    Ok(stats)
}

async fn bootstrap_offers_for_spec(
    db: &Db,
    retailer_id: i64,
    spec: &BootstrapCoverageSpec,
    limit_total: i64,
    chunk: i64,
    dry_run: bool,
) -> Result<BootstrapStats> {
    let currency_name = spec
        .currency_name
        .clone()
        .unwrap_or_else(|| default_currency_name(&spec.currency_code));
    info!(
        currency_code = %spec.currency_code,
        currency_name = %currency_name,
        minor_unit = spec.currency_minor_unit,
        "db-bootstrap-offers: ensuring currency"
    );

    let currency_id = ensure_currency(
        db,
        &spec.currency_code,
        &currency_name,
        spec.currency_minor_unit,
    )
    .await?;

    let country_name = spec
        .country_name
        .clone()
        .unwrap_or_else(|| default_country_name(&spec.country_code));
    info!(
        country_code = %spec.country_code,
        country_name = %country_name,
        currency_id = currency_id,
        "db-bootstrap-offers: ensuring country"
    );
    let country_id = ensure_country(db, &spec.country_code, &country_name, currency_id).await?;
    let jurisdiction_id = ensure_national_jurisdiction(db, country_id).await?;
    info!(
        country_id = country_id,
        jurisdiction_id = jurisdiction_id,
        "db-bootstrap-offers: jurisdiction ensured"
    );

    let mut stats = BootstrapStats::default();
    let mut last_id: i64 = 0;
    let started = Instant::now();

    loop {
        if stats.processed >= limit_total {
            info!(
                country = %spec.country_code,
                processed = stats.processed,
                limit_total = limit_total,
                "db-bootstrap-offers: coverage spec limit reached"
            );
            break;
        }

        let remaining = limit_total - stats.processed;
        let fetch = min(chunk, remaining);
        debug!(
            country = %spec.country_code,
            last_id = last_id,
            fetch = fetch,
            "db-bootstrap-offers: fetching sellable chunk"
        );

        let rows = sqlx::query(
            r#"
            SELECT s.id AS sellable_id,
                   o.id AS offer_id,
                   oj.id AS offer_jurisdiction_id,
                   vgt.id AS title_id,
                   COALESCE(vgt.title, '') AS title
            FROM public.sellables s
            LEFT JOIN public.offers o
              ON o.sellable_id = s.id
             AND o.retailer_id = $3
            LEFT JOIN public.offer_jurisdictions oj
              ON oj.offer_id = o.id
             AND oj.jurisdiction_id = $4
            LEFT JOIN public.video_game_titles vgt
              ON vgt.id = s.software_title_id
            WHERE s.kind = 'software'::sellable_kind
              AND s.id > $1
            ORDER BY s.id
            LIMIT $2
            "#,
        )
        .bind(last_id)
        .bind(fetch)
        .bind(retailer_id)
        .bind(jurisdiction_id)
        .fetch_all(&db.pool)
        .await?;

        if rows.is_empty() {
            info!(
                country = %spec.country_code,
                last_id = last_id,
                processed = stats.processed,
                "db-bootstrap-offers: coverage spec complete (no additional sellables)"
            );
            break;
        }

        for row in rows {
            let sellable_id: i64 = row.try_get("sellable_id")?;
            let offer_existing: Option<i64> = row.try_get("offer_id")?;
            let oj_existing: Option<i64> = row.try_get("offer_jurisdiction_id")?;
            let title: Option<String> = row.try_get("title")?;

            let title_display = title
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("<untitled>");

            last_id = sellable_id;
            stats.processed += 1;

            let missing_offer = offer_existing.is_none();
            let missing_jurisdiction = oj_existing.is_none();

            if missing_offer {
                stats.offers_missing += 1;
            } else {
                stats.offers_reused += 1;
            }

            if missing_jurisdiction {
                stats.jurisdictions_missing += 1;
            } else {
                stats.jurisdictions_reused += 1;
            }

            if dry_run {
                debug!(
                    sellable_id = sellable_id,
                    title = %title_display,
                    missing_offer,
                    missing_jurisdiction,
                    "db-bootstrap-offers: dry-run inspection"
                );
                if stats.processed >= limit_total {
                    info!(
                        country = %spec.country_code,
                        processed = stats.processed,
                        limit_total = limit_total,
                        "db-bootstrap-offers: dry-run limit reached"
                    );
                    break;
                }
                continue;
            }

            let offer_id = if let Some(existing) = offer_existing {
                existing
            } else {
                match ensure_offer(db, sellable_id, retailer_id, None).await {
                    Ok(id) => {
                        stats.offers_created += 1;
                        info!(
                            sellable_id = sellable_id,
                            title = %title_display,
                            offer_id = id,
                            retailer_id = retailer_id,
                            "db-bootstrap-offers: ensured offer"
                        );
                        id
                    }
                    Err(err) => {
                        stats.failures += 1;
                        error!(
                            sellable_id = sellable_id,
                            title = %title_display,
                            error = %err,
                            "db-bootstrap-offers: failed to ensure offer"
                        );
                        if stats.processed >= limit_total {
                            info!(
                                country = %spec.country_code,
                                processed = stats.processed,
                                limit_total = limit_total,
                                "db-bootstrap-offers: limit reached after offer failure"
                            );
                        }
                        continue;
                    }
                }
            };

            if missing_jurisdiction {
                match ensure_offer_jurisdiction(db, offer_id, jurisdiction_id, currency_id).await {
                    Ok(_) => {
                        stats.jurisdictions_created += 1;
                        info!(
                            sellable_id = sellable_id,
                            title = %title_display,
                            offer_id = offer_id,
                            jurisdiction_id = jurisdiction_id,
                            "db-bootstrap-offers: ensured offer jurisdiction"
                        );
                    }
                    Err(err) => {
                        stats.failures += 1;
                        error!(
                            sellable_id = sellable_id,
                            title = %title_display,
                            offer_id = offer_id,
                            error = %err,
                            "db-bootstrap-offers: failed to ensure offer jurisdiction"
                        );
                    }
                }
            }

            if stats.processed >= limit_total {
                info!(
                    country = %spec.country_code,
                    processed = stats.processed,
                    limit_total = limit_total,
                    "db-bootstrap-offers: limit reached"
                );
                break;
            }
        }
    }

    info!(
        country = %spec.country_code,
        currency = %spec.currency_code,
        processed = stats.processed,
        offers_created = stats.offers_created,
        offers_reused = stats.offers_reused,
        offers_missing = stats.offers_missing,
        jurisdictions_created = stats.jurisdictions_created,
        jurisdictions_reused = stats.jurisdictions_reused,
        jurisdictions_missing = stats.jurisdictions_missing,
        failures = stats.failures,
        dry_run = dry_run,
        elapsed_ms = started.elapsed().as_millis(),
        "db-bootstrap-offers: coverage spec completed"
    );

    Ok(stats)
}

async fn run_db_bootstrap_offers(
    db: &Db,
    retailer_name: &str,
    retailer_slug: &str,
    currency_code: &str,
    currency_name: Option<&str>,
    currency_minor_unit: i16,
    country_code: &str,
    country_name: Option<&str>,
    coverage: Option<&str>,
    limit_total: i64,
    chunk: i64,
    dry_run: bool,
) -> Result<BootstrapStats> {
    let base_currency_code = currency_code.trim().to_ascii_uppercase();
    let base_country_code = country_code.trim().to_ascii_uppercase();

    let mut coverage_specs = Vec::new();
    coverage_specs.push(BootstrapCoverageSpec {
        currency_code: base_currency_code.clone(),
        currency_name: currency_name.map(|s| s.to_string()),
        currency_minor_unit,
        country_code: base_country_code.clone(),
        country_name: country_name.map(|s| s.to_string()),
    });

    if let Some(extra) = coverage {
        let additional =
            parse_additional_coverage(extra, &base_currency_code, currency_minor_unit)?;
        coverage_specs.extend(additional);
    }

    let mut seen_specs: HashSet<(String, String)> = HashSet::new();
    let mut specs: Vec<BootstrapCoverageSpec> = Vec::new();
    for spec in coverage_specs.into_iter() {
        let key = (spec.country_code.clone(), spec.currency_code.clone());
        if seen_specs.insert(key) {
            specs.push(spec);
        } else {
            warn!(
                country = %spec.country_code,
                currency = %spec.currency_code,
                "db-bootstrap-offers: duplicate coverage spec skipped"
            );
        }
    }

    if specs.is_empty() {
        warn!("db-bootstrap-offers: no coverage specs resolved; nothing to do");
        return Ok(BootstrapStats::default());
    }

    let retailer_id = ensure_retailer(db, retailer_name, Some(retailer_slug)).await?;
    info!(
        retailer_id = retailer_id,
        retailer_name = %retailer_name,
        retailer_slug = %retailer_slug,
        "db-bootstrap-offers: retailer ensured"
    );

    info!(
        specs = specs.len(),
        chunk_size = chunk,
        limit_total = limit_total,
        dry_run = dry_run,
        "db-bootstrap-offers: starting coverage processing"
    );

    let overall_start = Instant::now();
    let mut totals = BootstrapStats::default();

    for spec in &specs {
        info!(
            country = %spec.country_code,
            currency = %spec.currency_code,
            minor_unit = spec.currency_minor_unit,
            "db-bootstrap-offers: processing coverage spec"
        );
        let stats =
            bootstrap_offers_for_spec(db, retailer_id, spec, limit_total, chunk, dry_run).await?;

        totals.absorb(&stats);
    }

    info!(
        specs = specs.len(),
        processed = totals.processed,
        offers_created = totals.offers_created,
        offers_reused = totals.offers_reused,
        offers_missing = totals.offers_missing,
        jurisdictions_created = totals.jurisdictions_created,
        jurisdictions_reused = totals.jurisdictions_reused,
        jurisdictions_missing = totals.jurisdictions_missing,
        failures = totals.failures,
        dry_run = dry_run,
        elapsed_ms = overall_start.elapsed().as_millis(),
        "db-bootstrap-offers: completed"
    );

    Ok(totals)
}

struct CoverageRow {
    country_code: String,
    country_name: String,
    currency_code: String,
    currency_name: String,
    minor_unit: i16,
}

struct DerivedCoverage {
    base: CoverageRow,
    additional_tokens: Vec<String>,
}

async fn derive_retailer_coverage(db: &Db, retailer_slug: &str) -> Result<Option<DerivedCoverage>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            c.iso2 AS country_code,
            c.name AS country_name,
            cur.code AS currency_code,
            cur.name AS currency_name,
            cur.minor_unit AS minor_unit
        FROM offer_jurisdictions oj
        JOIN offers o ON o.id = oj.offer_id
        JOIN retailers r ON r.id = o.retailer_id
        JOIN jurisdictions j ON j.id = oj.jurisdiction_id
        JOIN countries c ON c.id = j.country_id
        JOIN currencies cur ON cur.id = oj.currency_id
        WHERE r.slug = $1
        ORDER BY c.iso2, cur.code
        "#,
    )
    .bind(retailer_slug)
    .fetch_all(&db.pool)
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let mut specs: Vec<CoverageRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let country_code: String = row.try_get("country_code")?;
        let country_name: String = row.try_get("country_name")?;
        let currency_code: String = row.try_get("currency_code")?;
        let currency_name: String = row.try_get("currency_name")?;
        let minor_unit: i16 = row.try_get("minor_unit")?;
        specs.push(CoverageRow {
            country_code,
            country_name,
            currency_code,
            currency_name,
            minor_unit,
        });
    }

    let base_idx = specs
        .iter()
        .position(|spec| spec.country_code == "US")
        .unwrap_or(0);
    let base = specs.remove(base_idx);
    let additional_tokens = specs
        .into_iter()
        .map(|spec| {
            format!(
                "{}:{}:{}",
                spec.country_code, spec.currency_code, spec.minor_unit
            )
        })
        .collect();

    Ok(Some(DerivedCoverage {
        base,
        additional_tokens,
    }))
}

fn set_env_from_option<T: ToString>(key: &str, value: Option<T>) {
    if let Some(v) = value {
        set_env_value(key, v);
    }
}

fn set_env_value<T: ToString>(key: &str, value: T) {
    unsafe {
        std::env::set_var(key, value.to_string());
    }
}
