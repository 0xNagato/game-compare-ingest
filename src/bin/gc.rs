use anyhow::{anyhow, bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use i_miss_rust::cli::db_counts::{run as run_db_counts, DbCountsConfig};
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::giantbomb::ingest::ingest_from_file as gb_ingest_from_file;
use i_miss_rust::database_ops::igdb::client as igdb_client;
use i_miss_rust::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
    ensure_offer_jurisdiction, ensure_retailer, ensure_sellable, php_compat_schema,
};
use i_miss_rust::database_ops::itad;
use i_miss_rust::database_ops::nexarda::provider::{NexardaOptions, NexardaProvider};
use i_miss_rust::database_ops::rawg;
use i_miss_rust::database_ops::steam::provider::SteamProvider;
use i_miss_rust::database_ops::tgdb;

use i_miss_rust::psstore_seed_pipeline;
use i_miss_rust::util::env;
use rayon::{prelude::*, ThreadPoolBuilder};
use sqlx::{Executor, Row};
use std::cmp::min;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::{sleep, Instant};
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
        /// Skip IGDB catalogue ingestion
        #[arg(long, default_value_t = false)]
        skip_igdb: bool,
        /// Skip IsThereAnyDeal (ITAD) deals ingestion
        #[arg(long, default_value_t = false)]
        skip_itad: bool,
        /// Skip TheGamesDB (TGDB) mirror ingestion
        #[arg(long, default_value_t = false)]
        skip_tgdb: bool,
        /// Skip RAWG metadata ingestion
        #[arg(long, default_value_t = false)]
        skip_rawg: bool,
        /// Skip Xbox Store ingestion
        #[arg(long, default_value_t = false)]
        skip_xbox: bool,
        /// Enable Xbox year-based browse discovery
        #[arg(long, default_value_t = false)]
        xbox_enable_browse: bool,
        /// Xbox browse minimum release year
        #[arg(long)]
        xbox_year_min: Option<i32>,
        /// Xbox browse maximum release year
        #[arg(long)]
        xbox_year_max: Option<i32>,
        /// Xbox browse page size
        #[arg(long)]
        xbox_browse_page_size: Option<usize>,
        /// Xbox browse max pages per year
        #[arg(long)]
        xbox_browse_max_pages: Option<usize>,
        /// Xbox browse markets (comma-separated, e.g., "US,GB,JP")
        #[arg(long)]
        xbox_browse_markets: Option<String>,

        /// When set, re-runs the provider ingestion steps in a loop.
        ///
        /// Notes:
        /// - Backfill/bootstrap/ps-seed are still only executed once per invocation.
        /// - The loop only repeats provider steps (Nexarda/IGDB/RAWG/TGDB/ITAD/etc).
        #[arg(long)]
        loop_secs: Option<u64>,

        /// Optional max number of provider loop iterations (useful for smoke tests).
        ///
        /// When omitted and --loop-secs is set, the loop runs indefinitely.
        #[arg(long)]
        max_loops: Option<u32>,
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
                JOIN provider_items pi ON pi.id = po.video_game_source_id
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
                FROM vg_source_media_links pml
                JOIN provider_items pi ON pi.id = pml.video_game_source_id
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
                    provider_items = summary.video_game_source_ids.len(),
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
            use i_miss_rust::cli::db_counts::{run, DbCountsConfig};
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
            use i_miss_rust::cli::db_missing_stats::{run, DbMissingStatsConfig};
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
            use i_miss_rust::database_ops::schema_audit::{run, DbSchemaAuditConfig};
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
            skip_igdb,
            skip_itad,
            skip_tgdb,
            skip_rawg,
            skip_xbox,
            xbox_enable_browse,
            xbox_year_min,
            xbox_year_max,
            xbox_browse_page_size,
            xbox_browse_max_pages,
            xbox_browse_markets,
            loop_secs,
            max_loops,
        } => {
            let database_url = resolve_database_url(db_url)?;
            info!(url = %redact_postgres_url(&database_url), "unified-ingest: connecting");
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

            if !skip_bootstrap && coverage_value.is_none() {
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
                    provider_items = summary.video_game_source_ids.len(),
                    offer_jurisdictions = summary.offer_jurisdiction_ids.len(),
                    price_rows = summary.total_price_rows_written,
                    "unified-ingest: PlayStation seed completed"
                );
            }

            let loop_interval = loop_secs.and_then(|secs| {
                if secs > 0 {
                    Some(Duration::from_secs(secs))
                } else {
                    None
                }
            });

            if loop_interval.is_some() {
                info!(
                    loop_secs = loop_secs.unwrap_or_default(),
                    max_loops = max_loops,
                    "unified-ingest: live provider loop enabled"
                );
            }

            let max_provider_loops = max_loops.unwrap_or(u32::MAX);
            let mut provider_iteration: u32 = 0;

            // Provider retry policy: at least 3 retries for any failures.
            // Interpreted as: initial attempt + N retries => total attempts = retries + 1.
            // You can raise it via GC_PROVIDER_RETRIES, but cannot lower it below 3.
            let provider_retries: u32 = std::env::var("GC_PROVIDER_RETRIES")
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(3)
                .max(3);

            // Initialize Xbox provider once to reuse auth token cache
            let xbox_provider = if !skip_xbox {
                use i_miss_rust::database_ops::xbox::provider::XboxProvider;
                match XboxProvider::new() {
                    Ok(p) => Some(p),
                    Err(e) => {
                        warn!(error=%e, "unified-ingest: failed to initialize Xbox provider; skipping");
                        None
                    }
                }
            } else {
                None
            };

            loop {
                provider_iteration = provider_iteration.saturating_add(1);
                info!(
                    iteration = provider_iteration,
                    "unified-ingest: provider iteration starting"
                );

                type ProviderTask = Box<dyn FnOnce() + Send>;
                let runtime = tokio::runtime::Handle::current();
                let mut provider_tasks: Vec<ProviderTask> = Vec::new();

                // Build provider tasks based on skip flags
                if !skip_nexarda {
                    let db_clone = db.clone();
                    let handle = runtime.clone();
                    let retries = provider_retries;
                    provider_tasks.push(Box::new(move || {
                        info!("unified-ingest: starting Nexarda ingestion");
                        let start = Instant::now();
                        match run_with_retries_blocking("nexarda", retries, || {
                            handle.block_on(run_nexarda_provider(&db_clone))
                        }) {
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

                if !skip_giantbomb {
                    let db_clone = db.clone();
                    let handle = runtime.clone();
                    let retries = provider_retries;
                    provider_tasks.push(Box::new(move || {
                        info!("unified-ingest: starting GiantBomb ingest");
                        let start = Instant::now();
                        match run_with_retries_blocking("giantbomb", retries, || {
                            handle.block_on(run_giantbomb_ingest(&db_clone))
                        }) {
                            Ok(Some(count)) => {
                                info!(
                                    records = count,
                                    elapsed_ms = start.elapsed().as_millis() as u64,
                                    "unified-ingest: GiantBomb ingest completed"
                                );
                            }
                            Ok(None) => {
                                info!("unified-ingest: GiantBomb ingest skipped (payload missing)");
                            }
                            Err(err) => {
                                error!(error = %err, "unified-ingest: GiantBomb ingest failed");
                            }
                        }
                    }));
                }

                if !skip_steam {
                    let db_clone = db.clone();
                    let handle = runtime.clone();
                    let retries = provider_retries;
                    provider_tasks.push(Box::new(move || {
                        info!("unified-ingest: starting Steam provider run");
                        let start = Instant::now();
                        match run_with_retries_blocking("steam", retries, || {
                            handle.block_on(SteamProvider::run_from_env(&db_clone))
                        }) {
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

                if !skip_igdb && igdb_credentials_present() {
                    // Default to the monthly top-by-genre run unless the user explicitly configured IGDB_MODE.
                    // This keeps `gc igdb` (standalone) behavior unchanged while integrating the new mode
                    // into `gc unified-ingest`.
                    set_env_if_missing("IGDB_MODE", "top-monthly");

                    let db_clone = db.clone();
                    let handle = runtime.clone();
                    let retries = provider_retries;
                    provider_tasks.push(Box::new(move || {
                        info!("unified-ingest: starting IGDB provider run");
                        let start = Instant::now();
                        match run_with_retries_blocking("igdb", retries, || {
                            handle.block_on(igdb_client::run_from_env(&db_clone))
                        }) {
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

                if !skip_rawg {
                    // RAWG's internal default mode is a wide year-range crawl; in unified ingest we want a
                    // small, predictable run unless the user opted into something heavier.
                    set_env_if_missing("RAWG_MODE", "top_monthly");
                    // Reduce screenshot flooding in legacy/media-heavy schemas unless explicitly overridden.
                    set_env_if_missing("RAWG_MAX_SCREENSHOTS", "4");

                    let db_clone = db.clone();
                    let handle = runtime.clone();
                    let retries = provider_retries;
                    provider_tasks.push(Box::new(move || {
                        info!("unified-ingest: starting RAWG provider run");
                        let start = Instant::now();
                        let api_key = std::env::var("RAWG_API_KEY").ok().filter(|v| !v.is_empty());
                        match run_with_retries_blocking("rawg", retries, || {
                            handle.block_on(rawg::sync(&db_clone, api_key.clone()))
                        }) {
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

                if !skip_tgdb {
                    // TGDB is used as a catalogue mirror source and is intentionally gated so we don't
                    // accidentally hammer it in unattended environments.
                    let enabled = env::env_flag("CATALOGUE_SOURCE_TGDB_ENABLED", false)
                        || env::env_flag("TGDB_ENABLED", false);
                    if !enabled {
                        info!(
                            "unified-ingest: TGDB disabled (set CATALOGUE_SOURCE_TGDB_ENABLED=1 to enable)"
                        );
                    } else {
                        let api_key = env::env_opt("TGDB_API_KEY");
                        let allow_anon = env::env_flag("TGDB_ALLOW_ANON", false);
                        if api_key.is_some() || allow_anon {
                            let db_clone = db.clone();
                            let handle = runtime.clone();
                            let retries = provider_retries;
                            provider_tasks.push(Box::new(move || {
                                info!("unified-ingest: starting TGDB provider run");
                                let start = Instant::now();
                                match run_with_retries_blocking("tgdb", retries, || {
                                    handle.block_on(tgdb::sync(&db_clone, api_key.clone()))
                                }) {
                                    Ok(_) => {
                                        info!(
                                            elapsed_ms = start.elapsed().as_millis() as u64,
                                            "unified-ingest: TGDB provider completed"
                                        );
                                    }
                                    Err(err) => {
                                        // Prefer debug formatting so we get the full anyhow chain (root cause).
                                        error!(error = ?err, "unified-ingest: TGDB provider failed");
                                    }
                                }
                            }));
                        } else {
                            warn!(
                                "unified-ingest: TGDB enabled but TGDB_API_KEY missing (or set TGDB_ALLOW_ANON=1 to attempt anonymous calls)"
                            );
                        }
                    }
                }

                if !skip_itad {
                    // ITAD requires an API key for most usage; allow opt-in anonymous calls.
                    let api_key = env::env_opt("ITAD_API_KEY");
                    let allow_anon = env::env_flag("ITAD_ALLOW_ANON", false);
                    if api_key.is_some() || allow_anon {
                        let db_clone = db.clone();
                        let handle = runtime.clone();
                        let retries = provider_retries;
                        provider_tasks.push(Box::new(move || {
                            info!("unified-ingest: starting ITAD provider run");
                            let start = Instant::now();
                            match run_with_retries_blocking("itad", retries, || {
                                handle.block_on(itad::sync(&db_clone, api_key.clone()))
                            }) {
                                Ok(summary) => {
                                    info!(
                                        provider_items = summary.video_game_source_ids.len(),
                                        offer_jurisdictions = summary.offer_jurisdiction_ids.len(),
                                        price_rows = summary.total_price_rows_written,
                                        elapsed_ms = start.elapsed().as_millis() as u64,
                                        "unified-ingest: ITAD provider completed"
                                    );
                                }
                                Err(err) => {
                                    // Prefer debug formatting so we get the full anyhow chain (root cause).
                                    error!(error = ?err, "unified-ingest: ITAD provider failed");
                                }
                            }
                        }));
                    }
                }

                if !skip_xbox {
                    let db_clone = db.clone();
                    let handle = runtime.clone();
                    let retries = provider_retries;
                    // Clone the provider option for this iteration to reuse the auth cache
                    let provider_opt = xbox_provider.clone();

                    // Clone Xbox parameters from command args
                    let enable_browse = xbox_enable_browse;
                    let year_min = xbox_year_min;
                    let year_max = xbox_year_max;
                    let page_size = xbox_browse_page_size;
                    let max_pages = xbox_browse_max_pages;
                    let markets = xbox_browse_markets.clone();

                    provider_tasks.push(Box::new(move || {
                        info!("unified-ingest: starting Xbox provider run");
                        let start = Instant::now();

                        // Set environment variables for Xbox provider
                        if enable_browse {
                            std::env::set_var("XBOX_ENABLE_BROWSE", "true");
                        }
                        if let Some(min) = year_min {
                            std::env::set_var("XBOX_YEAR_MIN", min.to_string());
                        }
                        if let Some(max) = year_max {
                            std::env::set_var("XBOX_YEAR_MAX", max.to_string());
                        }
                        if let Some(size) = page_size {
                            std::env::set_var("XBOX_BROWSE_PAGE_SIZE", size.to_string());
                        }
                        if let Some(max) = max_pages {
                            std::env::set_var("XBOX_BROWSE_MAX_PAGES", max.to_string());
                        }
                        if let Some(ref mkts) = markets {
                            std::env::set_var("XBOX_BROWSE_MARKETS", mkts);
                        }

                        match run_with_retries_blocking("xbox", retries, || {
                            handle.block_on(async {
                                use i_miss_rust::database_ops::xbox::provider;
                                if let Some(prov) = &provider_opt {
                                    provider::run_with_provider(&db_clone, prov.clone()).await
                                } else {
                                    Err(anyhow::anyhow!("Xbox provider was not initialized successfully"))
                                }
                            })
                        }) {
                            Ok(_) => {
                                info!(
                                    elapsed_ms = start.elapsed().as_millis() as u64,
                                    "unified-ingest: Xbox provider completed"
                                );
                            }
                            Err(err) => {
                                error!(error = ?err, "unified-ingest: Xbox provider failed");
                            }
                        }
                    }));
                }

                let provider_task_count = provider_tasks.len();

                // Execute provider tasks in parallel using Rayon
                if provider_task_count > 0 {
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
                        provider_tasks.into_par_iter().for_each(|task| task());
                    });
                }

                info!(
                    iteration = provider_iteration,
                    provider_tasks = provider_task_count,
                    "unified-ingest: provider iteration finished"
                );

                let Some(interval) = loop_interval else {
                    break;
                };

                if provider_task_count == 0 {
                    warn!(
                        iteration = provider_iteration,
                        "unified-ingest: no providers enabled; exiting provider loop"
                    );
                    break;
                }

                if provider_iteration >= max_provider_loops {
                    info!(
                        iteration = provider_iteration,
                        max_loops = max_provider_loops,
                        "unified-ingest: reached max provider loop iterations"
                    );
                    break;
                }

                info!(
                    iteration = provider_iteration,
                    sleep_secs = interval.as_secs(),
                    "unified-ingest: sleeping before next provider iteration"
                );
                sleep(interval).await;
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

fn run_with_retries_blocking<T>(
    provider: &'static str,
    retries: u32,
    mut op: impl FnMut() -> Result<T>,
) -> Result<T> {
    let max_attempts = retries.saturating_add(1).max(1);

    for attempt in 1..=max_attempts {
        match op() {
            Ok(val) => {
                if attempt > 1 {
                    info!(
                        provider,
                        attempt, max_attempts, "provider succeeded after retries"
                    );
                }
                return Ok(val);
            }
            Err(err) => {
                if attempt >= max_attempts {
                    return Err(err);
                }

                // Exponential backoff, capped.
                let exp = (attempt - 1).min(6);
                let backoff = Duration::from_secs(2u64.saturating_pow(exp));
                warn!(
                    provider,
                    attempt,
                    max_attempts,
                    backoff_secs = backoff.as_secs(),
                    error = ?err,
                    "provider attempt failed; retrying"
                );
                std::thread::sleep(backoff);
            }
        }
    }

    // Unreachable (loop either returns Ok or Err)
    unreachable!("retry loop should always return");
}

fn build_nexarda_options_from_env() -> NexardaOptions {
    let products = std::env::var("NEXARDA_PRODUCTS")
        .ok()
        .and_then(|s| {
            if s.trim().is_empty() {
                return None;
            }
            match serde_json::from_str(&s) {
                Ok(value) => Some(value),
                Err(err) => {
                    warn!(error = %err, "NEXARDA_PRODUCTS was set but could not be parsed as JSON");
                    None
                }
            }
        })
        .or_else(|| {
            let path = std::env::var("NEXARDA_PRODUCTS_FILE").ok()?;
            if path.trim().is_empty() {
                return None;
            }

            let raw = match std::fs::read_to_string(&path) {
                Ok(raw) => raw,
                Err(err) => {
                    warn!(path = %path, error = %err, "Failed to read NEXARDA_PRODUCTS_FILE");
                    return None;
                }
            };

            match serde_json::from_str(&raw) {
                Ok(value) => Some(value),
                Err(err) => {
                    warn!(path = %path, error = %err, "NEXARDA_PRODUCTS_FILE contained invalid JSON");
                    None
                }
            }
        })
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
        "ES" => "Spain",
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
        "TW" => "Taiwan",
        "RU" => "Russia",
        "IN" => "India",
        "IT" => "Italy",
        "NL" => "Netherlands",
        "BE" => "Belgium",
        "AT" => "Austria",
        "FI" => "Finland",
        "IE" => "Ireland",
        "ZA" => "South Africa",
        "SA" => "Saudi Arabia",
        "AE" => "United Arab Emirates",
        "AZ" => "Azerbaijan",
        "TR" => "Turkey",
        "AR" => "Argentina",
        "CL" => "Chile",
        "EG" => "Egypt",
        "HE" => "Hungary",
        other => other,
    })
    .to_string()
}

fn redact_postgres_url(raw: &str) -> String {
    // Best-effort redaction for DSNs so we don't leak credentials into logs.
    // Preserve the host/port/db and query params (e.g. hostaddr) because they're useful for debugging.
    match url::Url::parse(raw.trim()) {
        Ok(mut u) => {
            let scheme = u.scheme().to_ascii_lowercase();
            if scheme == "postgres" || scheme == "postgresql" {
                let _ = u.set_username("***");
                let _ = u.set_password(Some("***"));
            }
            u.to_string()
        }
        Err(_) => {
            // Fallback: hide any userinfo portion.
            if raw.starts_with("postgres://") || raw.starts_with("postgresql://") {
                if let Some(proto) = raw.find("//") {
                    if let Some(at) = raw[proto + 2..].find('@') {
                        let host_part = &raw[proto + 2 + at + 1..];
                        return format!("{}***:{}", &raw[..proto + 2], host_part);
                    }
                }
                return "postgres://***".to_string();
            }

            raw.to_string()
        }
    }
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

    // Legacy/no-migrate tolerance: older DBs may not have the sellables/title linkage
    // used by this backfill. Treat missing schema as a no-op with a clear warning.
    let mut missing: Vec<String> = Vec::new();
    if !table_exists(db, "video_game_titles").await.unwrap_or(false) {
        missing.push("public.video_game_titles".to_string());
    }
    if !table_exists(db, "sellables").await.unwrap_or(false) {
        missing.push("public.sellables".to_string());
    }
    if missing.is_empty() {
        if !column_exists(db, "video_game_titles", "product_id")
            .await
            .unwrap_or(false)
        {
            missing.push("public.video_game_titles.product_id".to_string());
        }
        if !column_exists(db, "sellables", "software_title_id")
            .await
            .unwrap_or(false)
        {
            missing.push("public.sellables.software_title_id".to_string());
        }
        if !column_exists(db, "sellables", "kind")
            .await
            .unwrap_or(false)
        {
            missing.push("public.sellables.kind".to_string());
        }
    }
    if !missing.is_empty() {
        let compat = php_compat_schema(db).await.unwrap_or(false);
        warn!(missing = ?missing, php_compat = compat, "db-backfill-sellables: required schema missing; skipping backfill (no-op)");
        return Ok(stats);
    }
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

    // Legacy/no-migrate tolerance: this bootstrap currently targets the "modern" sellables/title schema.
    // If the required tables/columns are missing, do not hard-fail (so unified-ingest can still
    // run media/catalog steps); instead, warn and treat as a no-op.
    let mut missing: Vec<String> = Vec::new();
    for table in ["sellables", "video_game_titles"] {
        if !table_exists(db, table).await.unwrap_or(false) {
            missing.push(format!("public.{table}"));
        }
    }
    if missing.is_empty() {
        for (table, col) in [
            ("sellables", "kind"),
            ("sellables", "software_title_id"),
            ("video_game_titles", "product_id"),
        ] {
            if !column_exists(db, table, col).await.unwrap_or(false) {
                missing.push(format!("public.{table}.{col}"));
            }
        }
    }
    if !missing.is_empty() {
        let compat = php_compat_schema(db).await.unwrap_or(false);
        warn!(missing = ?missing, php_compat = compat, "db-bootstrap-offers: required schema missing; skipping bootstrap (no-op)");
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

fn is_undefined_table_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("42P01"),
        _ => false,
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

async fn derive_retailer_coverage_from_countries(
    db: &Db,
    _retailer_slug: &str,
) -> Result<Option<DerivedCoverage>> {
    // Fallback for legacy schemas: derive coverage from countries table.
    // We use a simple hardcoded country->currency mapping instead of joining.
    // This avoids schema assumptions and provides reasonable defaults.

    let rows =
        match sqlx::query("SELECT iso2 as code, name FROM countries WHERE iso2 IS NOT NULL ORDER BY iso2")
            .fetch_all(&db.pool)
            .await
        {
            Ok(rows) => rows,
            Err(e) if is_undefined_table_error(&e) => {
                warn!("derive_retailer_coverage_from_countries: countries table missing");
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

    if rows.is_empty() {
        return Ok(None);
    }

    // Simple country->currency mapping (ISO 4217)
    let country_to_currency = [
        ("US", "USD", "US Dollar", 2),
        ("GB", "GBP", "British Pound", 2),
        ("CA", "CAD", "Canadian Dollar", 2),
        ("AU", "AUD", "Australian Dollar", 2),
        ("JP", "JPY", "Japanese Yen", 0),
        ("DE", "EUR", "Euro", 2),
        ("FR", "EUR", "Euro", 2),
        ("IT", "EUR", "Euro", 2),
        ("ES", "EUR", "Euro", 2),
        ("NL", "EUR", "Euro", 2),
        ("BE", "EUR", "Euro", 2),
        ("AT", "EUR", "Euro", 2),
        ("CH", "CHF", "Swiss Franc", 2),
        ("SE", "SEK", "Swedish Krona", 2),
        ("NO", "NOK", "Norwegian Krone", 2),
        ("DK", "DKK", "Danish Krone", 2),
        ("BR", "BRL", "Brazilian Real", 2),
        ("MX", "MXN", "Mexican Peso", 2),
        ("CN", "CNY", "Chinese Yuan", 2),
        ("IN", "INR", "Indian Rupee", 2),
    ]
    .iter()
    .map(|&(c, cur, name, mu)| (c, (cur, name, mu)))
    .collect::<std::collections::HashMap<_, _>>();

    let mut specs: Vec<CoverageRow> = Vec::new();
    for row in rows {
        let country_code: String = row.try_get("code")?;
        let country_name: String = row.try_get("name")?;

        // Look up currency mapping; default to USD if not found
        let (currency_code, currency_name, minor_unit) = country_to_currency
            .get(country_code.as_str())
            .copied()
            .unwrap_or(("USD", "US Dollar", 2));

        specs.push(CoverageRow {
            country_code,
            country_name,
            currency_code: currency_code.to_string(),
            currency_name: currency_name.to_string(),
            minor_unit: minor_unit as i16,
        });
    }

    if specs.is_empty() {
        return Ok(None);
    }

    // Find base (prefer US, otherwise first entry)
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

async fn column_exists(db: &Db, table: &str, column: &str) -> Result<bool> {
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (\
            SELECT 1 FROM information_schema.columns\
            WHERE table_schema='public' AND table_name=$1 AND column_name=$2\
        )",
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_one(&db.pool)
    .await?;
    Ok(exists.0)
}

async fn derive_retailer_coverage(db: &Db, retailer_slug: &str) -> Result<Option<DerivedCoverage>> {
    // Legacy-safe: older schemas may not have the commerce/jurisdiction tables yet.
    // In that case, fall back to querying countries.iso2 for basic coverage.
    if !table_exists(db, "offer_jurisdictions")
        .await
        .unwrap_or(false)
    {
        warn!(
            retailer_slug = %retailer_slug,
            "derive_retailer_coverage: offer_jurisdictions missing; falling back to countries.iso2"
        );
        return derive_retailer_coverage_from_countries(db, retailer_slug).await;
    }

    let rows = match sqlx::query(
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
    .await
    {
        Ok(rows) => rows,
        Err(e) if is_undefined_table_error(&e) => {
            warn!(
                retailer_slug = %retailer_slug,
                "derive_retailer_coverage: required tables missing; skipping retailer coverage derivation"
            );
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    };

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

fn set_env_if_missing<T: ToString>(key: &str, value: T) {
    if std::env::var_os(key).is_none() {
        set_env_value(key, value);
    }
}
