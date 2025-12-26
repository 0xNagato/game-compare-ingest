use std::{
    collections::HashSet,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use i_miss_rust::database_ops::{
    alerts::evaluate_alerts,
    db::Db,
    giantbomb::ingest::ingest_from_file as gb_ingest_from_file,
    nexarda::provider::{
        NexardaOptions, NexardaProvider, Product, RegionDefinition, StoreConfig, StoreOverride,
    },
    rawg,
    steam::provider::SteamProvider,
};
use i_miss_rust::{
    database_ops::{igdb::client as igdb_client, xbox::provider as xbox_provider},
    psstore_seed_pipeline,
    util::env as env_util,
};
use serde::de::DeserializeOwned;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "ingest", version, about = "Unified provider ingestion CLI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run one or more provider pipelines (default when no command supplied)
    Run(RunArgs),
    /// List available provider identifiers
    Providers,
}

#[derive(Debug, Default, Args)]
struct RunArgs {
    /// Restrict execution to the specified providers (comma-separated)
    #[arg(long, value_enum, value_delimiter = ',')]
    only: Vec<ProviderKey>,
    /// Skip the specified providers (comma-separated)
    #[arg(long, value_enum, value_delimiter = ',')]
    skip: Vec<ProviderKey>,
    /// Disable post-run alert evaluation
    #[arg(long = "no-alerts", action = ArgAction::SetTrue)]
    no_alerts: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, ValueEnum)]
enum ProviderKey {
    Nexarda,
    Giantbomb,
    Playstation,
    Steam,
    Xbox,
    Igdb,
    Rawg,
}

impl ProviderKey {
    fn label(self) -> &'static str {
        match self {
            ProviderKey::Nexarda => "nexarda",
            ProviderKey::Giantbomb => "giantbomb",
            ProviderKey::Playstation => "playstation",
            ProviderKey::Steam => "steam",
            ProviderKey::Xbox => "xbox",
            ProviderKey::Igdb => "igdb",
            ProviderKey::Rawg => "rawg",
        }
    }

    fn display(self) -> &'static str {
        match self {
            ProviderKey::Nexarda => "Nexarda catalogue",
            ProviderKey::Giantbomb => "GiantBomb JSON",
            ProviderKey::Playstation => "PlayStation Store",
            ProviderKey::Steam => "Steam",
            ProviderKey::Xbox => "Xbox Display Catalog",
            ProviderKey::Igdb => "IGDB",
            ProviderKey::Rawg => "RAWG",
        }
    }
}

#[derive(Debug)]
struct ProviderExecution {
    offer_jurisdiction_ids: Vec<i64>,
    note: Option<String>,
}

#[derive(Debug)]
enum ProviderError {
    Skip(String),
    Fail(String),
}

#[derive(Debug)]
struct ProviderReport {
    provider: ProviderKey,
    duration: Duration,
    outcome: ProviderOutcome,
    offer_jurisdiction_ids: Vec<i64>,
}

#[derive(Debug)]
enum ProviderOutcome {
    Completed { note: Option<String> },
    Skipped { reason: String },
    Failed { error: String },
}

impl ProviderOutcome {
    fn is_success(&self) -> bool {
        matches!(self, ProviderOutcome::Completed { .. })
    }

    fn status_label(&self) -> &'static str {
        match self {
            ProviderOutcome::Completed { .. } => "ok",
            ProviderOutcome::Skipped { .. } => "skipped",
            ProviderOutcome::Failed { .. } => "failed",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ingest");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let run_args = match cli.command.unwrap_or(Command::Run(RunArgs::default())) {
        Command::Run(args) => args,
        Command::Providers => {
            list_providers();
            return Ok(());
        }
    };

    let database_url =
        env_util::db_url_prefer_session().context("no database URL configured for ingest CLI")?;
    let max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&database_url, max_conns).await?;
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;

    let providers = determine_providers(&run_args);
    if providers.is_empty() {
        info!("no providers selected; nothing to do");
        return Ok(());
    }

    info!(providers = %providers.iter().map(|p| p.label()).collect::<Vec<_>>().join(","), "ingest run start");
    let global_start = Instant::now();
    let reports = run_providers(&db, &providers).await;

    let mut touched: HashSet<i64> = HashSet::new();
    for rep in &reports {
        if rep.outcome.is_success() {
            touched.extend(rep.offer_jurisdiction_ids.iter().copied());
        }
    }

    if !run_args.no_alerts && !touched.is_empty() {
        let ids: Vec<i64> = touched.into_iter().collect();
        match evaluate_alerts(&db, &ids).await {
            Ok(alerts) if alerts.is_empty() => {
                info!("no alerts triggered in this run");
            }
            Ok(alerts) => {
                info!(count = alerts.len(), "alerts triggered during ingest run");
            }
            Err(err) => {
                warn!(error = %err, "alert evaluation failed");
            }
        }
    }

    for rep in &reports {
        let base = rep.provider.display();
        match &rep.outcome {
            ProviderOutcome::Completed { note } => {
                info!(
                    provider = rep.provider.label(),
                    status = rep.outcome.status_label(),
                    elapsed_ms = rep.duration.as_millis() as u64,
                    note = note.as_deref().unwrap_or(""),
                    touched = rep.offer_jurisdiction_ids.len(),
                    "{base} finished"
                );
            }
            ProviderOutcome::Skipped { reason } => {
                warn!(
                    provider = rep.provider.label(),
                    status = rep.outcome.status_label(),
                    elapsed_ms = rep.duration.as_millis() as u64,
                    reason = %reason,
                    "{base} skipped"
                );
            }
            ProviderOutcome::Failed { error } => {
                error!(
                    provider = rep.provider.label(),
                    status = rep.outcome.status_label(),
                    elapsed_ms = rep.duration.as_millis() as u64,
                    error = %error,
                    "{base} failed"
                );
            }
        }
    }

    info!(
        total_ms = global_start.elapsed().as_millis() as u64,
        "ingest run complete"
    );
    Ok(())
}

fn list_providers() {
    println!("Available providers:");
    for (idx, key) in DEFAULT_ORDER.iter().enumerate() {
        println!("  {:>2}. {:<12} ({})", idx + 1, key.label(), key.display());
    }
}

async fn run_providers(db: &Db, providers: &[ProviderKey]) -> Vec<ProviderReport> {
    let mut reports = Vec::with_capacity(providers.len());
    for provider in providers.iter().copied() {
        let start = Instant::now();
        let (outcome, offer_jurisdiction_ids) = match provider {
            ProviderKey::Nexarda => run_nexarda(db).await,
            ProviderKey::Giantbomb => run_giantbomb(db).await,
            ProviderKey::Playstation => run_playstation(db).await,
            ProviderKey::Steam => run_steam(db).await,
            ProviderKey::Xbox => run_xbox(db).await,
            ProviderKey::Igdb => run_igdb(db).await,
            ProviderKey::Rawg => run_rawg(db).await,
        };
        reports.push(ProviderReport {
            provider,
            duration: start.elapsed(),
            outcome,
            offer_jurisdiction_ids,
        });
    }
    reports
}

fn determine_providers(args: &RunArgs) -> Vec<ProviderKey> {
    let mut base: Vec<ProviderKey> = DEFAULT_ORDER.to_vec();
    if !args.only.is_empty() {
        let only: HashSet<ProviderKey> = args.only.iter().copied().collect();
        base.retain(|p| only.contains(p));
    }
    if !args.skip.is_empty() {
        let skip: HashSet<ProviderKey> = args.skip.iter().copied().collect();
        base.retain(|p| !skip.contains(p));
    }
    dedup_preserve(&mut base);
    base
}

fn dedup_preserve(keys: &mut Vec<ProviderKey>) {
    let mut seen: HashSet<ProviderKey> = HashSet::new();
    keys.retain(|k| seen.insert(*k));
}

const DEFAULT_ORDER: &[ProviderKey] = &[
    ProviderKey::Nexarda,
    ProviderKey::Giantbomb,
    ProviderKey::Playstation,
    ProviderKey::Steam,
    ProviderKey::Xbox,
    ProviderKey::Igdb,
    ProviderKey::Rawg,
];

async fn run_nexarda(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    let provider = match NexardaProvider::new(
        std::env::var("NEXARDA_BASE_URL").ok().as_deref(),
        std::env::var("NEXARDA_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok()),
    ) {
        Ok(p) => p,
        Err(err) => {
            return (
                ProviderOutcome::Failed {
                    error: err.to_string(),
                },
                vec![],
            );
        }
    };

    let opts = build_nexarda_options();
    match provider.ingest_to_db(db, opts).await {
        Ok(count) => (
            ProviderOutcome::Completed {
                note: Some(format!("ingested {count} catalogue entries")),
            },
            vec![],
        ),
        Err(err) => classify_error(err),
    }
}

async fn run_giantbomb(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    let enabled = std::env::var("GB_ENABLED")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(true);
    if !enabled {
        return (
            ProviderOutcome::Skipped {
                reason: "GB_ENABLED disabled".into(),
            },
            vec![],
        );
    }
    let path = std::env::var("GIANT_BOMB_FILE")
        .unwrap_or_else(|_| "keep/giant_bomb_games_detailed.json".into());
    if !Path::new(&path).exists() {
        return (
            ProviderOutcome::Skipped {
                reason: format!("file not found: {path}"),
            },
            vec![],
        );
    }
    let limit = std::env::var("GB_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    match gb_ingest_from_file(db, &path, limit).await {
        Ok(count) => (
            ProviderOutcome::Completed {
                note: Some(format!("processed {count} titles")),
            },
            vec![],
        ),
        Err(err) => classify_error(err),
    }
}

async fn run_playstation(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    match psstore_seed_pipeline(db).await {
        Ok(summary) => {
            let touched: Vec<i64> = summary.offer_jurisdiction_ids.iter().copied().collect();
            let note = format!(
                "psstore seed pipeline wrote {} price rows ({} current updates)",
                summary.total_price_rows_written, summary.total_current_updates
            );
            (ProviderOutcome::Completed { note: Some(note) }, touched)
        }
        Err(err) => classify_error(err),
    }
}

async fn run_steam(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    match SteamProvider::run_from_env(db).await {
        Ok(_) => (
            ProviderOutcome::Completed {
                note: Some("steam ingest completed".into()),
            },
            vec![],
        ),
        Err(err) => classify_error(err),
    }
}

async fn run_xbox(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    match xbox_provider::run_from_env(db).await {
        Ok(_) => (
            ProviderOutcome::Completed {
                note: Some("xbox ingest completed".into()),
            },
            vec![],
        ),
        Err(err) => classify_error(err),
    }
}

async fn run_igdb(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    match igdb_client::run_from_env(db).await {
        Ok(_) => (
            ProviderOutcome::Completed {
                note: Some("igdb ingest completed".into()),
            },
            vec![],
        ),
        Err(err) => classify_error(err),
    }
}

async fn run_rawg(db: &Db) -> (ProviderOutcome, Vec<i64>) {
    let api_key = std::env::var("RAWG_API_KEY").ok();
    match rawg::sync(db, api_key).await {
        Ok(_) => (
            ProviderOutcome::Completed {
                note: Some("rawg sync completed".into()),
            },
            vec![],
        ),
        Err(err) => classify_error(err),
    }
}

fn build_nexarda_options() -> NexardaOptions {
    let products: Vec<Product> = parse_env_json("NEXARDA_PRODUCTS");
    let store_map: std::collections::HashMap<
        String,
        std::collections::HashMap<String, StoreConfig>,
    > = parse_env_json("NEXARDA_STORE_MAP");
    let default_regions: Vec<RegionDefinition> = parse_env_json("NEXARDA_DEFAULT_REGIONS");
    let overrides: std::collections::HashMap<
        String,
        std::collections::HashMap<String, StoreOverride>,
    > = parse_env_json("NEXARDA_STORE_OVERRIDES");

    NexardaOptions {
        products,
        store_map,
        api_key: std::env::var("NEXARDA_API_KEY").ok(),
        auto_register_stores: Some(true),
        default_regions,
        dynamic_store_overrides: overrides,
        default_tax_inclusive: Some(true),
        context: None,
        base_url: None,
        timeout: None,
    }
}

fn parse_env_json<T>(key: &str) -> T
where
    T: Default + DeserializeOwned,
{
    std::env::var(key)
        .ok()
        .and_then(|raw| serde_json::from_str::<T>(&raw).ok())
        .unwrap_or_default()
}

fn classify_error(err: anyhow::Error) -> (ProviderOutcome, Vec<i64>) {
    let msg = err.to_string();
    if msg.to_ascii_lowercase().contains("missing env")
        || msg.to_ascii_lowercase().contains("missing environment")
    {
        (ProviderOutcome::Skipped { reason: msg }, vec![])
    } else {
        (ProviderOutcome::Failed { error: msg }, vec![])
    }
}
