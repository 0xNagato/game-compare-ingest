use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{env, str::FromStr};

#[derive(Debug, Clone, Default)]
pub struct DbCountsConfig {
    /// Optional override for the Postgres connection string.
    pub database_url: Option<String>,
    /// Force whether recent games should be displayed (defaults to env RECENT_GAMES).
    pub show_recent_games: Option<bool>,
    /// Override the recent games LIMIT (defaults to env RECENT_GAMES_LIMIT or 20).
    pub recent_games_limit: Option<i64>,
}

pub async fn run(cfg: DbCountsConfig) -> Result<()> {
    dotenv::dotenv().ok();
    let mut out = String::new();
    let db_url = if let Some(url) = cfg.database_url.clone() {
        url
    } else {
        env::var("SUPABASE_DB_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .or_else(|_| env::var("DB_URL"))?
    };
    eprintln!("db_counts connecting to: {}", db_url);
    let connect_options = PgConnectOptions::from_str(&db_url)?.statement_cache_capacity(0);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(connect_options)
        .await?;

    macro_rules! count {
        ($sql:expr) => {
            sqlx::query_scalar::<_, i64>($sql)
                .persistent(false)
                .fetch_one(&pool)
                .await?
        };
    }

    let titles = count!("SELECT count(*) FROM public.video_game_titles");
    let games = count!("SELECT count(*) FROM public.video_games");
    let products = count!("SELECT count(*) FROM public.products");
    let software_rows = count!("SELECT count(*) FROM public.software");
    let sellables_all = count!("SELECT count(*) FROM public.sellables");
    let sellables_kind_exists: bool =
        sqlx
            ::query_scalar::<_, i64>(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='sellables' AND column_name='kind'"
            )
            .persistent(false)
            .fetch_one(&pool).await
            .unwrap_or(0) > 0;
    let (sellables_sw, sellables_hw) = if sellables_kind_exists {
        let sw = count!("SELECT count(*) FROM public.sellables WHERE kind='software'");
        let hw = count!("SELECT count(*) FROM public.sellables WHERE kind='hardware'");
        (sw, hw)
    } else {
        (0, 0)
    };
    let rating_cols_exist: bool =
        sqlx
            ::query_scalar::<_, i64>(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='video_games' AND column_name='average_rating'"
            )
            .persistent(false)
            .fetch_one(&pool).await
            .unwrap_or(0) > 0;
    let games_with_rating = if rating_cols_exist {
        count!("SELECT count(*) FROM public.video_games WHERE average_rating IS NOT NULL")
    } else {
        0
    };
    let providers = count!("SELECT count(*) FROM public.providers");
    let provider_items = count!("SELECT count(*) FROM public.provider_items");
    let media_links = count!("SELECT count(*) FROM public.provider_media_links");
    let offers = count!("SELECT count(*) FROM public.offers");
    let offer_j = count!("SELECT count(*) FROM public.offer_jurisdictions");
    let prices_24h =
        count!("SELECT count(*) FROM public.prices WHERE recorded_at > now() - interval '1 day'");
    let current_prices = count!("SELECT count(*) FROM public.current_price");
    let guard_warns_24h = count!(
        "SELECT count(*) FROM public.prices WHERE recorded_at > now() - interval '1 day' AND (meta->'guard'->>'status') = 'warn_large_deviation'"
    );

    use std::fmt::Write as _;
    writeln!(out, "DB COUNTS SUMMARY:").ok();
    writeln!(out, "products: {products} (software rows: {software_rows})").ok();
    writeln!(out, "video_game_titles: {titles}").ok();
    println!("{}", out);
    if rating_cols_exist {
        writeln!(
            out,
            "video_games: {games} (with rating: {games_with_rating})"
        )
        .ok();
    } else {
        writeln!(out, "video_games: {games} (rating columns not present)").ok();
    }
    writeln!(out, "providers: {providers}").ok();
    println!(
        "sellables: {sellables_all}{}",
        if sellables_kind_exists {
            format!(" (software: {sellables_sw}, hardware: {sellables_hw})")
        } else {
            "".to_string()
        }
    );
    writeln!(out, "provider_items: {provider_items}").ok();
    writeln!(out, "provider_media_links: {media_links}").ok();
    writeln!(out, "offers: {offers}").ok();
    writeln!(out, "offer_jurisdictions: {offer_j}").ok();
    writeln!(out, "prices(last 24h): {prices_24h}").ok();
    writeln!(out, "current_price rows: {current_prices}").ok();
    writeln!(out, "guard warnings (last 24h): {guard_warns_24h}").ok();
    println!("{}", out);

    let retailer_rows = sqlx
        ::query(
            r#"
        WITH prices_by_offer AS (
            SELECT DISTINCT oj.offer_id
            FROM public.prices p
            JOIN public.offer_jurisdictions oj ON oj.id = p.offer_jurisdiction_id
        ),
        prices_by_oj AS (
            SELECT DISTINCT offer_jurisdiction_id
            FROM public.prices
        ),
        current_by_oj AS (
            SELECT DISTINCT offer_jurisdiction_id
            FROM public.current_price
        )
        SELECT
            r.id,
            r.name,
            COALESCE(r.slug, '') AS slug,
            COUNT(DISTINCT o.id) AS offers,
            COUNT(DISTINCT o.id) FILTER (WHERE po.offer_id IS NOT NULL) AS offers_with_price,
            COUNT(DISTINCT oj.id) AS offer_jurisdictions,
            COUNT(DISTINCT oj.id) FILTER (WHERE poj.offer_jurisdiction_id IS NOT NULL) AS jurisdictions_with_price,
            COUNT(DISTINCT oj.id) FILTER (WHERE cjo.offer_jurisdiction_id IS NOT NULL) AS jurisdictions_with_current
        FROM public.retailers r
        LEFT JOIN public.offers o ON o.retailer_id = r.id
        LEFT JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
        LEFT JOIN prices_by_offer po ON po.offer_id = o.id
        LEFT JOIN prices_by_oj poj ON poj.offer_jurisdiction_id = oj.id
        LEFT JOIN current_by_oj cjo ON cjo.offer_jurisdiction_id = oj.id
        GROUP BY r.id, r.name, r.slug
        ORDER BY r.name
        "#
        )
        .persistent(false)
        .fetch_all(&pool).await
        .unwrap_or_default();
    if !retailer_rows.is_empty() {
        writeln!(out, "retailer coverage summary:").ok();
        for row in retailer_rows {
            let retailer_id: i64 = row.get("id");
            let name: String = row.get("name");
            let slug: String = row.get("slug");
            let offers: i64 = row.get("offers");
            let offers_with_price: i64 = row.get("offers_with_price");
            let jurisdictions: i64 = row.get("offer_jurisdictions");
            let jurisdictions_with_price: i64 = row.get("jurisdictions_with_price");
            let jurisdictions_with_current: i64 = row.get("jurisdictions_with_current");

            let offer_pct = if offers > 0 {
                ((offers_with_price as f64) / (offers as f64)) * 100.0
            } else {
                0.0
            };
            let jurisdiction_pct = if jurisdictions > 0 {
                ((jurisdictions_with_price as f64) / (jurisdictions as f64)) * 100.0
            } else {
                0.0
            };
            let current_pct = if jurisdictions > 0 {
                ((jurisdictions_with_current as f64) / (jurisdictions as f64)) * 100.0
            } else {
                0.0
            };

            let slug_display = if slug.is_empty() { "-" } else { slug.as_str() };
            let mut flags: Vec<&str> = Vec::new();
            if offers > 0 && offers_with_price == 0 {
                flags.push("no price coverage");
            } else if offers_with_price < offers {
                flags.push("partial price coverage");
            }
            if jurisdictions_with_price > 0 && jurisdictions_with_current < jurisdictions_with_price
            {
                flags.push("current_price missing for some priced jurisdictions");
            }
            if jurisdictions == 0 {
                flags.push("no jurisdictions");
            }
            let note = if flags.is_empty() {
                String::new()
            } else {
                format!(" — {}", flags.join("; "))
            };

            writeln!(
                out,
                "  {name} #{retailer_id} ({slug_display}): offers {offers} (priced {offers_with_price}, {offer_pct:.1}%), jurisdictions {jurisdictions} (priced {jurisdictions_with_price}, {jurisdiction_pct:.1}%, current {jurisdictions_with_current}, {current_pct:.1}%){note}"
            ).ok();
        }
        println!("{}", out);
    }

    let retailer_gap_rows = sqlx::query(
        r#"
            WITH base AS (
                SELECT
                    r.id AS retailer_id,
                    r.name AS retailer_name,
                    COALESCE(r.slug, '') AS retailer_slug,
                    o.id AS offer_id,
                    oj.id AS offer_jurisdiction_id,
                    c.name AS country_name,
                    c.iso2 AS country_iso2,
                    j.region_code,
                    cur.code AS currency_code,
                    EXISTS (
                        SELECT 1 FROM public.prices p
                        WHERE p.offer_jurisdiction_id = oj.id
                    ) AS has_price,
                    EXISTS (
                        SELECT 1 FROM public.current_price cp
                        WHERE cp.offer_jurisdiction_id = oj.id
                    ) AS has_current,
                    (
                        SELECT MAX(p2.recorded_at)
                        FROM public.prices p2
                        WHERE p2.offer_jurisdiction_id = oj.id
                    ) AS last_price_at
                FROM public.retailers r
                JOIN public.offers o ON o.retailer_id = r.id
                JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
                JOIN public.jurisdictions j ON j.id = oj.jurisdiction_id
                JOIN public.countries c ON c.id = j.country_id
                JOIN public.currencies cur ON cur.id = oj.currency_id
            )
            SELECT *
            FROM (
                SELECT
                    b.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.retailer_id
                        ORDER BY
                            (CASE WHEN b.has_price THEN 1 ELSE 0 END),
                            (CASE WHEN b.has_current THEN 1 ELSE 0 END),
                            b.country_name,
                            b.region_code NULLS FIRST,
                            b.offer_id
                    ) AS rn
                FROM base b
                WHERE NOT b.has_price OR NOT b.has_current
            ) ranked
            WHERE rn <= 5
            ORDER BY retailer_name, country_name, region_code NULLS FIRST, offer_id
            "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !retailer_gap_rows.is_empty() {
        writeln!(out, "retailer jurisdiction gaps (first 5 per retailer):").ok();
        let mut current: Option<i64> = None;
        for row in retailer_gap_rows {
            let retailer_id: i64 = row.get("retailer_id");
            let retailer_name: String = row.get("retailer_name");
            let retailer_slug: String = row.get("retailer_slug");
            let offer_id: i64 = row.get("offer_id");
            let offer_jurisdiction_id: i64 = row.get("offer_jurisdiction_id");
            let country_name: String = row.get("country_name");
            let country_iso2: String = row.get("country_iso2");
            let region_code: Option<String> = row.try_get("region_code").ok();
            let currency_code: String = row.get("currency_code");
            let has_price: bool = row.get("has_price");
            let has_current: bool = row.get("has_current");
            let last_price_at: Option<DateTime<Utc>> = row.try_get("last_price_at").ok();

            if current != Some(retailer_id) {
                current = Some(retailer_id);
                let slug_display = if retailer_slug.is_empty() {
                    "-"
                } else {
                    retailer_slug.as_str()
                };
                writeln!(out, "  {retailer_name} #{retailer_id} ({slug_display}):").ok();
            }

            let mut region_label = String::new();
            if let Some(code) = region_code
                .as_ref()
                .and_then(|s| if s.is_empty() { None } else { Some(s) })
            {
                region_label = format!("-{code}");
            }
            let status = match (has_price, has_current) {
                (false, false) => "no prices ingested",
                (false, true) => "current_price exists but no prices",
                (true, false) => "missing current_price",
                (true, true) => "ok",
            };
            let last_seen = last_price_at
                .map(|ts| format!(", last price at {} UTC", ts.format("%Y-%m-%d %H:%M")))
                .unwrap_or_else(|| ".".to_string());
            writeln!(
                out,
                "    • offer #{offer_id}, offer_jurisdiction #{offer_jurisdiction_id} — {country_name} ({country_iso2}{region_label}) [{currency_code}] → {status}{last_seen}"
            ).ok();
        }
        println!("{}", out);
    }

    let total_game_media: i64 = count!("SELECT count(*) FROM public.game_media");
    let total_game_images: i64 =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.game_images")
            .persistent(false)
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
    let media_with_known_provider: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM public.game_media gm
        JOIN public.providers p ON p.slug = lower(gm.source::text)
        WHERE gm.source IS NOT NULL AND gm.source <> ''
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    let media_without_provider: i64 = total_game_media - media_with_known_provider;
    writeln!(out, "media coverage:").ok();
    writeln!(out, "  game_media rows: {total_game_media}").ok();
    writeln!(out, "  game_images rows: {total_game_images}").ok();
    writeln!(out, "  with matching provider: {media_with_known_provider}").ok();
    writeln!(out, "  without matching provider: {media_without_provider}").ok();
    println!("{}", out);

    let media_examples = sqlx
        ::query(
            r#"
        SELECT
            COALESCE(vgt_direct.title, vgt_offer.title, gc.model, 'provider item #' || pi.id::text) AS game_name,
            p.slug AS provider_slug,
            COALESCE(pml.media_type, 'unknown') AS media_type,
            COALESCE(NULLIF(pml.title, ''), gm.title, '') AS media_title,
            COALESCE(pml.url, gm.url, '') AS media_url
        FROM public.provider_media_links pml
        JOIN public.provider_items pi ON pi.id = pml.provider_item_id
        JOIN public.providers p ON p.id = pi.provider_id
        LEFT JOIN public.video_games vg_direct ON vg_direct.id = pml.video_game_id
        LEFT JOIN public.video_game_titles vgt_direct ON vgt_direct.id = vg_direct.title_id
        LEFT JOIN public.provider_offers pof ON pof.provider_item_id = pi.id
        LEFT JOIN public.offers o ON o.id = pof.offer_id
        LEFT JOIN public.sellables s ON s.id = o.sellable_id
        LEFT JOIN public.video_game_titles vgt_offer ON vgt_offer.id = s.software_title_id
        LEFT JOIN public.game_consoles gc ON gc.id = s.console_id
        LEFT JOIN public.game_media gm ON gm.id = pml.media_id
        ORDER BY pml.updated_at DESC
        LIMIT 5
        "#
        )
        .persistent(false)
        .fetch_all(&pool).await
        .unwrap_or_default();
    if !media_examples.is_empty() {
        writeln!(out, "media samples (latest 5 provider_media_links):").ok();
        for row in media_examples {
            let game_name: String = row.get("game_name");
            let provider_slug: String = row.get("provider_slug");
            let media_type: String = row.get("media_type");
            let media_title: String = row.get("media_title");
            let media_url: String = row.get("media_url");
            let title_suffix = if media_title.is_empty() {
                "".to_string()
            } else {
                format!(" — {media_title}")
            };
            let url_suffix = if media_url.is_empty() {
                "".to_string()
            } else {
                format!(" [{}]", media_url)
            };
            writeln!(
                out,
                "  {game_name} (provider: {provider_slug}, {media_type}){title_suffix}{url_suffix}"
            )
            .ok();
        }
        println!("{}", out);
    }

    let pml_breakdown = sqlx::query(
        r#"
        SELECT p.slug, COUNT(*) AS n
        FROM public.provider_media_links pml
        JOIN public.provider_items pi ON pi.id = pml.provider_item_id
        JOIN public.providers p ON p.id = pi.provider_id
        GROUP BY p.slug
        ORDER BY n DESC
        LIMIT 10
        "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !pml_breakdown.is_empty() {
        writeln!(out, "provider_media_links by provider (top 10):").ok();
        for r in pml_breakdown {
            let slug: String = r.get("slug");
            let n: i64 = r.get("n");
            writeln!(out, "  {slug}: {n}").ok();
        }
        println!("{}", out);
    }

    let media_gap_examples = sqlx
        ::query(
            r#"
        SELECT
            COALESCE(vgt.title, 'video game #' || COALESCE(gm.video_game_id::text, 'unknown')) AS game_name,
            gm.source::text AS source_slug,
            COALESCE(gm.url, '') AS media_url
        FROM public.game_media gm
        JOIN public.providers p ON p.slug = lower(gm.source::text)
        LEFT JOIN public.video_games vg ON vg.id = gm.video_game_id
        LEFT JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
        LEFT JOIN public.provider_items pi
          ON pi.provider_id = p.id
         AND pi.external_id = 'media:' || COALESCE(gm.external_id, gm.url)
        LEFT JOIN public.provider_media_links pml
          ON pml.provider_item_id = pi.id
         AND pml.url = gm.url
        WHERE gm.source IS NOT NULL AND gm.source <> '' AND pml.id IS NULL
        ORDER BY gm.updated_at DESC NULLS LAST
        LIMIT 5
        "#
        )
        .persistent(false)
        .fetch_all(&pool).await
        .unwrap_or_default();
    if !media_gap_examples.is_empty() {
        writeln!(
            out,
            "media gaps (provider mapped but missing pml, limit 5):"
        )
        .ok();
        for row in media_gap_examples {
            let game_name: String = row.get("game_name");
            let source_slug: String = row.get("source_slug");
            let media_url: String = row.get("media_url");
            let url_suffix = if media_url.is_empty() {
                "".to_string()
            } else {
                format!(" [{}]", media_url)
            };
            writeln!(out, "  {game_name} — source {source_slug}{url_suffix}").ok();
        }
        println!("{}", out);
    }

    let unknown_sources = sqlx::query(
        r#"
        SELECT lower(gm.source::text) AS src, COUNT(*) AS n
        FROM public.game_media gm
        LEFT JOIN public.providers p ON p.slug = lower(gm.source::text)
        WHERE gm.source IS NOT NULL AND gm.source <> '' AND p.id IS NULL
        GROUP BY lower(gm.source::text)
        ORDER BY n DESC
        LIMIT 10
        "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !unknown_sources.is_empty() {
        writeln!(out, "unknown media sources (no provider match, top 10):").ok();
        for r in unknown_sources {
            let src: String = r.get("src");
            let n: i64 = r.get("n");
            writeln!(out, "  {src}: {n}").ok();
        }
        println!("{}", out);
    }

    let ps_view_exists: bool =
        sqlx
            ::query_scalar::<_, i64>(
                "SELECT count(*) FROM information_schema.views WHERE table_schema='public' AND table_name='ps_price_integrity'"
            )
            .persistent(false)
            .fetch_one(&pool).await
            .unwrap_or(0) > 0;
    if ps_view_exists {
        let total_ps_prices: i64 =
            sqlx::query_scalar("SELECT count(*) FROM public.ps_price_integrity")
                .persistent(false)
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
        let neg_discounts: i64 = sqlx
            ::query_scalar(
                "SELECT count(*) FROM public.ps_price_integrity WHERE kind='discount' AND COALESCE(discount_negative_flag,false)"
            )
            .persistent(false)
            .fetch_one(&pool).await
            .unwrap_or(0);
        let no_base_found: i64 = sqlx
            ::query_scalar(
                "SELECT count(*) FROM public.ps_price_integrity WHERE kind='discount' AND latest_base_minor IS NULL"
            )
            .persistent(false)
            .fetch_one(&pool).await
            .unwrap_or(0);
        writeln!(
            out,
            "ps_price_integrity: total_ps_prices={total_ps_prices}, negative_discounts={neg_discounts}, discounts_without_base={no_base_found}"
        ).ok();
        println!("{}", out);
    } else {
        writeln!(
            out,
            "ps_price_integrity view not found (skipping PS checks)"
        )
        .ok();
    }

    let rows = sqlx::query(
        r#"
        SELECT pi.external_item_id, p.amount_minor, p.recorded_at
        FROM public.prices p
        JOIN public.offer_jurisdictions oj ON oj.id = p.offer_jurisdiction_id
        JOIN public.offers o ON o.id = oj.offer_id
        JOIN public.provider_offers pof ON pof.offer_id = o.id
        JOIN public.provider_items pi ON pi.id = pof.provider_item_id
        JOIN public.providers pr ON pr.id = pi.provider_id
        WHERE pr.name = 'steam' AND p.recorded_at > now() - interval '1 day'
        ORDER BY p.recorded_at DESC
        LIMIT 5
    "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !rows.is_empty() {
        writeln!(out, "recent steam prices (last 24h):").ok();
        for r in rows {
            let appid: String = r.get("external_item_id");
            let amount_minor: i64 = r.get("amount_minor");
            let recorded_at: String = r
                .get::<chrono::DateTime<chrono::Utc>, _>("recorded_at")
                .to_rfc3339();
            writeln!(out, "  {recorded_at} steam app:{appid} -> {amount_minor}").ok();
        }
        println!("{}", out);
    }

    let want_recent_games = cfg.show_recent_games.unwrap_or_else(|| {
        env::var("RECENT_GAMES")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false)
    });
    if want_recent_games {
        let limit: i64 = cfg.recent_games_limit.unwrap_or_else(|| {
            env::var("RECENT_GAMES_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(20)
        });
        let recent = sqlx::query(
            r#"
            SELECT vgt.title, p.name as platform, vg.edition, vg.release_date, vg.created_at
            FROM public.video_games vg
            JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
            JOIN public.platforms p ON p.id = vg.platform_id
            ORDER BY vg.release_date DESC NULLS LAST, vg.created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .persistent(false)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        writeln!(out, "recent games (by release_date desc, limit {limit}):").ok();
        for r in recent {
            let title: String = r.get("title");
            let platform: String = r.get("platform");
            let edition: Option<String> = r.try_get("edition").ok();
            let release_date: Option<chrono::NaiveDate> = r.try_get("release_date").ok();
            writeln!(
                out,
                "  {} [{}]{} — {}",
                title,
                platform,
                edition
                    .as_ref()
                    .map(|e| format!(" ({e})"))
                    .unwrap_or_default(),
                release_date
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "no-date".to_string())
            )
            .ok();
        }
        println!("{}", out);
    }

    let products_without_titles: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM public.products p
        LEFT JOIN public.software s ON s.video_game_id = p.id
        LEFT JOIN public.video_game_titles vgt ON vgt.video_game_id = p.id
        WHERE s.product_id IS NOT NULL AND vgt.id IS NULL
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    let titles_without_sellables: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM public.video_game_titles vgt
        LEFT JOIN public.sellables s ON s.software_title_id = vgt.id
        WHERE s.id IS NULL
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    writeln!(out, "linkage diagnostics:").ok();
    writeln!(
        out,
        "  products with software but without titles: {products_without_titles}"
    )
    .ok();
    writeln!(
        out,
        "  titles without sellables: {titles_without_sellables}"
    )
    .ok();
    println!("{}", out);

    let duplicates_products_slug: i64 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(dup_count - 1),0) FROM (
          SELECT COUNT(*) AS dup_count FROM public.products GROUP BY slug HAVING COUNT(*) > 1
        ) t
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    let duplicates_titles_per_product: i64 = sqlx
        ::query_scalar(
            r#"
        SELECT COALESCE(SUM(dup_count - 1),0) FROM (
          SELECT COUNT(*) AS dup_count FROM public.video_game_titles GROUP BY video_game_id HAVING COUNT(*) > 1
        ) t
        "#
        )
        .persistent(false)
        .fetch_one(&pool).await
        .unwrap_or(0);
    let duplicates_games_combo: i64 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(dup_count - 1),0) FROM (
          SELECT COUNT(*) AS dup_count
          FROM public.video_games
          GROUP BY title_id, platform_id, COALESCE(edition,'')
          HAVING COUNT(*) > 1
        ) t
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    writeln!(out, "uniqueness checks:").ok();
    writeln!(
        out,
        "  duplicate products by slug (extra rows): {duplicates_products_slug}"
    )
    .ok();
    writeln!(
        out,
        "  duplicate titles per product (extra rows): {duplicates_titles_per_product}"
    )
    .ok();
    writeln!(
        out,
        "  duplicate games by (title, platform, edition) (extra rows): {duplicates_games_combo}"
    )
    .ok();
    println!("{}", out);

    let sellable_indexes = sqlx::query(
        r#"
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname='public' AND tablename='sellables'
        ORDER BY indexname
        "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !sellable_indexes.is_empty() {
        writeln!(out, "sellables indexes:").ok();
        for r in &sellable_indexes {
            let name: String = r.get("indexname");
            let def: String = r.get("indexdef");
            writeln!(out, "  {name}: {def}").ok();
        }
        let have_sw = sellable_indexes.iter().any(|r| {
            let def: String = r.get("indexdef");
            def.contains("software_title_id") && def.contains("UNIQUE")
        });
        let have_hw = sellable_indexes.iter().any(|r| {
            let def: String = r.get("indexdef");
            def.contains("console_id") && def.contains("UNIQUE")
        });
        writeln!(
            out,
            "  hardened software_title UNIQUE present: {}",
            if have_sw { "yes" } else { "no" }
        )
        .ok();
        writeln!(
            out,
            "  hardened console UNIQUE present: {}",
            if have_hw { "yes" } else { "no" }
        )
        .ok();
        println!("{}", out);
    }

    let core_tables = [
        "products",
        "video_game_titles",
        "video_games",
        "providers",
        "offers",
        "offer_jurisdictions",
        "prices",
    ];
    for tbl in core_tables.iter() {
        let idx_rows = sqlx
            ::query(
                r#"SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=$1 ORDER BY indexname"#
            )
            .bind(tbl)
            .persistent(false)
            .fetch_all(&pool).await
            .unwrap_or_default();
        if !idx_rows.is_empty() {
            writeln!(out, "indexes ({tbl}):").ok();
            for r in idx_rows {
                let name: String = r.get("indexname");
                let def: String = r.get("indexdef");
                writeln!(out, "  {name}: {def}").ok();
            }
        }
    }
    println!("{}", out);

    let platform_counts = sqlx::query(
        r#"
        SELECT p.name, COUNT(vg.id) AS games
        FROM public.platforms p
        LEFT JOIN public.video_games vg ON vg.platform_id = p.id
        GROUP BY p.name
        ORDER BY games DESC, p.name ASC
        LIMIT 20
        "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !platform_counts.is_empty() {
        let total_games = games;
        let unknown_games: i64 = sqlx
            ::query_scalar(
                "SELECT COUNT(*) FROM public.video_games vg JOIN public.platforms p ON p.id=vg.platform_id WHERE p.name='unknown'"
            )
            .persistent(false)
            .fetch_one(&pool).await
            .unwrap_or(0);
        writeln!(out, "platform distribution (top 20):").ok();
        for r in &platform_counts {
            let name: String = r.get("name");
            let g: i64 = r.get("games");
            writeln!(out, "  {name}: {g}").ok();
        }
        let unknown_pct = if total_games > 0 {
            ((unknown_games as f64) / (total_games as f64)) * 100.0
        } else {
            0.0
        };
        writeln!(
            out,
            "unknown platform games: {unknown_games}/{total_games} ({unknown_pct:.2}%)"
        )
        .ok();
        println!("{}", out);
    }

    let pml_gaps: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM public.game_media gm
        JOIN public.providers p ON p.slug = lower(gm.source::text)
        LEFT JOIN public.provider_items pi
          ON pi.provider_id = p.id
         AND pi.external_id = 'media:' || COALESCE(gm.external_id, gm.url)
        LEFT JOIN public.provider_media_links pml
          ON pml.provider_item_id = pi.id
         AND pml.url = gm.url
        WHERE gm.source IS NOT NULL AND gm.source <> '' AND pml.id IS NULL
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    writeln!(
        out,
        "provider_media_link gaps (gm with provider but no pml): {pml_gaps}"
    )
    .ok();

    let total_offers: i64 = count!("SELECT count(*) FROM public.offers");
    let offers_with_prices: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT o.id)
        FROM public.offers o
        JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
        JOIN public.prices p ON p.offer_jurisdiction_id = oj.id
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    let total_provider_items: i64 = count!("SELECT count(*) FROM public.provider_items");
    let provider_items_with_prices: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT pi.id)
        FROM public.provider_items pi
        JOIN public.provider_offers pof ON pof.provider_item_id = pi.id
        JOIN public.offers o ON o.id = pof.offer_id
        JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
        JOIN public.prices p ON p.offer_jurisdiction_id = oj.id
        "#,
    )
    .persistent(false)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    writeln!(out, "price coverage:").ok();
    writeln!(
        out,
        "  offers with ≥1 price: {offers_with_prices}/{total_offers}"
    )
    .ok();
    writeln!(
        out,
        "  provider_items with ≥1 price: {provider_items_with_prices}/{total_provider_items}"
    )
    .ok();
    println!("{}", out);

    let price_gap_examples = sqlx::query(
        r#"
        SELECT
            COALESCE(vgt_sw.title, gc.model, 'Sellable #' || s.id::text) AS game_name,
            r.name AS retailer_name,
            COALESCE(c.iso2, '--') AS country_code,
            COALESCE(curr.code, '--') AS currency_code
        FROM public.offer_jurisdictions oj
        JOIN public.offers o ON o.id = oj.offer_id
        JOIN public.retailers r ON r.id = o.retailer_id
        JOIN public.sellables s ON s.id = o.sellable_id
        LEFT JOIN public.video_game_titles vgt_sw ON vgt_sw.id = s.software_title_id
        LEFT JOIN public.game_consoles gc ON gc.id = s.console_id
        LEFT JOIN public.jurisdictions jur ON jur.id = oj.jurisdiction_id
        LEFT JOIN public.countries c ON c.id = jur.country_id
        LEFT JOIN public.currencies curr ON curr.id = oj.currency_id
        WHERE NOT EXISTS (
            SELECT 1 FROM public.prices p WHERE p.offer_jurisdiction_id = oj.id
        )
        ORDER BY o.created_at DESC, game_name ASC
        LIMIT 5
        "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !price_gap_examples.is_empty() {
        writeln!(
            out,
            "  sample offer jurisdictions without prices (limit 5):"
        )
        .ok();
        for row in price_gap_examples {
            let game_name: String = row.get("game_name");
            let retailer_name: String = row.get("retailer_name");
            let country_code: String = row.get("country_code");
            let currency_code: String = row.get("currency_code");
            writeln!(
                out,
                "    {game_name} — {retailer_name} [{country_code}/{currency_code}] (no price rows)"
            )
            .ok();
        }
        println!("{}", out);
    }

    let provider_item_gaps = sqlx::query(
        r#"
        SELECT
            COALESCE(vgt_sw.title, gc.model, 'Sellable #' || s.id::text) AS game_name,
            pr.slug AS provider_slug,
            COALESCE(NULLIF(pi.external_id, ''), 'no-external-id') AS external_id
        FROM public.provider_items pi
        JOIN public.providers pr ON pr.id = pi.provider_id
        JOIN public.provider_offers pof ON pof.provider_item_id = pi.id
        JOIN public.offers o ON o.id = pof.offer_id
        JOIN public.sellables s ON s.id = o.sellable_id
        LEFT JOIN public.video_game_titles vgt_sw ON vgt_sw.id = s.software_title_id
        LEFT JOIN public.game_consoles gc ON gc.id = s.console_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.offer_jurisdictions oj
            JOIN public.prices p ON p.offer_jurisdiction_id = oj.id
            WHERE oj.offer_id = o.id
        )
        ORDER BY pi.created_at DESC
        LIMIT 5
        "#,
    )
    .persistent(false)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    if !provider_item_gaps.is_empty() {
        writeln!(
            out,
            "  sample provider items without priced offers (limit 5):"
        )
        .ok();
        for row in provider_item_gaps {
            let game_name: String = row.get("game_name");
            let provider_slug: String = row.get("provider_slug");
            let external_id: String = row.get("external_id");
            writeln!(
                out,
                "    {game_name} — provider {provider_slug} (external: {external_id})"
            )
            .ok();
        }
        println!("{}", out);
    }

    let _ = std::fs::write("db_counts.out", &out);

    Ok(())
}
