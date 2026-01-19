use anyhow::Result;
use serde_json::Value;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    Row,
};
use std::{fs, str::FromStr};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("gb_ingest_debug");
    dotenv::dotenv().ok();

    let db_url = std::env::var("SUPABASE_DB_URL").or_else(|_| std::env::var("DATABASE_URL"))?;
    let path = std::env::var("GIANT_BOMB_FILE")
        .unwrap_or_else(|_| "keep/giant_bomb_games_detailed.json".into());
    let limit = std::env::var("GB_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5000);

    println!("🔗 Connecting to database (no prepared statements)...");
    let mut connect_options = PgConnectOptions::from_str(&db_url)?.statement_cache_capacity(0); // PgBouncer safe!

    // Ensure TLS is enabled when DSN contains sslmode=require
    if db_url.contains("sslmode=require") && !db_url.contains("sslmode=disable") {
        connect_options = connect_options.ssl_mode(PgSslMode::Require);
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected successfully");

    // Test basic query (no prepared statements + no persistence)
    let row = sqlx::query("SELECT version()")
        .persistent(false)
        .fetch_one(&pool)
        .await?;
    let version: String = row.get(0);
    println!(
        "📊 PostgreSQL: {}",
        version
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
    );

    // Check schemas (no prepared statements + no persistence)
    let row = sqlx::query(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'gamecompare'",
    )
    .persistent(false)
    .fetch_one(&pool)
    .await?;
    let schema_count: i64 = row.get(0);
    println!("📋 Tables in gamecompare schema: {}", schema_count);

    if schema_count == 0 {
        println!("⚠️  No tables found in gamecompare schema!");
        return Ok(());
    }

    println!("📖 Reading JSON file: {}", path);
    let raw = fs::read_to_string(&path)?;
    let v: Value = serde_json::from_str(&raw)?;

    println!("🎮 Starting ingest (limit: {})...", limit);

    let mut count = 0;

    match v {
        Value::Object(map) => {
            println!("📦 Found object with {} keys", map.len());

            for (idx, (guid, val)) in map.into_iter().enumerate() {
                if count >= limit {
                    break;
                }

                if let Value::Object(obj) = val {
                    let payload = Value::Object(obj.clone());
                    let name = obj.get("name").and_then(|n| n.as_str()).unwrap_or(&guid);

                    if name.trim().is_empty() {
                        println!("⏭️  Skipping entry {} (empty name)", idx);
                        continue;
                    }

                    println!("\n🔧 Processing #{}: {}", idx + 1, name);

                    // Start explicit transaction
                    let mut tx = pool.begin().await?;

                    // 1. Ensure provider
                    println!("  📌 Ensuring provider...");
                    let provider_id: i64 = match sqlx::query(
                        "SELECT id FROM providers WHERE name = $1",
                    )
                    .bind("gb")
                    .fetch_optional(&mut *tx)
                    .await?
                    {
                        Some(row) => {
                            let id: i64 = row.get("id");
                            println!("    ✓ Provider exists: {}", id);
                            id
                        }
                        None => {
                            let row = sqlx
                                ::query(
                                    "INSERT INTO providers (name, kind, slug) VALUES ($1, $2, $3) RETURNING id"
                                )
                                .bind("gb")
                                .bind("catalog")
                                .bind("gb")
                                .fetch_one(&mut *tx).await?;
                            let id: i64 = row.get("id");
                            println!("    ✓ Provider created: {}", id);
                            id
                        }
                    };

                    // 2. Ensure platform
                    println!("  📌 Ensuring platform...");
                    let platform_id: i64 =
                        match sqlx::query("SELECT id FROM platforms WHERE name = $1")
                            .bind("PC")
                            .fetch_optional(&mut *tx)
                            .await?
                        {
                            Some(row) => {
                                let id: i64 = row.get("id");
                                println!("    ✓ Platform exists: {}", id);
                                id
                            }
                            None => {
                                let row = sqlx::query(
                                "INSERT INTO platforms (name, slug) VALUES ($1, $2) RETURNING id",
                            )
                            .bind("PC")
                            .bind("pc")
                            .fetch_one(&mut *tx)
                            .await?;
                                let id: i64 = row.get("id");
                                println!("    ✓ Platform created: {}", id);
                                id
                            }
                        };

                    // 3. Ensure product
                    println!("  📌 Ensuring product...");
                    let slug = name.to_lowercase().replace(" ", "-");
                    let row = sqlx
                        ::query(
                            "INSERT INTO products (slug, name, platform, category) VALUES ($1, $2, $3, $4) \
                             ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, updated_at=now() RETURNING id"
                        )
                        .bind(&slug)
                        .bind(name)
                        .bind("unknown")
                        .bind("software")
                        .fetch_one(&mut *tx).await?;
                    let product_id: i64 = row.get("id");
                    println!("    ✓ Product: {}", product_id);

                    // 4. Ensure software row
                    println!("  📌 Ensuring software...");
                    let _ = sqlx
                        ::query(
                            "INSERT INTO software (video_game_id) VALUES ($1) ON CONFLICT (video_game_id) DO NOTHING"
                        )
                        .bind(product_id)
                        .execute(&mut *tx).await?;

                    // 5. Ensure video_game_title
                    println!("  📌 Ensuring title...");
                    let row = sqlx
                        ::query(
                            "INSERT INTO video_game_titles (video_game_id, name, slug) VALUES ($1, $2, $3) ON CONFLICT (video_game_id) DO UPDATE SET name = EXCLUDED.name RETURNING id"
                        )
                        .bind(product_id)
                        .bind(name)
                        .bind(&slug)
                        .fetch_one(&mut *tx).await?;
                    let title_id: i64 = row.get("id");
                    println!("    ✓ Title: {}", title_id);

                    // 5b. Update title metadata (summary, primary image, minimal source tag)
                    let summary: Option<&str> = obj.get("description").and_then(|d| d.as_str());
                    let primary_image = obj
                        .get("image")
                        .and_then(|im| im.get("super_url").or_else(|| im.get("original_url")))
                        .and_then(|u| u.as_str());
                    let meta = serde_json::json!({"source":"gb", "gb_guid": guid});
                    let _ = sqlx
                        ::query(
                            "UPDATE video_game_titles SET summary = COALESCE($1, summary), primary_image_url = COALESCE($2, primary_image_url), metadata = COALESCE(metadata,'{}'::jsonb) || $3::jsonb, updated_at = now() WHERE id = $4"
                        )
                        .bind(summary)
                        .bind(primary_image)
                        .bind(meta)
                        .bind(title_id)
                        .execute(&mut *tx).await?;

                    // 6. Ensure video_game
                    println!("  📌 Ensuring video game...");
                    let _vg_id: i64 = match
                        sqlx
                            ::query(
                                "SELECT id FROM video_games WHERE title_id = $1 AND platform_id = $2 AND edition IS NULL"
                            )
                            .bind(title_id)
                            .bind(platform_id)
                            .fetch_optional(&mut *tx).await?
                    {
                        Some(row) => {
                            let id: i64 = row.get("id");
                            println!("    ✓ Video game exists: {}", id);
                            id
                        }
                        None => {
                            let row = sqlx
                                ::query(
                                    "INSERT INTO video_games (title_id, platform_id, edition) VALUES ($1, $2, NULL) RETURNING id"
                                )
                                .bind(title_id)
                                .bind(platform_id)
                                .fetch_one(&mut *tx).await?;
                            let id: i64 = row.get("id");
                            println!("    ✓ Video game created: {}", id);
                            id
                        }
                    };

                    // 7. Provider item
                    println!("  📌 Ensuring provider item...");
                    let video_game_source_id: i64 = match
                        sqlx
                            ::query(
                                "SELECT id FROM provider_items WHERE provider_id = $1 AND external_item_id = $2"
                            )
                            .bind(provider_id)
                            .bind(&guid)
                            .fetch_optional(&mut *tx).await?
                    {
                        Some(row) => {
                            let id: i64 = row.get("id");
                            println!("    ✓ Provider item exists: {}", id);
                            id
                        }
                        None => {
                            let row = sqlx
                                ::query(
                                    "INSERT INTO provider_items (provider_id, external_item_id, payload) VALUES ($1, $2, $3) RETURNING id"
                                )
                                .bind(provider_id)
                                .bind(&guid)
                                .bind(&payload)
                                .fetch_one(&mut *tx).await?;
                            let id: i64 = row.get("id");
                            println!("    ✓ Provider item created: {}", id);
                            id
                        }
                    };

                    // 8. Link title to provider item (source mapping)
                    println!("  📌 Linking source mapping...");
                    let _ = sqlx
                        ::query(
                            "INSERT INTO video_game_title_sources (title_id, video_game_source_id) VALUES ($1, $2) ON CONFLICT (video_game_source_id) DO NOTHING"
                        )
                        .bind(title_id)
                        .bind(video_game_source_id)
                        .execute(&mut *tx).await?;

                    // Commit transaction
                    println!("  💾 Committing transaction...");
                    tx.commit().await?;
                    println!("  ✅ Transaction committed successfully!");

                    count += 1;
                }
            }
        }
        Value::Array(items) => {
            println!("📦 Found array with {} items", items.len());
            // Similar handling for array...
        }
        _ => {
            println!("⚠️  Unexpected JSON structure");
        }
    }

    println!("\n✨ Ingest complete! Processed {} titles", count);

    // Verify data
    let row = sqlx::query("SELECT COUNT(*) FROM video_game_titles")
        .fetch_one(&pool)
        .await?;
    let title_count: i64 = row.get(0);

    let row = sqlx
        ::query(
            "SELECT COUNT(*) FROM provider_items WHERE provider_id IN (SELECT id FROM providers WHERE slug = 'gb')"
        )
        .fetch_one(&pool).await?;
    let provider_item_count: i64 = row.get(0);

    println!("\n📊 Database verification:");
    println!("  • Total video_game_titles: {}", title_count);
    println!("  • GiantBomb provider_items: {}", provider_item_count);

    Ok(())
}
