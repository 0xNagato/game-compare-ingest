use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// ITAD (IsThereAnyDeal) Provider for pricing and media data
/// Public API: https://api.isthereanydeal.com/v2/
///
/// Key endpoints:
/// - /search/search/{query} - Search for games
/// - /game/overview/{game_id} - Get game details with media
/// - /deals/latest - Get latest deals
/// - /charts - Get chart data (trending, popular, etc.)
///
/// Media support:
/// - Game cover image
/// - Screenshots
/// - Trailer videos
/// - Additional artwork
#[derive(Debug, Clone)]
pub struct ItadProvider {
    base_url: String,
    timeout_secs: u64,
    http: Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItadGame {
    pub id: String,
    pub title: String,
    pub slug: String,
    pub image_url: Option<String>,
    pub store_count: Option<i32>,
    pub lowest_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItadDeal {
    pub game_id: String,
    pub store_id: String,
    pub store_name: String,
    pub price: f64,
    pub regular_price: f64,
    pub discount: i32,
    pub url: String,
    pub currency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItadMedia {
    pub game_id: String,
    pub url: String,
    pub r#type: String, // "image" or "video"
    pub role: String,   // "cover", "screenshot", "trailer", etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItadResponse<T> {
    pub data: Option<T>,
    pub error: Option<String>,
}

impl ItadProvider {
    pub fn new(base_url: Option<&str>, timeout_secs: Option<u64>) -> Result<Self> {
        let base_url = base_url
            .unwrap_or("https://api.isthereanydeal.com/v2")
            .trim_end_matches('/')
            .to_string();
        let timeout_secs = timeout_secs.unwrap_or(15);
        let http = Client::builder()
            .user_agent("ItadProvider/1.0")
            .timeout(Duration::from_secs(timeout_secs))
            .build()?;

        Ok(Self {
            base_url,
            timeout_secs,
            http,
        })
    }

    /// Search for a game by title
    pub async fn search_game(&self, query: &str) -> Result<Vec<ItadGame>> {
        let url = format!(
            "{}/search/search/{}",
            self.base_url,
            urlencoding::encode(query)
        );

        let resp = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("ITAD search failed: {}", resp.status()));
        }

        let body: Value = resp.json().await?;

        let mut games = Vec::new();
        if let Some(results) = body
            .get("data")
            .and_then(|d| d.get("results"))
            .and_then(|r| r.as_array())
        {
            for item in results {
                if let (Some(id), Some(title)) = (
                    item.get("id").and_then(|v| v.as_str()),
                    item.get("title").and_then(|v| v.as_str()),
                ) {
                    games.push(ItadGame {
                        id: id.to_string(),
                        title: title.to_string(),
                        slug: item
                            .get("slug")
                            .and_then(|v| v.as_str())
                            .unwrap_or(id)
                            .to_string(),
                        image_url: item
                            .get("image")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        store_count: item
                            .get("storeCount")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as i32),
                        lowest_price: item.get("lowestPrice").and_then(|v| v.as_f64()),
                    });
                }
            }
        }

        Ok(games)
    }

    /// Get game overview with details and media
    pub async fn game_overview(&self, game_id: &str) -> Result<(ItadGame, Vec<ItadMedia>)> {
        let url = format!("{}/game/overview/{}", self.base_url, game_id);

        let resp = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("ITAD overview failed: {}", resp.status()));
        }

        let body: Value = resp.json().await?;

        let game_data = body.get("data").and_then(|d| d.get("game"));

        if game_data.is_none() {
            return Err(anyhow!("No game data in ITAD response"));
        }

        let game_data = game_data.unwrap();

        let game = ItadGame {
            id: game_id.to_string(),
            title: game_data
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown")
                .to_string(),
            slug: game_data
                .get("slug")
                .and_then(|v| v.as_str())
                .unwrap_or(game_id)
                .to_string(),
            image_url: game_data
                .get("image")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            store_count: None,
            lowest_price: None,
        };

        // Extract media
        let mut media = Vec::new();

        // Cover image
        if let Some(img) = game_data.get("image").and_then(|v| v.as_str()) {
            media.push(ItadMedia {
                game_id: game_id.to_string(),
                url: img.to_string(),
                r#type: "image".to_string(),
                role: "cover".to_string(),
            });
        }

        // Screenshots from screenshot array
        if let Some(screenshots) = game_data.get("screenshots").and_then(|v| v.as_array()) {
            for (idx, screenshot) in screenshots.iter().enumerate() {
                if let Some(url) = screenshot.as_str() {
                    media.push(ItadMedia {
                        game_id: game_id.to_string(),
                        url: url.to_string(),
                        r#type: "image".to_string(),
                        role: format!("screenshot_{}", idx),
                    });
                } else if let Some(url) = screenshot.get("image").and_then(|v| v.as_str()) {
                    media.push(ItadMedia {
                        game_id: game_id.to_string(),
                        url: url.to_string(),
                        r#type: "image".to_string(),
                        role: format!("screenshot_{}", idx),
                    });
                }
            }
        }

        // Videos/trailers
        if let Some(videos) = game_data.get("videos").and_then(|v| v.as_array()) {
            for (_idx, video) in videos.iter().enumerate() {
                if let Some(url) = video.as_str() {
                    media.push(ItadMedia {
                        game_id: game_id.to_string(),
                        url: url.to_string(),
                        r#type: "video".to_string(),
                        role: "trailer".to_string(),
                    });
                } else if let Some(url) = video.get("url").and_then(|v| v.as_str()) {
                    let video_type = video
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("video")
                        .to_string();
                    media.push(ItadMedia {
                        game_id: game_id.to_string(),
                        url: url.to_string(),
                        r#type: "video".to_string(),
                        role: video_type,
                    });
                }
            }
        }

        Ok((game, media))
    }

    /// Get latest deals
    /// Optional filters: shops, until (timestamp), exclude (comma-separated IDs)
    pub async fn get_latest_deals(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<ItadDeal>> {
        let limit = limit.unwrap_or(100).min(500); // API max is 500
        let offset = offset.unwrap_or(0);

        let url = format!(
            "{}/deals/latest?limit={}&offset={}",
            self.base_url, limit, offset
        );

        let resp = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("ITAD deals fetch failed: {}", resp.status()));
        }

        let body: Value = resp.json().await?;
        let mut deals = Vec::new();

        if let Some(deals_array) = body
            .get("data")
            .and_then(|d| d.get("deals"))
            .and_then(|r| r.as_array())
        {
            for deal in deals_array {
                if let (Some(game_id), Some(shop), Some(price)) = (
                    deal.get("gameID").and_then(|v| v.as_str()),
                    deal.get("shop").and_then(|v| v.as_object()),
                    deal.get("price").and_then(|v| v.as_f64()),
                ) {
                    let shop_id = shop.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
                    let shop_name = shop
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    let regular_price = deal
                        .get("regularPrice")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(price);
                    let discount = ((1.0 - price / regular_price) * 100.0).round() as i32;

                    deals.push(ItadDeal {
                        game_id: game_id.to_string(),
                        store_id: shop_id.to_string(),
                        store_name: shop_name.to_string(),
                        price,
                        regular_price,
                        discount: discount.max(0),
                        url: deal
                            .get("url")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        currency: deal
                            .get("currency")
                            .and_then(|v| v.as_str())
                            .unwrap_or("USD")
                            .to_string(),
                    });
                }
            }
        }

        Ok(deals)
    }

    /// Get popular/trending games
    pub async fn get_trending(&self, limit: Option<u32>) -> Result<Vec<ItadGame>> {
        let limit = limit.unwrap_or(50).min(100);
        let url = format!("{}/charts/popular?limit={}", self.base_url, limit);

        let resp = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("ITAD trending fetch failed: {}", resp.status()));
        }

        let body: Value = resp.json().await?;
        let mut games = Vec::new();

        if let Some(items) = body
            .get("data")
            .and_then(|d| d.get("games"))
            .and_then(|r| r.as_array())
        {
            for item in items {
                if let (Some(id), Some(title)) = (
                    item.get("id").and_then(|v| v.as_str()),
                    item.get("title").and_then(|v| v.as_str()),
                ) {
                    games.push(ItadGame {
                        id: id.to_string(),
                        title: title.to_string(),
                        slug: item
                            .get("slug")
                            .and_then(|v| v.as_str())
                            .unwrap_or(id)
                            .to_string(),
                        image_url: item
                            .get("image")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        store_count: None,
                        lowest_price: item.get("price").and_then(|v| v.as_f64()),
                    });
                }
            }
        }

        Ok(games)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_provider_initialization() {
        let provider = ItadProvider::new(None, Some(15)).unwrap();
        assert!(provider.base_url.contains("isthereanydeal"));
    }
}
