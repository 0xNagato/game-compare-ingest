use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

fn truncate_for_log(mut s: String, max_len: usize) -> String {
    if s.len() > max_len {
        s.truncate(max_len);
        s.push_str("…");
    }
    s
}

/// ITAD (IsThereAnyDeal) Provider for pricing and media data
/// Public API (base): https://api.isthereanydeal.com/
///
/// Key endpoints:
/// - GET /games/search/v1?title=... - Search for games
/// - GET /games/info/v2?id=... - Get game details with assets (media)
/// - GET /deals/v2 - Get latest deals list (paged)
/// - (Optional) charts endpoints exist but are not required for price ingest
///
/// Media support:
/// - Game cover image
/// - Screenshots
/// - Trailer videos
/// - Additional artwork
#[derive(Debug, Clone)]
pub struct ItadProvider {
    base_url: String,
    http: Client,
    api_key: Option<String>,
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
            .unwrap_or("https://api.isthereanydeal.com")
            .trim_end_matches('/')
            .to_string();
        let timeout_secs = timeout_secs.unwrap_or(15);
        let http = Client::builder()
            .user_agent("ItadProvider/1.0")
            .timeout(Duration::from_secs(timeout_secs))
            .build()?;

        Ok(Self {
            base_url,
            http,
            api_key: None,
        })
    }

    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key.filter(|s| !s.trim().is_empty());
        self
    }

    fn add_auth_query<'a>(&'a self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.api_key.as_deref() {
            Some(key) => req.query(&[("key", key)]),
            None => req,
        }
    }

    fn value_as_f64(v: &Value) -> Option<f64> {
        if let Some(n) = v.as_f64() {
            return Some(n);
        }
        if let Some(n) = v.as_i64() {
            return Some(n as f64);
        }
        if let Some(s) = v.as_str() {
            return s.parse::<f64>().ok();
        }
        None
    }

    fn extract_asset_url(obj: &Value, key: &str) -> Option<String> {
        obj.get("assets")
            .and_then(|a| a.get(key))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    fn extract_screenshots(obj: &Value) -> Vec<String> {
        let mut out = Vec::new();
        let Some(arr) = obj
            .get("assets")
            .and_then(|a| a.get("screenshots"))
            .and_then(|v| v.as_array())
        else {
            return out;
        };

        for item in arr {
            if let Some(s) = item.as_str() {
                if !s.trim().is_empty() {
                    out.push(s.to_string());
                }
                continue;
            }
            if let Some(s) = item.get("url").and_then(|v| v.as_str()) {
                if !s.trim().is_empty() {
                    out.push(s.to_string());
                }
            }
        }

        out
    }

    fn extract_videos(obj: &Value) -> Vec<String> {
        let mut out = Vec::new();
        let Some(arr) = obj
            .get("assets")
            .and_then(|a| a.get("videos"))
            .and_then(|v| v.as_array())
        else {
            return out;
        };

        for item in arr {
            if let Some(s) = item.as_str() {
                if !s.trim().is_empty() {
                    out.push(s.to_string());
                }
                continue;
            }
            if let Some(s) = item.get("url").and_then(|v| v.as_str()) {
                if !s.trim().is_empty() {
                    out.push(s.to_string());
                }
            }
        }

        out
    }

    /// Search for a game by title
    pub async fn search_game(&self, query: &str) -> Result<Vec<ItadGame>> {
        let url = format!("{}/games/search/v1", self.base_url);

        let req = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .query(&[("title", query)]);
        let resp = self.add_auth_query(req).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = truncate_for_log(resp.text().await.unwrap_or_default(), 2000);
            return Err(anyhow!(
                "ITAD search failed: {status} url={url} body={body}"
            ));
        }

        let body: Value = resp.json().await?;

        let mut games = Vec::new();
        if let Some(results) = body.as_array() {
            for item in results {
                let id = item.get("id").and_then(|v| v.as_str());
                let title = item.get("title").and_then(|v| v.as_str());
                if let (Some(id), Some(title)) = (id, title) {
                    games.push(ItadGame {
                        id: id.to_string(),
                        title: title.to_string(),
                        slug: item
                            .get("slug")
                            .and_then(|v| v.as_str())
                            .unwrap_or(id)
                            .to_string(),
                        image_url: Self::extract_asset_url(item, "boxart")
                            .or_else(|| Self::extract_asset_url(item, "banner")),
                        store_count: None,
                        lowest_price: None,
                    });
                }
            }
        }

        Ok(games)
    }

    /// Get game overview with details and media
    pub async fn game_overview(&self, game_id: &str) -> Result<(ItadGame, Vec<ItadMedia>)> {
        let url = format!("{}/games/info/v2", self.base_url);

        let req = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .query(&[("id", game_id)]);
        let resp = self.add_auth_query(req).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = truncate_for_log(resp.text().await.unwrap_or_default(), 2000);
            return Err(anyhow!(
                "ITAD overview failed: {status} url={url} game_id={game_id} body={body}"
            ));
        }

        let body: Value = resp.json().await?;

        // /games/info/v2 returns a single object (not wrapped in {data: ...}).
        if !body.is_object() {
            return Err(anyhow!(
                "Unexpected ITAD game info response shape (expected object)"
            ));
        }

        let title = body
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        let slug = body
            .get("slug")
            .and_then(|v| v.as_str())
            .unwrap_or(game_id)
            .to_string();

        let image_url = Self::extract_asset_url(&body, "boxart")
            .or_else(|| Self::extract_asset_url(&body, "banner"));

        let game = ItadGame {
            id: game_id.to_string(),
            title,
            slug,
            image_url,
            store_count: None,
            lowest_price: None,
        };

        // Extract media
        let mut media = Vec::new();

        // Cover image
        if let Some(img) = game.image_url.as_deref().filter(|s| !s.trim().is_empty()) {
            media.push(ItadMedia {
                game_id: game_id.to_string(),
                url: img.to_string(),
                r#type: "image".to_string(),
                role: "cover".to_string(),
            });
        }

        // Screenshots
        for (idx, url) in Self::extract_screenshots(&body).into_iter().enumerate() {
            media.push(ItadMedia {
                game_id: game_id.to_string(),
                url,
                r#type: "image".to_string(),
                role: format!("screenshot_{idx}"),
            });
        }

        // Videos
        for url in Self::extract_videos(&body) {
            media.push(ItadMedia {
                game_id: game_id.to_string(),
                url,
                r#type: "video".to_string(),
                role: "video".to_string(),
            });
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
        self.get_latest_deals_for_country(limit, offset, None).await
    }

    /// Get latest deals, optionally scoped to a country.
    pub async fn get_latest_deals_for_country(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
        country: Option<&str>,
    ) -> Result<Vec<ItadDeal>> {
        let limit = limit.unwrap_or(100).min(500); // API max is 500
        let offset = offset.unwrap_or(0);

        let url = format!("{}/deals/v2", self.base_url);
        let mut req = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .query(&[("limit", limit.to_string()), ("offset", offset.to_string())]);
        if let Some(country) = country {
            req = req.query(&[("country", country)]);
        }

        let resp = self.add_auth_query(req).send().await?;
        let status = resp.status();

        if !status.is_success() {
            let body = truncate_for_log(resp.text().await.unwrap_or_default(), 2000);
            return Err(anyhow!(
                "ITAD deals fetch failed: {status} url={url} country={:?} limit={limit} offset={offset} body={body}",
                country
            ));
        }

        let body: Value = resp.json().await?;
        let mut deals = Vec::new();

        let Some(deals_array) = body.get("list").and_then(|v| v.as_array()) else {
            // Defensive: keep a helpful error for unexpected shapes (prevents silent 0 deals).
            return Err(anyhow!(
                "Unexpected ITAD deals response shape (missing 'list' array)"
            ));
        };

        for item in deals_array {
            // Common structure in docs: { deal: {...}, game: {...} }
            let deal_obj = item.get("deal").unwrap_or(item);
            let game_obj = item.get("game").unwrap_or(&Value::Null);

            let game_id = game_obj
                .get("id")
                .and_then(|v| v.as_str())
                .or_else(|| deal_obj.get("gameId").and_then(|v| v.as_str()))
                .or_else(|| deal_obj.get("gameID").and_then(|v| v.as_str()));

            let shop_obj = deal_obj.get("shop");
            let shop_id = shop_obj
                .and_then(|s| s.get("id"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let shop_name = shop_obj
                .and_then(|s| s.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            let price = deal_obj
                .get("price")
                .and_then(|p| p.get("amount"))
                .and_then(Self::value_as_f64)
                .or_else(|| deal_obj.get("price").and_then(Self::value_as_f64));

            let regular_price = deal_obj
                .get("regular")
                .and_then(|p| p.get("amount"))
                .and_then(Self::value_as_f64)
                .or_else(|| deal_obj.get("regularPrice").and_then(Self::value_as_f64));

            let currency = deal_obj
                .get("price")
                .and_then(|p| p.get("currency"))
                .and_then(|v| v.as_str())
                .or_else(|| deal_obj.get("currency").and_then(|v| v.as_str()))
                .unwrap_or("USD")
                .to_string();

            let Some(game_id) = game_id else {
                continue;
            };
            let Some(price) = price else {
                continue;
            };

            let regular_price = regular_price.unwrap_or(price);
            let discount = deal_obj
                .get("cut")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32)
                .unwrap_or_else(|| ((1.0 - price / regular_price) * 100.0).round() as i32)
                .max(0);

            let url = deal_obj
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            deals.push(ItadDeal {
                game_id: game_id.to_string(),
                store_id: shop_id.to_string(),
                store_name: shop_name.to_string(),
                price,
                regular_price,
                discount,
                url,
                currency,
            });
        }

        Ok(deals)
    }

    /// Get popular/trending games
    pub async fn get_trending(&self, limit: Option<u32>) -> Result<Vec<ItadGame>> {
        let limit = limit.unwrap_or(50).min(100);
        // NOTE: Charts endpoints have changed across ITAD API iterations and are not required
        // for the price ingest flow. We keep this method best-effort.
        let url = format!("{}/charts/popular?limit={}", self.base_url, limit);

        let req = self.http.get(&url).header("Accept", "application/json");
        let resp = self.add_auth_query(req).send().await?;
        let status = resp.status();

        if !status.is_success() {
            let body = truncate_for_log(resp.text().await.unwrap_or_default(), 2000);
            return Err(anyhow!(
                "ITAD trending fetch failed: {status} url={url} limit={limit} body={body}"
            ));
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
