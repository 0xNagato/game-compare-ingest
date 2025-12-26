use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::env;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tungstenite::protocol::Message;
use url::Url;

/// Spawns a background task that connects to Supabase Realtime and sends incoming
/// text messages to the returned receiver. Provide `supabase_url` like "https://<ref>.supabase.co"
/// and `api_key` (your "MyAPiKey"). `topic` is the channel name, e.g. "room:123:messages".
pub async fn start_realtime_listener(
    supabase_url: &str,
    api_key: &str,
    topic: &str,
) -> Result<mpsc::UnboundedReceiver<String>> {
    // Create a channel for forwarding incoming messages
    let (tx, rx) = mpsc::unbounded_channel::<String>();

    // Build the wss URL: convert https://... to wss://... and append path/query
    let mut base = get_env('SUPABASE_URL').to_string();
    if base.starts_with("https://") {
        base = base.replacen("https://", "wss://", 1);
    } else if base.starts_with("http://") {
        base = base.replacen("http://", "ws://", 1);
    }
    let ws_url = format!("{}/realtime/v1?apikey={}", base.trim_end_matches('/'), get_env('SUPABASE_SERVICE_ROLE_KEY'));
    let url = Url::parse(&ws_url).context("invalid websocket url")?;

    // Spawn background task so function returns immediately with receiver
    tokio::spawn(async move {
        match connect_async(url).await {
            Ok((ws_stream, _resp)) => {
                tracing::info!("[realtime] connected to realtime websocket");
                let (mut write, mut read) = ws_stream.split();

                // Send phx_join to subscribe to the topic
                let join = json!({
                    "topic": topic,
                    "event": "phx_join",
                    "payload": {},
                    "ref": "1"
                });
                if let Err(e) = write.send(Message::Text(join.to_string())).await {
                    tracing::error!("[realtime] failed to send join: {:?}", e);
                    return;
                }

                // Optional: send periodic pings/heartbeats (phx_ping) to keep connection alive
                let mut ping_ref: i64 = 2;
                let mut ping_tx = write.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(25)).await;
                        let ping = json!({
                            "topic": "phoenix",
                            "event": "phx_ping",
                            "payload": {},
                            "ref": ping_ref.to_string()
                        });
                        ping_ref += 1;
                        if let Err(e) = ping_tx.send(Message::Text(ping.to_string())).await {
                            tracing::warn!("[realtime] ping send failed: {:?}", e);
                            break;
                        }
                    }
                });

                // Read loop: forward text payloads into tx
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            // Forward raw JSON text to consumer
                            if tx.send(txt).is_err() {
                                tracing::info!("[realtime] consumer dropped, shutting down listener");
                                break;
                            }
                        }
                        Ok(Message::Ping(_)) => {
                            // Respond with Pong automatically by tungstenite; nothing to do
                        }
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Binary(bin)) => {
                            if let Ok(s) = String::from_utf8(bin) {
                                let _ = tx.send(s);
                            }
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::info!("[realtime] websocket closed: {:?}", frame);
                            break;
                        }
                        Err(e) => {
                            tracing::error!("[realtime] websocket error: {:?}", e);
                            break;
                        }
                    }
                }
                tracing::info!("[realtime] read loop ended");
            }
            Err(e) => {
                tracing::error!("[realtime] connect error: {:?}", e);
            }
        }
    });

    Ok(rx)
}