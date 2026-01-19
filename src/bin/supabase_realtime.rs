use anyhow::{anyhow, Context, Result};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use i_miss_rust::util::env as env_util;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhxMessage {
    topic: String,
    event: String,
    payload: Value,
    #[serde(rename = "ref")]
    ref_field: Option<String>,
    #[serde(rename = "join_ref")]
    join_ref: Option<String>,
}

impl PhxMessage {
    fn join(topic: &str, payload: Value, reference: Option<&str>) -> Self {
        Self {
            topic: topic.to_owned(),
            event: "phx_join".into(),
            payload,
            ref_field: reference.map(|r| r.to_owned()),
            join_ref: None,
        }
    }

    fn heartbeat(reference: Option<&str>) -> Self {
        Self {
            topic: "phoenix".into(),
            event: "heartbeat".into(),
            payload: json!({}),
            ref_field: reference.map(|r| r.to_owned()),
            join_ref: None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("supabase_realtime");

    let supabase_url = env::var("SUPABASE_HTTP_URL")
        .context("SUPABASE_URL env required (e.g. https://xyz.supabase.co)")?;
    let anon_key = env::var("SUPABASE_APIKEY").context("SUPABASE_APIKEY env required")?;
    let topic = env::var("SUPABASE_REALTIME_TOPIC")
        .unwrap_or_else(|_| "realtime:public:prices".to_string());

    let mut base = supabase_url
        .trim_end_matches('/')
        .replace("https://", "wss://");
    base = base.replace("http://", "ws://");
    let ws_url = format!("{base}/realtime/v1/websocket?apikey={anon_key}");

    let url = Url::parse(&ws_url).map_err(|e| anyhow!("invalid SUPABASE_URL: {e}"))?;
    println!("[supabase_realtime] Connecting to {url}");

    let (ws_stream, response) = connect_async(ws_url.clone()).await?;
    println!(
        "[supabase_realtime] WebSocket handshake complete: status {}",
        response.status()
    );

    let (mut write, mut read) = ws_stream.split();

    let join_msg = PhxMessage::join(&topic, json!({}), Some("1"));
    send_envelope(&mut write, &join_msg).await?;
    println!("[supabase_realtime] Sent phx_join for topic '{topic}'");

    // Supabase expects periodic heartbeats (~30s). Use a conservative 25 second interval.
    let mut heartbeat = interval(Duration::from_secs(25));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let hb = PhxMessage::heartbeat(Some("hb"));
                if let Err(e) = send_envelope(&mut write, &hb).await {
                    eprintln!("[supabase_realtime] heartbeat send failed: {e:?}");
                    break;
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        match serde_json::from_str::<Value>(&txt) {
                            Ok(value) => {
                                println!("[supabase_realtime] Received JSON: {}", serde_json::to_string_pretty(&value)?);
                            }
                            Err(_) => println!("[supabase_realtime] Received text: {txt}"),
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        println!("[supabase_realtime] Received binary message ({} bytes)", bin.len());
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await.ok();
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Frame(_))) => {
                        // Frames are internal control details; no action needed for client monitoring.
                    }
                    Some(Ok(Message::Close(frame))) => {
                        println!("[supabase_realtime] Connection closed by server: {:?}", frame);
                        break;
                    }
                    Some(Err(err)) => {
                        eprintln!("[supabase_realtime] Read error: {err:?}");
                        break;
                    }
                    None => {
                        println!("[supabase_realtime] WebSocket stream ended");
                        break;
                    }
                }
            }
        }
    }

    println!("[supabase_realtime] Shutdown");
    Ok(())
}

async fn send_envelope(
    write: &mut SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>,
    msg: &PhxMessage,
) -> Result<()> {
    let payload = serde_json::to_string(msg)?;
    write
        .send(Message::Text(payload))
        .await
        .map_err(|e| anyhow!("websocket send error: {e}"))
}
