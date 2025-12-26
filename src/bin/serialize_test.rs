use i_miss_rust::util::env;
use psstore_client::PsProductSummary;
use serde_json;

fn main() {
    env::bootstrap_cli("serialize_test");
    let sample = PsProductSummary {
        product_id: Some("test123".into()),
        concept_id: Some("conceptX".into()),
        name: Some("Sample Game".into()),
        release_date: Some("2025-11-01".into()),
        base_price_minor: Some(5999),
        discounted_price_minor: Some(2999),
        is_free: Some(false),
        media_urls: vec!["https://example.com/a.jpg".into()],
        media_image_urls: vec!["https://example.com/a.jpg".into()],
        media_video_urls: vec!["https://example.com/a.mp4".into()],
        media_images: vec![],
        media_videos: vec![],
        genres: vec!["action".into(), "adventure".into()],
        average_rating: Some(4.25),
        rating_count: Some(128),
    };
    let v = vec![sample];
    let json = serde_json::to_string_pretty(&v).expect("serialize PsProductSummary");
    println!("{}", json);
}
