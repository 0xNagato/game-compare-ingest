use crate::database_ops::media_map::MediaMap;
use anyhow::Result;

pub fn print_from_env() -> Result<()> {
    let path = std::env::var("MEDIA_MAP_FILE").unwrap_or_else(|_| "merged_final.json".to_string());
    let limit = std::env::var("MEDIA_MAP_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let top = std::env::var("PRINT_TOP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(20);

    let map = MediaMap::from_file(&path, limit).unwrap_or_else(|_| MediaMap::empty());
    let mut items = map.titles_with_ratings();
    items.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| b.1.total_cmp(&a.1)));

    println!("Ratings parsed from {}:", path);
    println!("Total titles with ratings: {}", items.len());
    for (idx, (title_slug, avg, cnt)) in items.iter().take(top).enumerate() {
        let genres_preview = map
            .get_genres(title_slug)
            .map(|gs| gs.iter().take(3).cloned().collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        if genres_preview.is_empty() {
            println!(
                "{:>3}. {:<40} avg={:.2} count={}",
                idx + 1,
                title_slug,
                avg,
                cnt
            );
        } else {
            println!(
                "{:>3}. {:<40} avg={:.2} count={} genres=[{}]",
                idx + 1,
                title_slug,
                avg,
                cnt,
                genres_preview
            );
        }
    }
    if items.len() > top {
        println!("... (showing top {} of {})", top, items.len());
    }
    let titles_with_genres = map.titles_with_genres();
    println!("Titles with genres: {}", titles_with_genres.len());
    Ok(())
}
