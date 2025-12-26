use std::fs;
use std::path::Path;

const SCHEMA_FILE: &str = "docs/schema_reference.sql";

fn read_schema() -> String {
    if !Path::new(SCHEMA_FILE).exists() {
        panic!("{SCHEMA_FILE} is missing. Run scripts/schema_dump.sh to generate it.");
    }
    fs::read_to_string(SCHEMA_FILE)
        .unwrap_or_else(|err| panic!("failed to read {SCHEMA_FILE}: {err}"))
}

#[test]
fn schema_snapshot_is_not_empty() {
    let contents = read_schema();
    assert!(
        !contents.trim().is_empty(),
        "schema snapshot is empty; rerun scripts/schema_dump.sh"
    );
}

#[test]
fn schema_snapshot_mentions_core_tables() {
    let contents = read_schema();
    let needles = [
        "CREATE TABLE public.currencies",
        "CREATE TABLE public.video_game_titles",
        "CREATE TABLE public.prices",
        "CREATE TABLE public.current_price",
        "CREATE TABLE public.provider_offers",
    ];

    for needle in needles {
        assert!(
            contents.contains(needle),
            "expected to find `{}` in schema snapshot; rerun scripts/schema_dump.sh",
            needle
        );
    }
}
