use anyhow::Result;
use serde_json::{Deserializer, Value};
use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

pub struct MediaMap {
    // Back-compat image map used by existing call sites (get())
    images: HashMap<String, String>,
    // New: videos by normalized title
    videos: HashMap<String, String>,
    // Optional roles (labels) as seen in raw payloads
    image_roles: HashMap<String, String>,
    video_roles: HashMap<String, String>,
    // Star ratings if present in raw dump (average + count) keyed by normalized title
    ratings_avg: HashMap<String, f32>,
    ratings_count: HashMap<String, i64>,
    // Genres per normalized title (collected liberally from various keys)
    genres: HashMap<String, Vec<String>>,
}

impl MediaMap {
    pub fn empty() -> Self {
        Self {
            images: HashMap::new(),
            videos: HashMap::new(),
            image_roles: HashMap::new(),
            video_roles: HashMap::new(),
            ratings_avg: HashMap::new(),
            ratings_count: HashMap::new(),
            genres: HashMap::new(),
        }
    }

    pub fn from_file(path: impl AsRef<Path>, limit: Option<usize>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::empty());
        }
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        // Try streaming array first
        let mut images: HashMap<String, String> = HashMap::new();
        let mut videos: HashMap<String, String> = HashMap::new();
        let mut image_roles: HashMap<String, String> = HashMap::new();
        let mut video_roles: HashMap<String, String> = HashMap::new();
        let mut ratings_avg: HashMap<String, f32> = HashMap::new();
        let mut ratings_count: HashMap<String, i64> = HashMap::new();
        let mut genres: HashMap<String, Vec<String>> = HashMap::new();
        // Use generic Value stream to avoid rigid schema requirements
        let stream = Deserializer::from_reader(reader).into_iter::<Value>();
        // The file may be a huge array or a single object; we'll collect heuristically
        for value in stream.flatten() {
            Self::collect_from_value(
                &value,
                &mut images,
                &mut videos,
                &mut image_roles,
                &mut video_roles,
                &mut ratings_avg,
                &mut ratings_count,
                &mut genres,
                limit,
            );
            if let Some(l) = limit {
                if images.len() >= l && videos.len() >= l {
                    break;
                }
            }
        }
        Ok(Self {
            images,
            videos,
            image_roles,
            video_roles,
            ratings_avg,
            ratings_count,
            genres,
        })
    }

    fn collect_from_value(
        v: &Value,
        images: &mut HashMap<String, String>,
        videos: &mut HashMap<String, String>,
        image_roles: &mut HashMap<String, String>,
        video_roles: &mut HashMap<String, String>,
        ratings_avg: &mut HashMap<String, f32>,
        ratings_count: &mut HashMap<String, i64>,
        genres: &mut HashMap<String, Vec<String>>,
        limit: Option<usize>,
    ) {
        match v {
            Value::Array(arr) => {
                for item in arr {
                    Self::collect_from_value(
                        item,
                        images,
                        videos,
                        image_roles,
                        video_roles,
                        ratings_avg,
                        ratings_count,
                        genres,
                        limit,
                    );
                    if Self::maybe_break(images, limit) && Self::maybe_break(videos, limit) {
                        break;
                    }
                }
            }
            Value::Object(obj) => {
                // Try extract title
                let title_keys = ["title", "name", "game_title", "product_name"];
                let media_keys = [
                    "image_url",
                    "image",
                    "cover",
                    "thumbnail",
                    "thumb",
                    "preview",
                    "video",
                    "gameplay",
                    "trailer",
                ];
                let mut title: Option<String> = None;
                for k in &title_keys {
                    if let Some(Value::String(s)) = obj.get(*k) {
                        if !s.is_empty() {
                            title = Some(s.clone());
                            break;
                        }
                    }
                }
                let mut media_fallback: Option<String> = None;
                let mut video: Option<(String, Option<String>)> = None; // (url, role)
                let mut image: Option<(String, Option<String>)> = None; // (url, role)

                // First: handle canonical "media" array with (__typeof, url, role)
                if let Some(Value::Array(media_arr)) = obj.get("media") {
                    for el in media_arr {
                        if let Value::Object(o) = el {
                            let kind_raw = o.get("__typeof").and_then(|v| v.as_str()).unwrap_or("");
                            let kind = kind_raw.to_ascii_lowercase();
                            let url = o.get("url").and_then(|v| v.as_str());
                            let role = o
                                .get("role")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                            if let Some(u) = url {
                                match kind.as_str() {
                                    "video" | "gameplay" | "trailer" => {
                                        if video.is_none() {
                                            video = Some((u.to_string(), role.clone()));
                                        }
                                    }
                                    "preview" | "image" => {
                                        if image.is_none() {
                                            image = Some((u.to_string(), role.clone()));
                                        }
                                    }
                                    _ => {
                                        /* unknown type: don't block; set as image fallback */
                                        if image.is_none() {
                                            image = Some((u.to_string(), role.clone()));
                                        }
                                    }
                                }
                            }
                        }
                        if image.is_some() && video.is_some() {
                            break;
                        }
                    }
                }

                // Legacy fallbacks: various media-like fields
                for k in &media_keys {
                    match obj.get(*k) {
                        Some(Value::String(s)) if !s.is_empty() => {
                            media_fallback = media_fallback.or_else(|| Some(s.clone()));
                            if image.is_none() {
                                image = Some((s.clone(), None));
                            }
                            break;
                        }
                        Some(Value::Object(o)) => {
                            if let Some(Value::String(u)) = o.get("url") {
                                media_fallback = media_fallback.or_else(|| Some(u.clone()));
                                if image.is_none() {
                                    image = Some((
                                        u.clone(),
                                        o.get("role")
                                            .and_then(|v| v.as_str())
                                            .map(|s| s.to_string()),
                                    ));
                                }
                                break;
                            }
                        }
                        Some(Value::Array(a)) => {
                            for el in a {
                                if let Value::Object(o) = el {
                                    let kind_raw =
                                        o.get("__typeof").and_then(|v| v.as_str()).unwrap_or("");
                                    let kind = kind_raw.to_ascii_lowercase();
                                    let url = o.get("url").and_then(|v| v.as_str());
                                    let role = o
                                        .get("role")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string());
                                    if let Some(u) = url {
                                        match kind.as_str() {
                                            "video" | "gameplay" | "trailer" => {
                                                if video.is_none() {
                                                    video = Some((u.to_string(), role.clone()));
                                                }
                                            }
                                            "image" | "cover" | "thumbnail" | "thumb"
                                            | "preview" | "" => {
                                                if image.is_none() {
                                                    image = Some((u.to_string(), role.clone()));
                                                }
                                            }
                                            _ => {
                                                if image.is_none() {
                                                    image = Some((u.to_string(), role.clone()));
                                                }
                                            }
                                        }
                                    }
                                }
                                if image.is_some() && video.is_some() {
                                    break;
                                }
                            }
                            if image.is_some() && video.is_some() {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(t) = title {
                    let key = normalize_title(&t);
                    if let Some((u, role)) = image {
                        images.entry(key.clone()).or_insert(u);
                        if let Some(r) = role {
                            image_roles.entry(key.clone()).or_insert(r);
                        }
                    } else if let Some(u) = media_fallback {
                        images.entry(key.clone()).or_insert(u);
                    }
                    if let Some((u, role)) = video {
                        videos.entry(key.clone()).or_insert(u);
                        if let Some(r) = role {
                            video_roles.entry(key.clone()).or_insert(r);
                        }
                    }
                    // Capture rating fields if present on same object (associate with title)
                    Self::extract_ratings(obj, &key, ratings_avg, ratings_count);
                    // Capture genre fields if present
                    let gs = Self::extract_genres(obj);
                    if !gs.is_empty() {
                        genres.entry(key).or_default().extend(gs);
                    }
                }
                // Recurse for nested arrays/objects that might contain media
                for (_k, vv) in obj.iter() {
                    if vv.is_array() || vv.is_object() {
                        Self::collect_from_value(
                            vv,
                            images,
                            videos,
                            image_roles,
                            video_roles,
                            ratings_avg,
                            ratings_count,
                            genres,
                            limit,
                        );
                        if Self::maybe_break(images, limit) && Self::maybe_break(videos, limit) {
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn extract_ratings(
        obj: &serde_json::Map<String, Value>,
        key: &str,
        ratings_avg: &mut HashMap<String, f32>,
        ratings_count: &mut HashMap<String, i64>,
    ) {
        // Direct fields
        if let Some(v) = obj.get("averageRating") {
            if let Some(f) = v.as_f64() {
                ratings_avg.entry(key.to_string()).or_insert(f as f32);
            } else if let Some(s) = v.as_str().and_then(|s| s.parse::<f32>().ok()) {
                ratings_avg.entry(key.to_string()).or_insert(s);
            }
        }
        if let Some(v) = obj.get("ratingCount") {
            if let Some(i) = v.as_i64() {
                ratings_count.entry(key.to_string()).or_insert(i);
            } else if let Some(s) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                ratings_count.entry(key.to_string()).or_insert(s);
            }
        }
        // Nested common containers: starRating, rating, ratings
        for nested_key in ["starRating", "rating", "ratings"] {
            if let Some(Value::Object(nested)) = obj.get(nested_key) {
                if let Some(v) = nested.get("averageRating") {
                    if let Some(f) = v.as_f64() {
                        ratings_avg.entry(key.to_string()).or_insert(f as f32);
                    }
                }
                if let Some(v) = nested.get("ratingCount") {
                    if let Some(i) = v.as_i64() {
                        ratings_count.entry(key.to_string()).or_insert(i);
                    }
                }
                // Alternative key names
                if let Some(v) = nested.get("avg") {
                    if let Some(f) = v.as_f64() {
                        ratings_avg.entry(key.to_string()).or_insert(f as f32);
                    }
                }
                if let Some(v) = nested.get("count") {
                    if let Some(i) = v.as_i64() {
                        ratings_count.entry(key.to_string()).or_insert(i);
                    }
                }
            }
        }
    }

    // Liberal genre extraction: checks a variety of common keys and shapes
    fn extract_genres(obj: &serde_json::Map<String, Value>) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let keys = [
            "genres",
            "genre",
            "productGenres",
            "product_genres",
            "product_genre",
            "category",
            "categories",
        ];
        for k in keys {
            if let Some(val) = obj.get(k) {
                match val {
                    Value::String(s) => {
                        if !s.is_empty() {
                            out.push(s.clone());
                        }
                    }
                    Value::Array(arr) => {
                        for el in arr {
                            match el {
                                Value::String(s) => {
                                    if !s.is_empty() {
                                        out.push(s.clone());
                                    }
                                }
                                Value::Object(o) => {
                                    if let Some(dn) = o.get("displayName").and_then(|x| x.as_str())
                                    {
                                        out.push(dn.to_string());
                                    } else if let Some(name) =
                                        o.get("name").and_then(|x| x.as_str())
                                    {
                                        out.push(name.to_string());
                                    } else if let Some(key) = o.get("key").and_then(|x| x.as_str())
                                    {
                                        out.push(key.to_string());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        // Dedup and normalize casing (title-case display strings; keep UPPER keys)
        if !out.is_empty() {
            out.sort_unstable();
            out.dedup();
        }
        out
    }

    #[inline]
    fn maybe_break(map: &HashMap<String, String>, limit: Option<usize>) -> bool {
        if let Some(l) = limit {
            map.len() >= l
        } else {
            false
        }
    }

    pub fn get(&self, title: &str) -> Option<&str> {
        let key = normalize_title(title);
        self.images.get(&key).map(|s| s.as_str())
    }

    pub fn get_video(&self, title: &str) -> Option<&str> {
        let key = normalize_title(title);
        self.videos.get(&key).map(|s| s.as_str())
    }

    pub fn get_image_role(&self, title: &str) -> Option<&str> {
        let key = normalize_title(title);
        self.image_roles.get(&key).map(|s| s.as_str())
    }

    pub fn get_video_role(&self, title: &str) -> Option<&str> {
        let key = normalize_title(title);
        self.video_roles.get(&key).map(|s| s.as_str())
    }

    pub fn get_genres(&self, title: &str) -> Option<&[String]> {
        let key = normalize_title(title);
        self.genres.get(&key).map(|v| v.as_slice())
    }

    /// Return a list of (normalized_title, genres[]) for entries with any genre.
    pub fn titles_with_genres(&self) -> Vec<(String, Vec<String>)> {
        let mut out = Vec::new();
        for (k, gs) in &self.genres {
            if !gs.is_empty() {
                out.push((k.clone(), gs.clone()));
            }
        }
        out
    }

    pub fn get_rating_avg(&self, title: &str) -> Option<f32> {
        let key = normalize_title(title);
        self.ratings_avg.get(&key).copied()
    }

    pub fn get_rating_count(&self, title: &str) -> Option<i64> {
        let key = normalize_title(title);
        self.ratings_count.get(&key).copied()
    }

    pub fn get_rating(&self, title: &str) -> Option<(f32, i64)> {
        let key = normalize_title(title);
        match (self.ratings_avg.get(&key), self.ratings_count.get(&key)) {
            (Some(a), Some(c)) => Some((*a, *c)),
            _ => None,
        }
    }

    /// Return a list of (normalized_title, average, count) for entries that have both values.
    pub fn titles_with_ratings(&self) -> Vec<(String, f32, i64)> {
        let mut out = Vec::new();
        for (k, avg) in &self.ratings_avg {
            if let Some(cnt) = self.ratings_count.get(k) {
                out.push((k.clone(), *avg, *cnt));
            }
        }
        out
    }
}

pub fn normalize_title(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}
