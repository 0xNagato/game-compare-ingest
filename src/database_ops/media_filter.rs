//! Shared media filtering and classification utilities.
//!
//! This module provides consistent media filtering across all providers to ensure:
//! 1. Screenshots are excluded (they clutter the database and provide minimal user value)
//! 2. High-value media is prioritized (covers, hero images, backgrounds, artwork)
//! 3. Media types are classified consistently
//!
//! **Reference Implementation**: PlayStation Store provider (playstation/prices.rs)
//! demonstrates the gold standard for media filtering.

use std::collections::HashMap;

/// Media classification for images
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageType {
    Cover,       // Box art, cover art, primary product image
    Hero,        // Hero/banner images, key art
    Background,  // Background images, backdrops
    Artwork,     // Promotional artwork, concept art
    Character,   // Character portraits, character art
    Logo,        // Game logos
    Icon,        // App icons, small icons
    Screenshot,  // Screenshots (LOWEST PRIORITY - typically excluded)
    Thumbnail,   // Thumbnail images (typically excluded)
    Unknown,     // Unclassified images
}

/// Media classification for videos
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VideoType {
    Trailer,      // Official trailers
    Advertisement, // Promotional videos, adverts
    Preview,      // Preview clips, teasers
    Gameplay,     // Gameplay footage
    Unknown,      // Unclassified videos
}

impl ImageType {
    /// Priority order for image types (lower = higher priority)
    /// Screenshots and thumbnails get highest number (lowest priority)
    pub fn priority(&self) -> u8 {
        match self {
            ImageType::Cover => 1,
            ImageType::Hero => 2,
            ImageType::Background => 3,
            ImageType::Artwork => 4,
            ImageType::Character => 5,
            ImageType::Logo => 6,
            ImageType::Icon => 7,
            ImageType::Thumbnail => 98,
            ImageType::Screenshot => 99,
            ImageType::Unknown => 50,
        }
    }

    /// Convert to database media_type string
    pub fn to_media_type(&self) -> &'static str {
        match self {
            ImageType::Cover => "cover",
            ImageType::Hero => "hero",
            ImageType::Background => "background",
            ImageType::Artwork => "artwork",
            ImageType::Character => "character",
            ImageType::Logo => "logo",
            ImageType::Icon => "icon",
            ImageType::Screenshot => "screenshot",
            ImageType::Thumbnail => "thumbnail",
            ImageType::Unknown => "image",
        }
    }

    /// Should this image type be included in ingestion?
    /// By default, excludes screenshots, thumbnails, logos, and icons.
    pub fn should_include(&self, include_screenshots: bool) -> bool {
        match self {
            ImageType::Screenshot | ImageType::Thumbnail => include_screenshots,
            ImageType::Logo | ImageType::Icon => false, // Always exclude
            _ => true,
        }
    }
}

impl VideoType {
    /// Priority order for video types (lower = higher priority)
    pub fn priority(&self) -> u8 {
        match self {
            VideoType::Trailer => 1,
            VideoType::Advertisement => 2,
            VideoType::Preview => 3,
            VideoType::Gameplay => 4,
            VideoType::Unknown => 50,
        }
    }

    /// Convert to database media_type string
    pub fn to_media_type(&self) -> &'static str {
        match self {
            VideoType::Trailer => "trailer",
            VideoType::Advertisement => "advertisement",
            VideoType::Preview => "preview",
            VideoType::Gameplay => "gameplay",
            VideoType::Unknown => "video",
        }
    }
}

/// Classify image from URL patterns and optional role/purpose metadata
///
/// # Examples
/// ```
/// use i_miss_rust::database_ops::media_filter::classify_image_from_url;
///
/// let img_type = classify_image_from_url("https://cdn.example.com/cover.jpg", None);
/// assert_eq!(img_type, ImageType::Cover);
///
/// let img_type = classify_image_from_url("https://cdn.example.com/img.jpg", Some("BoxArt"));
/// assert_eq!(img_type, ImageType::Cover);
/// ```
pub fn classify_image_from_url(url: &str, role: Option<&str>) -> ImageType {
    let url_lc = url.to_ascii_lowercase();

    // Check role/purpose first (provider metadata)
    if let Some(r) = role {
        let role_lc = r.to_ascii_lowercase();

        // Cover/BoxArt patterns
        if role_lc.contains("boxart")
            || role_lc.contains("box-art")
            || role_lc.contains("cover")
            || role_lc.contains("poster")
        {
            return ImageType::Cover;
        }

        // Hero/Banner patterns
        if role_lc.contains("hero") || role_lc.contains("banner") || role_lc.contains("keyart") {
            return ImageType::Hero;
        }

        // Background patterns
        if role_lc.contains("background") || role_lc.contains("backdrop") {
            return ImageType::Background;
        }

        // Artwork patterns
        if role_lc.contains("artwork") || role_lc.contains("promo") {
            return ImageType::Artwork;
        }

        // Screenshot patterns (to exclude)
        if role_lc.contains("screenshot") || role_lc.contains("screen-shot") {
            return ImageType::Screenshot;
        }

        // Logo/Icon patterns (to exclude)
        if role_lc.contains("logo") || role_lc.contains("icon") {
            return ImageType::Logo;
        }
    }

    // Explicit screenshot detection from URL (EXCLUDE by default)
    if url_lc.contains("screenshot")
        || url_lc.contains("screenshots")
        || url_lc.contains("screen-shot")
        || url_lc.contains("screen_shot")
    {
        return ImageType::Screenshot;
    }

    // Explicit thumbnail detection from URL (EXCLUDE)
    if url_lc.contains("thumb") || url_lc.contains("thumbnail") {
        return ImageType::Thumbnail;
    }

    // Explicit logo/icon detection from URL (EXCLUDE)
    if url_lc.contains("logo") || url_lc.contains("icon") {
        return ImageType::Logo;
    }

    // Cover/BoxArt patterns in URL
    if url_lc.contains("cover")
        || url_lc.contains("boxart")
        || url_lc.contains("box-art")
        || url_lc.contains("poster")
    {
        return ImageType::Cover;
    }

    // Hero/Banner patterns in URL
    if url_lc.contains("hero")
        || url_lc.contains("banner")
        || url_lc.contains("keyart")
        || url_lc.contains("key-art")
    {
        return ImageType::Hero;
    }

    // Background patterns in URL
    if url_lc.contains("background") || url_lc.contains("backdrop") {
        return ImageType::Background;
    }

    // Artwork patterns in URL
    if url_lc.contains("artwork") || url_lc.contains("promo") {
        return ImageType::Artwork;
    }

    // Character patterns in URL
    if url_lc.contains("character") || url_lc.contains("portrait") {
        return ImageType::Character;
    }

    // Default: treat as unknown (will be filtered based on policy)
    ImageType::Unknown
}

/// Classify video from URL patterns and optional metadata
pub fn classify_video_from_url(url: &str, video_type: Option<&str>) -> VideoType {
    let url_lc = url.to_ascii_lowercase();

    // Check video_type metadata first
    if let Some(vt) = video_type {
        let vt_lc = vt.to_ascii_lowercase();

        if vt_lc.contains("trailer") {
            return VideoType::Trailer;
        }
        if vt_lc.contains("ad") || vt_lc.contains("advert") || vt_lc.contains("commercial") {
            return VideoType::Advertisement;
        }
        if vt_lc.contains("preview") || vt_lc.contains("teaser") {
            return VideoType::Preview;
        }
        if vt_lc.contains("gameplay") || vt_lc.contains("game-play") {
            return VideoType::Gameplay;
        }
    }

    // Check URL patterns
    if url_lc.contains("trailer") {
        return VideoType::Trailer;
    }
    if url_lc.contains("gameplay") || url_lc.contains("game-play") {
        return VideoType::Gameplay;
    }
    if url_lc.contains("preview") || url_lc.contains("teaser") {
        return VideoType::Preview;
    }

    VideoType::Unknown
}

/// Filter and prioritize images based on classification
///
/// Returns a filtered list of images, excluding screenshots/thumbnails/logos by default.
/// Images are sorted by priority (covers first, screenshots last if included).
pub fn filter_images<T>(
    images: Vec<(String, ImageType, T)>,
    include_screenshots: bool,
) -> Vec<(String, ImageType, T)> {
    let mut filtered: Vec<(String, ImageType, T)> = images
        .into_iter()
        .filter(|(_, img_type, _)| img_type.should_include(include_screenshots))
        .collect();

    // Sort by priority (lower number = higher priority)
    filtered.sort_by_key(|(_, img_type, _)| img_type.priority());

    filtered
}

/// Filter and prioritize videos based on classification
///
/// Returns videos sorted by priority (trailers first, gameplay last).
pub fn filter_videos<T>(videos: Vec<(String, VideoType, T)>) -> Vec<(String, VideoType, T)> {
    let mut filtered = videos;
    filtered.sort_by_key(|(_, video_type, _)| video_type.priority());
    filtered
}

/// Check if screenshots should be included based on environment variable
pub fn should_include_screenshots() -> bool {
    std::env::var("MEDIA_INCLUDE_SCREENSHOTS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Statistics for media classification (for logging/debugging)
#[derive(Debug, Default)]
pub struct MediaStats {
    pub by_image_type: HashMap<ImageType, usize>,
    pub by_video_type: HashMap<VideoType, usize>,
    pub total_images: usize,
    pub total_videos: usize,
    pub excluded_screenshots: usize,
    pub excluded_logos: usize,
}

impl MediaStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_image(&mut self, img_type: ImageType, included: bool) {
        self.total_images += 1;
        if included {
            *self.by_image_type.entry(img_type).or_insert(0) += 1;
        } else {
            match img_type {
                ImageType::Screenshot | ImageType::Thumbnail => self.excluded_screenshots += 1,
                ImageType::Logo | ImageType::Icon => self.excluded_logos += 1,
                _ => {}
            }
        }
    }

    pub fn record_video(&mut self, video_type: VideoType) {
        self.total_videos += 1;
        *self.by_video_type.entry(video_type).or_insert(0) += 1;
    }

    pub fn log_summary(&self, provider: &str) {
        tracing::info!(
            provider=%provider,
            total_images=%self.total_images,
            total_videos=%self.total_videos,
            excluded_screenshots=%self.excluded_screenshots,
            excluded_logos=%self.excluded_logos,
            "media ingestion summary"
        );

        for (img_type, count) in &self.by_image_type {
            tracing::debug!(
                provider=%provider,
                media_type=%img_type.to_media_type(),
                count=%count,
                "image type distribution"
            );
        }

        for (video_type, count) in &self.by_video_type {
            tracing::debug!(
                provider=%provider,
                media_type=%video_type.to_media_type(),
                count=%count,
                "video type distribution"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_image_from_url_screenshots_excluded() {
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/screenshot_01.jpg", None),
            ImageType::Screenshot
        );
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/screenshots/img.png", None),
            ImageType::Screenshot
        );
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/thumb.jpg", None),
            ImageType::Thumbnail
        );
    }

    #[test]
    fn test_classify_image_from_url_covers_allowed() {
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/cover.jpg", None),
            ImageType::Cover
        );
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/boxart.png", None),
            ImageType::Cover
        );
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/img.jpg", Some("BoxArt")),
            ImageType::Cover
        );
    }

    #[test]
    fn test_classify_image_from_url_hero_background_artwork() {
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/hero.jpg", None),
            ImageType::Hero
        );
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/background.jpg", None),
            ImageType::Background
        );
        assert_eq!(
            classify_image_from_url("https://cdn.example.com/artwork.jpg", None),
            ImageType::Artwork
        );
    }

    #[test]
    fn test_image_type_priority() {
        assert!(ImageType::Cover.priority() < ImageType::Screenshot.priority());
        assert!(ImageType::Hero.priority() < ImageType::Screenshot.priority());
        assert!(ImageType::Background.priority() < ImageType::Screenshot.priority());
        assert!(ImageType::Cover.priority() < ImageType::Background.priority());
    }

    #[test]
    fn test_should_include_filtering() {
        assert!(ImageType::Cover.should_include(false));
        assert!(ImageType::Hero.should_include(false));
        assert!(!ImageType::Screenshot.should_include(false));
        assert!(ImageType::Screenshot.should_include(true));
        assert!(!ImageType::Logo.should_include(true)); // Always excluded
    }

    #[test]
    fn test_classify_video_from_url() {
        assert_eq!(
            classify_video_from_url("https://cdn.example.com/trailer.mp4", None),
            VideoType::Trailer
        );
        assert_eq!(
            classify_video_from_url("https://cdn.example.com/video.mp4", Some("Trailer")),
            VideoType::Trailer
        );
        assert_eq!(
            classify_video_from_url("https://cdn.example.com/gameplay.mp4", None),
            VideoType::Gameplay
        );
    }
}
