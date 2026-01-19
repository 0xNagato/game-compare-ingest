use strsim::jaro_winkler;

/// Minimum similarity score (Jaro-Winkler) required for two platform names
/// to be treated as equivalent.
pub const MIN_PLATFORM_SIMILARITY: f64 = 0.80;

/// Canonicalized platform key used for fuzzy comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformKey {
    normalized: String,
    numeric_sig: Option<String>,
}

impl PlatformKey {
    /// Build a normalized comparison key from a raw platform label.
    ///
    /// Normalization steps:
    /// - trim whitespace
    /// - lowercase and remove punctuation/whitespace
    /// - remove PAL/NTSC/JPY style region prefixes
    /// - expand PSx abbreviations to "playstationx"
    /// - record the numeric signature so PS4 ≠ PS5
    pub fn new(raw: &str) -> Self {
        let trimmed = raw.trim().to_ascii_lowercase();
        let without_prefix = strip_region_prefixes(&trimmed);
        let alnum_only: String = without_prefix
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();

        let expanded = expand_common_abbreviations(&alnum_only);
        let digits: String = expanded.chars().filter(|c| c.is_ascii_digit()).collect();
        let numeric_sig = if digits.is_empty() {
            None
        } else {
            Some(digits)
        };

        Self {
            normalized: expanded,
            numeric_sig,
        }
    }

    /// The normalization output as a lowercase ASCII alphanumeric token.
    pub fn normalized(&self) -> &str {
        &self.normalized
    }

    /// Optional numeric signature extracted from the normalized form (e.g., "5" for PS5).
    pub fn numeric_signature(&self) -> Option<&str> {
        self.numeric_sig.as_deref()
    }

    /// Whether the numeric signatures are compatible (both empty or equal).
    pub fn numeric_compatible(&self, other: &Self) -> bool {
        match (&self.numeric_sig, &other.numeric_sig) {
            (None, None) => true,
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }

    /// Jaro-Winkler similarity between two normalized keys.
    pub fn similarity(&self, other: &Self) -> f64 {
        jaro_winkler(self.normalized(), other.normalized())
    }
}

fn strip_region_prefixes(input: &str) -> &str {
    const PREFIXES: [&str; 3] = ["pal", "ntsc", "jpy"];
    for prefix in PREFIXES {
        for sep in ["-", "_", " "] {
            let candidate = format!("{prefix}{sep}");
            if input.starts_with(&candidate) {
                return input[candidate.len()..].trim();
            }
        }
    }
    input
}

fn expand_common_abbreviations(input: &str) -> String {
    if let Some(rest) = input.strip_prefix("ps") {
        if rest.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            return format!("playstation{rest}");
        }
    }
    input.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_region_prefixes_and_punctuation() {
        let a = PlatformKey::new("PAL-PlayStation®5");
        let b = PlatformKey::new("PlayStation 5");
        assert_eq!(a.numeric_signature(), Some("5"));
        assert_eq!(b.numeric_signature(), Some("5"));
        assert!(a.numeric_compatible(&b));
        assert!(a.similarity(&b) >= MIN_PLATFORM_SIMILARITY);
    }

    #[test]
    fn expands_ps_abbreviation() {
        let a = PlatformKey::new("PS5");
        let b = PlatformKey::new("PlayStation 5");
        assert!(a.numeric_compatible(&b));
        assert!(a.similarity(&b) >= MIN_PLATFORM_SIMILARITY);
    }

    #[test]
    fn preserves_numeric_distinctions() {
        let ps4 = PlatformKey::new("PlayStation 4");
        let ps5 = PlatformKey::new("PlayStation 5");
        assert!(!ps4.numeric_compatible(&ps5));
        assert!(ps4.similarity(&ps5) < 1.0);
    }
}
