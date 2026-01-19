use std::collections::HashMap;

use serde_json::Value;

/// Strategy describing how to interpret a rating field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RatingStrategy {
    /// Value already represents a 0-5 float.
    ZeroToFive,
    /// Value is 0-100 and must be normalized to 0-5.
    ZeroToHundred,
    /// Value is a string like "4.5 stars" that should be parsed into 0-5.
    StarString,
}

#[derive(Debug, Clone)]
pub struct RatingAlias {
    pub field: &'static str,
    pub strategy: RatingStrategy,
}

impl RatingAlias {
    pub const fn new(field: &'static str, strategy: RatingStrategy) -> Self {
        Self { field, strategy }
    }
}

/// Configuration-driven mapper translating provider payloads into a unified 0-5 rating.
#[derive(Debug, Default, Clone)]
pub struct RatingMapper {
    aliases: HashMap<String, RatingAlias>,
}

impl RatingMapper {
    /// Build a mapper seeded with the default aliases described in the specs.
    pub fn with_defaults() -> Self {
        Self::default()
            .register(
                "provider_a",
                RatingAlias::new("user_ratings", RatingStrategy::ZeroToFive),
            )
            .register(
                "provider_b",
                RatingAlias::new("aggregated_rating", RatingStrategy::ZeroToHundred),
            )
            .register(
                "provider_c",
                RatingAlias::new("product_star_rating", RatingStrategy::StarString),
            )
            .register(
                "rawg",
                RatingAlias::new("metacritic", RatingStrategy::ZeroToHundred),
            )
    }

    /// Register or override an alias for a provider key.
    pub fn register(mut self, provider_key: impl Into<String>, alias: RatingAlias) -> Self {
        self.aliases
            .insert(provider_key.into().to_ascii_lowercase(), alias);
        self
    }

    /// Attempt to map the given payload into a normalized 0-5 rating.
    pub fn map(&self, provider_key: &str, payload: &Value) -> Option<f32> {
        let alias = self.aliases.get(&provider_key.to_ascii_lowercase())?;
        let value = payload.get(alias.field)?;
        let rating = match alias.strategy {
            RatingStrategy::ZeroToFive => value.as_f64()?,
            RatingStrategy::ZeroToHundred => value.as_f64()? / 20.0,
            RatingStrategy::StarString => parse_star_string(value.as_str()?)?,
        } as f32;
        if (0.0..=5.0).contains(&rating) {
            Some(rating)
        } else {
            None
        }
    }
}

fn parse_star_string(input: &str) -> Option<f64> {
    let digits: String = input
        .chars()
        .take_while(|c| *c != ' ')
        .filter(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    digits.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(field: &str, value: Value) -> Value {
        serde_json::json!({ field: value })
    }

    #[test]
    fn maps_default_aliases() {
        let mapper = RatingMapper::with_defaults();

        assert_eq!(
            mapper.map("provider_a", &payload("user_ratings", Value::from(4.2))),
            Some(4.2)
        );

        assert_eq!(
            mapper.map("provider_b", &payload("aggregated_rating", Value::from(86))),
            Some(4.3)
        );

        assert_eq!(
            mapper.map(
                "provider_c",
                &payload("product_star_rating", Value::from("4.5 stars")),
            ),
            Some(4.5)
        );
    }

    #[test]
    fn maps_rawg_metacritic() {
        let mapper = RatingMapper::with_defaults();
        assert_eq!(
            mapper.map("rawg", &payload("metacritic", Value::from(80))),
            Some(4.0)
        );
    }

    #[test]
    fn rejects_out_of_bounds_values() {
        let mapper = RatingMapper::with_defaults();
        assert!(mapper
            .map("provider_a", &payload("user_ratings", Value::from(7.5)))
            .is_none());
    }

    #[test]
    fn allows_custom_alias() {
        let mapper = RatingMapper::with_defaults().register(
            "custom",
            RatingAlias::new("score", RatingStrategy::ZeroToHundred),
        );
        assert_eq!(
            mapper.map("custom", &payload("score", Value::from(50))),
            Some(2.5)
        );
    }
}
