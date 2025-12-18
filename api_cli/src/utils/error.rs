#![allow(unused)]

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("API request failed: {0}")]
    RequestError(String),
    #[error("Failed to parse JSON response: {0}")]
    JsonParseError(String),
    #[error("Invalid output path: {0}")]
    OutputPathError(String),
}

pub fn parse_key_value(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("Invalid format {}, expected KEY=VALUE", s))
}