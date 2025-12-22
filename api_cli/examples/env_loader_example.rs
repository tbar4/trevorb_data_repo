use anyhow::Result;
use dotenv::dotenv;
use std::env;

/// Loads environment variables from a .env file
fn load_env() -> Result<()> {
    dotenv().ok();
    Ok(())
}

/// Gets an environment variable or returns a default value
fn get_env_var_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Gets an environment variable or returns None if it's not set
fn get_env_var(key: &str) -> Option<String> {
    env::var(key).ok()
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    load_env()?;

    // Get an environment variable or use a default value
    let api_base_url = get_env_var_or_default("API_BASE_URL", "https://api.example.com");
    println!("API Base URL: {}", api_base_url);

    // Get an environment variable or None if it doesn't exist
    if let Some(api_key) = get_env_var("API_KEY") {
        println!("API Key: {}", api_key);
    } else {
        println!("No API Key found in environment");
    }

    Ok(())
}