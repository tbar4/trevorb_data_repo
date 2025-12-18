use anyhow::Result;
use clap::Parser;
use reqwest;
use serde_json::Value;
use std::io::Write;
use std::path::Path;
use tokio;

#[derive(Debug, Parser)]
#[clap(name = "api_cli", about = "A CLI tool for API operations")]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Parser)]
enum Commands {
    /// Fetch data from Spaceflight News API
    Fetch {
        /// Endpoint to fetch data from
        #[clap(short, long)]
        endpoint: String,

        /// Query Params for Endpoint
        #[clap(short, long, value_delimiter = ',', value_parser = parse_key_value)]
        params: Option<Vec<(String, String)>>,

        /// Output file path
        #[clap(short, long)]
        output: Option<String>,
    },
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error("API request failed: {0}")]
    RequestError(String),
    #[error("Failed to parse JSON response: {0}")]
    JsonParseError(String),
    #[error("Invalid output path: {0}")]
    OutputPathError(String),
}

fn parse_key_value(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("Invalid format {}, expected KEY=VALUE", s))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch {
            endpoint,
            params,
            output,
        } => fetch_data(&endpoint, params.as_deref(), output.as_deref()).await?
    }


    Ok(())
}

async fn fetch_data(
    endpoint: &str,
    params: Option<&[(String, String)]>,
    output: Option<&str>,
) -> Result<()> {
    // Build URL with query parameters
    let url = build_url(endpoint, params)?;
    println!("Fetching data from endpoint: {}", url);

    // Make HTTP request
    let response = reqwest::get(&url).await?;

    if !response.status().is_success() {
        return Err(ApiError::RequestError(format!(
            "HTTP request failed with status: {}",
            response.status()
        ))
        .into());
    }

    let body: Value = response.json().await?;
    handle_response(body, output).await
}

fn build_url(endpoint: &str, params: Option<&[(String, String)]>) -> Result<String> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|e| ApiError::RequestError(format!("Invalid URL: {}", e)))?;

    if let Some(params) = params {
        for (key, value) in params {
            url.query_pairs_mut().append_pair(key, value);
        }
    }

    Ok(url.to_string())
}

async fn handle_response(body: Value, output: Option<&str>) -> Result<()> {
    if let Some(path) = output {
        save_to_file(&body, path).await?;
        println!("Data saved to: {}", path);
    } else {
        println!("{}", serde_json::to_string_pretty(&body)?);
    }
    Ok(())
}

async fn save_to_file(body: &Value, path: &str) -> Result<()> {
    let path_ref = Path::new(path);
    
    // Create parent directories if they don't exist
    if let Some(parent) = path_ref.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    
    // Convert JSON to string
    let json_string = serde_json::to_string_pretty(body)?;
    
    // Write to file
    tokio::fs::write(path_ref, json_string).await?;
    
    Ok(())
}
