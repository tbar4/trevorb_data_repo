use reqwest;
use serde_json::Value;
use anyhow::Result;
use std::path::Path;
use crate::utils::error::ApiError;

pub async fn fetch_data(
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