use reqwest;
use reqwest::Client;
use serde_json::Value;
use anyhow::Result;
use std::path::Path;
use crate::utils::error::ApiError;
use super::loader::DataSink;

#[derive(Debug)]
pub struct ApiClient {
    client: Client,
    base_url: String,
}

impl ApiClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    pub async fn fetch_data(
        &self,
        endpoint: &str,
        params: Option<&[(String, String)]>,
        sink: Option<&str>,
        data_sink: Option<&DataSink>,
    ) -> Result<()> {
        let url = self.build_url(endpoint, params)?;
        println!("Fetching data from endpoint: {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ApiError::RequestError(format!(
                "HTTP request failed with status: {}",
                response.status()
            ))
            .into());
        }

        let body: Value = response.json().await?;
        
        // Save data using the provided data sink or fallback to file system
        if let Some(sink_impl) = data_sink {
            sink_impl.save(&body, sink).await?;
        } else {
            self.handle_response(body, sink).await?;
        }
        
        Ok(())
    }

    fn build_url(&self, endpoint: &str, params: Option<&[(String, String)]>) -> Result<String> {
        let full_url = if endpoint.starts_with("http") {
            endpoint.to_string()
        } else {
            // Ensure base_url ends with a slash and endpoint doesn't start with one
            let base = if self.base_url.ends_with('/') {
                self.base_url.clone()
            } else {
                format!("{}/", self.base_url)
            };
            
            let path = if endpoint.starts_with('/') {
                endpoint[1..].to_string()
            } else {
                endpoint.to_string()
            };
            
            format!("{}{}", base, path)
        };

        let mut url = reqwest::Url::parse(&full_url)
            .map_err(|e| ApiError::RequestError(format!("Invalid URL: {}", e)))?;

        if let Some(params) = params {
            for (key, value) in params {
                url.query_pairs_mut().append_pair(key, value);
            }
        }

        Ok(url.to_string())
    }

    async fn handle_response(&self, body: Value, output: Option<&str>) -> Result<()> {
        if let Some(path) = output {
            self.save_to_file(&body, path).await?;
            println!("Data saved to: {}", path);
        } else {
            println!("{}", serde_json::to_string_pretty(&body)?);
        }
        Ok(())
    }

    async fn save_to_file(&self, body: &Value, path: &str) -> Result<()> {
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
}