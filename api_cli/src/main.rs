use anyhow::Result;
use clap::Parser;
use reqwest;
use std::fs;
use tokio;
use std::io::Write;

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
        #[clap(short, long, value_delimiter=',', value_parser=parse_key_value)]
        params: Option<Vec<(String, String)>>,

        /// Output file path
        #[clap(short, long)]
        output: Option<String>,
    },
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
        } => {
            // Build URL with query parameters
            let url = if let Some(params) = params {
                let mut query_params = String::new();
                for (i, (key, value)) in params.iter().enumerate() {
                    if i > 0 {
                        query_params.push('&');
                    }
                    query_params.push_str(&format!("{}={}", key, value));
                }
                format!("{}?{}", endpoint, query_params)
            } else {
                endpoint
            };

            println!("Fetching data from endpoint: {}", url);

            // Make HTTP request
            let response = reqwest::get(&url).await?;
            let body = response.json::<serde_json::Value>().await?;
            let results = body["results"].as_array().unwrap();
    
            
            if let Some(path) = output {
                println!("Saving to: {}", &path);
                let path_ref = std::path::Path::new(&path);
                if path_ref.exists() {
                    println!("File already exists...\nOverwriting...");
                    let mut file = std::fs::OpenOptions::new()
                            .create(true)        // Create file if it doesn't exist
                            .append(true)        // Open in append mode
                            .open(&path)?;
                    for j in results.iter() {
                        let jp = serde_json::to_string_pretty(j)?;
                        println!("{file:?}");
                        writeln!(file, "{}", jp)?;
                    }
                } else {
                    tokio::fs::File::create(path_ref).await?;
                    //tokio::task::spawn_blocking(move || fs::write(&path, &json)).await??;
                }
            } else {
                println!("{}", body);
            }
        }
    }

    Ok(())
}
