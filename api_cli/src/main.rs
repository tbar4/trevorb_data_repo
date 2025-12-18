use anyhow::Result;
use clap::Parser;
use tokio;
mod extractor;
mod utils;
use extractor::api::fetch_data;
use utils::error::parse_key_value;

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


