use anyhow::Result;
use clap::Parser;
mod extractor;
mod utils;
use extractor::api::ApiClient;
use extractor::loader::{DataSink, LoaderType, LocalSink, S3Sink, SqliteSink, PostgresSink};
use utils::error::parse_key_value;
use utils::env_loader::load_env;



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
        /// Base URL for the API
        #[clap(long, default_value = "https://api.spaceflightnewsapi.net/v4")]
        base_url: String,
        
        /// Endpoint to fetch data from
        #[clap(short, long)]
        endpoint: String,

        /// Query Params for Endpoint
        #[clap(short, long, value_delimiter = ',', value_parser = parse_key_value)]
        params: Option<Vec<(String, String)>>,

        /// Output destination type (local, s3, sqlite, postgres)
        #[clap(long, value_enum, default_value = "local")]
        output_type: LoaderType,

        /// Connection string or path for the output destination
        #[clap(long)]
        output_destination: Option<String>,

        /// Table name for database outputs
        #[clap(long)]
        table_name: Option<String>,

        /// Output file path or key (for local/S3)
        #[clap(short, long)]
        sink: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    load_env()?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch {
            base_url,
            endpoint,
            params,
            output_type,
            output_destination,
            table_name: _,
            sink,
        } => {
            // Create the appropriate data sink based on the output type
            let data_sink = match output_type {
                LoaderType::Local => {
                    Some(DataSink::Local(LocalSink))
                },
                LoaderType::S3 => {
                    if let Some(bucket) = &output_destination {
                        Some(DataSink::S3(S3Sink::new(bucket.clone())))
                    } else {
                        println!("S3 bucket name required for S3 output");
                        None
                    }
                },
                LoaderType::Sqlite => {
                    if let Some(connection_string) = &output_destination {
                        Some(DataSink::Sqlite(SqliteSink::new(connection_string.clone())))
                    } else {
                        println!("SQLite connection string required for SQLite output");
                        None
                    }
                },
                LoaderType::Postgres => {
                    if let Some(connection_string) = &output_destination {
                        Some(DataSink::Postgres(PostgresSink::new(connection_string.clone())))
                    } else {
                        println!("PostgreSQL connection string required for PostgreSQL output");
                        None
                    }
                },
            };

            let client = ApiClient::new(base_url);
            client.fetch_data(&endpoint, params.as_deref(), sink.as_deref(), data_sink.as_ref()).await?
        }
    }


    Ok(())
}

