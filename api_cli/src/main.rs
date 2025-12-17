use anyhow::Result;
use clap::Parser;

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
        
        /// Output file path
        #[clap(short, long)]
        output: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Fetch { endpoint, output } => {
            println!("Fetching data from endpoint: {}", endpoint);
            if let Some(path) = output {
                println!("Will save to: {}", path);
            }
        }
    }
    
    Ok(())
}
