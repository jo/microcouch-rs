mod database;

use crate::database::Database;

use clap::Parser;
use url::Url;

#[derive(Parser)]
#[clap(name = "microcouch")]
#[clap(about = "Small embedded CouchDB replicating database.", long_about = None)]
pub struct Args {
    /// Source database url
    #[clap(index = 1, required = true, value_name = "SOURCE")]
    source: String,

    /// Target database url
    #[clap(index = 2, required = true, value_name = "TARGET")]
    target: String,
}

pub struct Microcouch {
    args: Args,
}

impl Microcouch {
    pub fn new(args: Args) -> Self {
        Self { args }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let source_url = Url::parse(&self.args.source).expect("invalid source url");
        let source = Database::new(source_url);

        let target_url = Url::parse(&self.args.target).expect("invalid target url");
        let target = Database::new(target_url);

        match target.pull(&source).await? {
            Some(stats) => {
                println!("Replication complete: {:?}", stats);
            }
            None => {
                println!("Error, no result returned.");
            }
        };

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Microcouch::new(args);
    client.run().await
}
