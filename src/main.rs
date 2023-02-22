mod database;
mod http_database;
mod replicator;
mod sqlite_database;

use crate::http_database::HttpDatabase;
use crate::replicator::replicate;
use crate::sqlite_database::SqliteDatabase;

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

    /// Concurrency
    #[clap(short, long, required = true, value_name = "N")]
    concurrency: usize,

    /// Batch size
    #[clap(short, long, required = true, value_name = "N")]
    batch_size: usize,
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
        let source = HttpDatabase::new(source_url);

        if self.args.target.starts_with("https://") {
            let target_url = Url::parse(&self.args.target).expect("invalid target url");
            let target = HttpDatabase::new(target_url);

            replicate(
                &source,
                &target,
                self.args.concurrency,
                self.args.batch_size,
            )
            .await;
        } else {
            let target = SqliteDatabase::new(&self.args.target);

            replicate(
                &source,
                &target,
                self.args.concurrency,
                self.args.batch_size,
            )
            .await;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Microcouch::new(args);
    client.run().await
}
