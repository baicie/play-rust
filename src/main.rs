// src/main.rs
mod config;
mod job;
mod sinks;
mod sources;
mod transforms;

use config::Config;
use job::SyncJob;
use sinks::MySQLSink;
use sources::PostgresSource;
use transforms::IdentityTransformer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load_from_file("config.json")?;

    let source = PostgresSource::new(config.source_db.url, config.source_db.table);
    let sink = MySQLSink::new(config.target_db.url, config.target_db.table);
    let transformer = IdentityTransformer;

    let job = SyncJob::new(source, transformer, sink);
    job.run().await?;

    Ok(())
}
