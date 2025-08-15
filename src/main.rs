use anyhow::Result;

use cli::parse_cli;

mod cli;
mod config;
mod progress;
mod redis_ops;

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_cli();

    redis_ops::run_dump(config).await
}
