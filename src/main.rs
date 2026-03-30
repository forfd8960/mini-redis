use anyhow::Result;
use mini_redis::server::{INFO, run};

#[tokio::main]
async fn main() -> Result<()> {
    println!("{}", INFO);
    run().await
}
