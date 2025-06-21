mod server;
mod db;
mod commands;

use server::run_server;
use db::Database;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Database::new();
    run_server("127.0.0.1:6379", db).await
}
