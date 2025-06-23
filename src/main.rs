mod server;
mod db;
mod commands;

use db::Database;
use server::run_server;
use std::sync::Arc;
use tokio::signal;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::new();

    // Try loading snapshot from disk at startup
    let _ = db.load_snapshot("dump.json");

    let db_clone = Arc::clone(&db);

    // Spawn a task to save snapshot on Ctrl+C
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen for ctrl_c");
        println!("Received Ctrl+C, saving snapshot...");
        if let Err(e) = db_clone.save_snapshot("dump.json") {
            eprintln!("Error saving snapshot: {:?}", e);
        }
        std::process::exit(0);
    });

    // Run the server, pass db instance
    run_server("127.0.0.1:6379", db).await
}
