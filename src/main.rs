mod server;
mod database;
mod commands;
mod protocol;
mod data_types;
mod auth;

use clap::Parser;
use server::Server;

#[derive(Parser)]
#[command(name = "redis-clone")]
#[command(about = "A Redis-like database implementation in Rust")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value = "6379")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting Redis-clone server on {}:{}", args.host, args.port);

    let server = Server::new(args.host, args.port);
    server.run().await?;

    Ok(())
}