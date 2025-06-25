mod database;
mod commands;
mod protocol;
mod data_types;
mod server;
mod auth;
mod persistence;

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

    #[arg(long)]
    password: Option<String>,

    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting Redis-clone server on {}:{}", args.host, args.port);

    if args.password.is_some() {
        println!("Password protection enabled");
    }

    let server = Server::new(args.host, args.port, args.password, args.dbfilename);
    server.run().await?;

    Ok(())
}