mod database;
mod commands;
mod protocol;
mod data_types;
mod server;
mod auth;
mod persistence;
mod persistence_clean;
mod memory;
mod memory;

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

    #[arg(long, help = "Maximum memory usage (e.g., 100MB, 1GB, 512KB)")]
    maxmemory: Option<String>,

    #[arg(long, default_value = "allkeys-lru", help = "Memory eviction policy: noeviction, allkeys-lru, allkeys-lfu, volatile-lru, volatile-lfu, allkeys-random, volatile-random")]
    maxmemory_policy: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting Redis-clone server on {}:{}", args.host, args.port);

    if args.password.is_some() {
        println!("Password protection enabled");
    }

    // Parse memory limit
    let memory_limit = if let Some(max_mem) = &args.maxmemory {
        match parse_memory_size(max_mem) {
            Ok(size) => {
                println!("Memory limit set to: {} bytes ({})", size, max_mem);
                Some(size)
            },
            Err(e) => {
                eprintln!("Invalid memory size '{}': {}", max_mem, e);
                return Err(e);
            }
        }
    } else {
        println!("No memory limit set");
        None
    };

    // Validate eviction policy
    let eviction_policy = match args.maxmemory_policy.as_str() {
        "noeviction" | "allkeys-lru" | "allkeys-lfu" | "volatile-lru" |
        "volatile-lfu" | "allkeys-random" | "volatile-random" => args.maxmemory_policy.clone(),
        _ => {
            eprintln!("Invalid eviction policy: {}", args.maxmemory_policy);
            return Err("Invalid eviction policy".into());
        }
    };

    println!("Memory eviction policy: {}", eviction_policy);

    let server = Server::new(
        args.host,
        args.port,
        args.password,
        args.dbfilename,
        memory_limit,
        eviction_policy
    );
    server.run().await?;

    Ok(())
}

fn parse_memory_size(size_str: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let size_str = size_str.to_uppercase();

    if let Some(number_part) = size_str.strip_suffix("KB") {
        Ok(number_part.parse::<usize>()? * 1024)
    } else if let Some(number_part) = size_str.strip_suffix("MB") {
        Ok(number_part.parse::<usize>()? * 1024 * 1024)
    } else if let Some(number_part) = size_str.strip_suffix("GB") {
        Ok(number_part.parse::<usize>()? * 1024 * 1024 * 1024)
    } else if let Some(number_part) = size_str.strip_suffix("B") {
        Ok(number_part.parse::<usize>()?)
    } else {
        // Assume bytes if no suffix
        Ok(size_str.parse::<usize>()?)
    }
}
