use crate::commands::execute_command;
use crate::database::{create_database, Database};
use crate::protocol::parse_command;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub struct Server {
    host: String,
    port: u16,
    database: Database,
}

impl Server {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            database: create_database(),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;

        println!("Redis-clone server listening on {}", addr);
        println!("Ready to accept connections");

        loop {
            let (socket, addr) = listener.accept().await?;
            let db = Arc::clone(&self.database);

            println!("New client connected: {}", addr);

            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, db).await {
                    eprintln!("Error handling client {}: {}", addr, e);
                }
                println!("Client {} disconnected", addr);
            });
        }
    }
}

async fn handle_client(
    mut socket: TcpStream,
    database: Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    // Send welcome message
    writer.write_all(b"Welcome to Redis-clone!\r\n").await?;
    writer.write_all(b"redis-clone> ").await?;
    writer.flush().await?;

    loop {
        line.clear();

        match reader.read_line(&mut line).await? {
            0 => break, // Client disconnected
            _ => {
                let command_str = line.trim();

                if command_str.is_empty() {
                    writer.write_all(b"redis-clone> ").await?;
                    writer.flush().await?;
                    continue;
                }

                match parse_command(command_str) {
                    Ok(command) => {
                        let is_quit = matches!(command, crate::commands::Command::Quit);
                        let response = execute_command(Arc::clone(&database), command);

                        writer.write_all(response.as_bytes()).await?;
                        writer.write_all(b"\r\n").await?;

                        if is_quit {
                            writer.flush().await?;
                            break;
                        }
                    },
                    Err(error) => {
                        writer.write_all(error.as_bytes()).await?;
                        writer.write_all(b"\r\n").await?;
                    }
                }

                writer.write_all(b"redis-clone> ").await?;
                writer.flush().await?;
            }
        }
    }

    Ok(())
}