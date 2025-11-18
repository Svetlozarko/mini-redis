use crate::commands::execute_command;
use crate::database::{create_database_with_memory_config, create_database_with_data, Database};
use crate::protocol::parse_command;
use crate::auth::{AuthConfig, ClientAuth};
use crate::persistence_clean::MmapPersistence;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval, Duration};

pub struct Server {
    host: String,
    port: u16,
    database: Database,
    auth_config: Arc<AuthConfig>,
    persistence: Arc<MmapPersistence>,
}

impl Server {
    pub fn new(
        host: String,
        port: u16,
        password: Option<String>,
        dbfilename: String,
        max_memory: Option<usize>,
        eviction_policy: String
    ) -> Self {
        let auth_config = Arc::new(AuthConfig::new(password));
        let persistence = Arc::new(MmapPersistence::new(dbfilename));

        let database = match persistence.load_database() {
            Ok(mut db) => {
                db.memory_manager = crate::memory::MemoryManager::new(max_memory, eviction_policy);
                create_database_with_data(db)
            },
            Err(e) => {
                eprintln!("Failed to load database: {}", e);
                create_database_with_memory_config(max_memory, eviction_policy)
            }
        };

        Self {
            host,
            port,
            database,
            auth_config,
            persistence,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;

        println!("Redis-clone server listening on {}", addr);

        {
            let db = self.database.read().await;
            let memory_info = db.get_memory_info();
            if let Some(max_mem) = memory_info.get("maxmemory_human") {
                if max_mem != "unlimited" {
                    println!("Memory limit: {}", max_mem);
                    println!("Eviction policy: {}", memory_info.get("maxmemory_policy").unwrap_or(&"unknown".to_string()));
                }
            }
            println!("Current memory usage: {}", memory_info.get("used_memory_human").unwrap_or(&"unknown".to_string()));
        }

        println!("Ready to accept connections");

        let db_clone = Arc::clone(&self.database);
        let persistence_clone = Arc::clone(&self.persistence);
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let db = db_clone.read().await;
                if let Err(e) = persistence_clone.save_database(&db) {
                    eprintln!("Background save failed: {}", e);
                }
            }
        });

        loop {
            let (socket, addr) = listener.accept().await?;
            let db = Arc::clone(&self.database);
            let auth_config = Arc::clone(&self.auth_config);

            println!("New client connected: {}", addr);

            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, db, auth_config).await {
                    eprintln!("Error handling client: {}", e);
                }
            });
        }
    }
}

async fn handle_client(
    mut socket: TcpStream,
    database: Database,
    auth_config: Arc<AuthConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut client_auth = ClientAuth::new(auth_config);
    let mut buffer = String::new();

    writer.write_all(b"Welcome to Redis-clone!\r\n").await?;
    writer.flush().await?;

    loop {
        buffer.clear();

        match reader.read_line(&mut buffer).await? {
            0 => {
                // Client disconnected
                break;
            },
            _ => {
                let command_str = buffer.trim();
                println!("[v0] Received raw input: {:?}", buffer);
                println!("[v0] Trimmed command: {:?}", command_str);

                if command_str.is_empty() {
                    continue;
                }

                match parse_command(command_str) {
                    Ok(command) => {
                        println!("[v0] Parsed command: {:?}", command);
                        let is_quit = matches!(command, crate::commands::Command::Quit);
                        let response = execute_command(
                            Arc::clone(&database),
                            command,
                            &mut client_auth,
                            None
                        ).await;

                        writer.write_all(response.as_bytes()).await?;
                        writer.write_all(b"\r\n").await?;
                        writer.flush().await?;

                        if is_quit {
                            break;
                        }
                    },
                    Err(error) => {
                        println!("[v0] Parse error: {}", error);
                        writer.write_all(error.as_bytes()).await?;
                        writer.write_all(b"\r\n").await?;
                        writer.flush().await?;
                    }
                }
            }
        }
    }

    Ok(())
}
