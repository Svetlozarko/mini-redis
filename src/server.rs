use crate::commands::execute_command;
use crate::database::{create_database_with_memory_config, create_database_with_data, Database};
use crate::protocol::parse_command;
use crate::auth::{AuthConfig, ClientAuth};
use crate::persistence_clean::MmapPersistence;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval, Duration};

pub struct Server {
    host: String,
    port: u16,
    database: Database,
    auth_config: Arc<AuthConfig>,
    persistence: MmapPersistence,
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
        let persistence = MmapPersistence::new(dbfilename);

        // Try to load existing database
        let database = match persistence.load_database() {
            Ok(mut db) => {
                // Update memory configuration for loaded database
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
        let persistence_clone = MmapPersistence::new(self.persistence.file_path.clone());
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60)); // Save every minute
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
fn extract_one_command(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
        let command = &buffer[..pos];
        let remaining = &buffer[pos + 1..];
        Some((command, remaining))
    } else {
        None
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

    writer.write_all(b"Welcome to Redis-clone!\r\n").await?;
    
    {
        let db = database.read().await;
        let memory_info = db.get_memory_info();
        if let Some(max_mem) = memory_info.get("maxmemory_human") {
            if max_mem != "unlimited" {
                let usage = memory_info
                    .get("used_memory_percentage")
                    .cloned()
                    .unwrap_or("0%".to_string());

                writer
                    .write_all(
                        format!(
                            "Memory: {} used of {} ({})\r\n",
                            memory_info.get("used_memory_human").unwrap_or(&"0B".to_string()),
                            max_mem,
                            usage
                        )
                            .as_bytes(),
                    )
                    .await?;
            }
        }
    }

    if client_auth.requires_auth() {
        writer
            .write_all(b"Authentication required. Use AUTH <password>\r\n")
            .await?;
    }

    writer.write_all(b"redis-clone> ").await?;
    writer.flush().await?;

    let mut buffer = Vec::new();
    let mut start = 0;

    loop {
        let n = reader.read_buf(&mut buffer).await?;
        if n == 0 {
            break;
        }

        while let Some(pos) = buffer[start..].iter().position(|&b| b == b'\n') {
            let command_bytes = &buffer[start..start + pos];
            let command_str = std::str::from_utf8(command_bytes)?.trim();
            start += pos + 1; // move past the newline
            let command = parse_command(command_str)?;
            let response = execute_command(Arc::clone(&database), command, &mut client_auth).await;

           
            let response_json = serde_json::to_string(&response)?;
            writer.write_all(response_json.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
        
        buffer.drain(..start);
        start = 0;

        writer.flush().await?;
    }

    Ok(())
}

