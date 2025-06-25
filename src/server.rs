use crate::commands::execute_command;
use crate::database::{create_database, create_database_with_data, Database};
use crate::protocol::parse_command;
use crate::auth::{AuthConfig, ClientAuth};
use crate::persistence::MmapPersistence;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
    pub fn new(host: String, port: u16, password: Option<String>, dbfilename: String) -> Self {
        let auth_config = Arc::new(AuthConfig::new(password));
        let persistence = MmapPersistence::new(dbfilename);

        // Try to load existing database
        let database = match persistence.load_database() {
            Ok(db) => create_database_with_data(db),
            Err(e) => {
                eprintln!("Failed to load database: {}", e);
                create_database()
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
        println!("Ready to accept connections");

        // Start background save task
        let db_clone = Arc::clone(&self.database);
        let persistence_clone = MmapPersistence::new(self.persistence.file_path.clone());
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60)); // Save every minute
            loop {
                interval.tick().await;
                let db = db_clone.read().unwrap();
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
    auth_config: Arc<AuthConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut client_auth = ClientAuth::new(auth_config);

    // Send welcome message
    writer.write_all(b"Welcome to Redis-clone!\r\n").await?;

    if client_auth.requires_auth() {
        writer.write_all(b"Authentication required. Use AUTH <password>\r\n").await?;
    }

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
                        let response = execute_command(Arc::clone(&database), command, &mut client_auth);

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