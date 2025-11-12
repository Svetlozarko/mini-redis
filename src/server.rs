use crate::commands::{execute_command, Command};
use crate::database::{create_database_with_memory_config, create_database_with_data, Database};
use crate::protocol::parse_command;
use crate::auth::{AuthConfig, ClientAuth};
use crate::persistence_clean::MmapPersistence;
use crate::pub_sub::{create_pubsub_manager, PubSubManager, PubSubMessage};
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
    pubsub_manager: PubSubManager,
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
            pubsub_manager: create_pubsub_manager(),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;

        println!("Redis-clone server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            let db = Arc::clone(&self.database);
            let auth_config = Arc::clone(&self.auth_config);
            let pubsub_manager = Arc::clone(&self.pubsub_manager);

            println!("New client connected: {}", addr);

            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, db, auth_config, pubsub_manager).await {
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
    pubsub_manager: PubSubManager,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut client_auth = ClientAuth::new(auth_config);

    writer.write_all(b"Welcome to Redis-clone!\r\n").await?;

    let mut buffer = String::new();
    let mut subscriber_mode = false;
    let mut subscriber_id: Option<usize> = None;
    let mut message_receiver: Option<tokio::sync::mpsc::UnboundedReceiver<PubSubMessage>> = None;

    loop {
        tokio::select! {
            // Handle Pub/Sub messages
            Some(msg) = async { 
                if let Some(rx) = message_receiver.as_mut() { rx.recv().await } else { None } 
            } => {
                match msg {
                    PubSubMessage::Message { channel, message } => {
                        let response = format!("1) \"message\"\n2) \"{}\"\n3) \"{}\"\r\n", channel, message);
                        writer.write_all(response.as_bytes()).await?;
                    },
                    PubSubMessage::Subscribe { channel, count } => {
                        let response = format!("1) \"subscribe\"\n2) \"{}\"\n3) (integer) {}\r\n", channel, count);
                        writer.write_all(response.as_bytes()).await?;
                    },
                    PubSubMessage::Unsubscribe { channel, count } => {
                        let response = format!("1) \"unsubscribe\"\n2) \"{}\"\n3) (integer) {}\r\n", channel, count);
                        writer.write_all(response.as_bytes()).await?;
                        if count == 0 {
                            subscriber_mode = false;
                            if let Some(id) = subscriber_id.take() {
                                let mut pubsub = pubsub_manager.write().await;
                                pubsub.remove_subscriber(id);
                                message_receiver = None;
                            }
                        }
                    },
                    PubSubMessage::PSubscribe { pattern, count } => {
                        let response = format!("1) \"psubscribe\"\n2) \"{}\"\n3) (integer) {}\r\n", pattern, count);
                        writer.write_all(response.as_bytes()).await?;
                    },
                    PubSubMessage::PUnsubscribe { pattern, count } => {
                        let response = format!("1) \"punsubscribe\"\n2) \"{}\"\n3) (integer) {}\r\n", pattern, count);
                        writer.write_all(response.as_bytes()).await?;
                        if count == 0 {
                            subscriber_mode = false;
                            if let Some(id) = subscriber_id.take() {
                                let mut pubsub = pubsub_manager.write().await;
                                pubsub.remove_subscriber(id);
                                message_receiver = None;
                            }
                        }
                    }
                }
                writer.flush().await?;
            },

           n = reader.read_line(&mut buffer) => {
    let n = n?;
    if n == 0 { break; } // client disconnected
    
    let command_str = buffer.trim().to_string();
    buffer.clear();

                if command_str.is_empty() { continue; }

                let command = parse_command(&command_str)?;
                match command {
                    Command::Subscribe { channels } => {
                        if subscriber_id.is_none() {
                            let mut pubsub = pubsub_manager.write().await;
                            let (id, rx) = pubsub.create_subscriber();
                            subscriber_id = Some(id);
                            message_receiver = Some(rx);
                            subscriber_mode = true;
                        }

                        if let Some(id) = subscriber_id {
                            let mut pubsub = pubsub_manager.write().await;
                            for channel in channels {
                                let count = pubsub.subscribe(id, channel.clone());
                                if let Some(tx) = pubsub.subscribers.get(&id) {
                                    let _ = tx.send(PubSubMessage::Subscribe { channel, count });
                                }
                            }
                        }
                    },
                    Command::Unsubscribe { channels } => {
                        if let Some(id) = subscriber_id {
                            let mut pubsub = pubsub_manager.write().await;
                            let target_channels = if channels.is_empty() {
                                pubsub.channels.keys().cloned().collect()
                            } else { channels };
                            for channel in target_channels {
                                let count = pubsub.unsubscribe(id, &channel);
                                if let Some(tx) = pubsub.subscribers.get(&id) {
                                    let _ = tx.send(PubSubMessage::Unsubscribe { channel, count });
                                }
                            }
                        }
                    },
                    Command::PSubscribe { patterns } => {
                        if subscriber_id.is_none() {
                            let mut pubsub = pubsub_manager.write().await;
                            let (id, rx) = pubsub.create_subscriber();
                            subscriber_id = Some(id);
                            message_receiver = Some(rx);
                            subscriber_mode = true;
                        }

                        if let Some(id) = subscriber_id {
                            let mut pubsub = pubsub_manager.write().await;
                            for pattern in patterns {
                                let count = pubsub.psubscribe(id, pattern.clone());
                                if let Some(tx) = pubsub.subscribers.get(&id) {
                                    let _ = tx.send(PubSubMessage::PSubscribe { pattern, count });
                                }
                            }
                        }
                    },
                    Command::PUnsubscribe { patterns } => {
                        if let Some(id) = subscriber_id {
                            let mut pubsub = pubsub_manager.write().await;
                            let target_patterns = if patterns.is_empty() {
                                pubsub.patterns.keys().cloned().collect()
                            } else { patterns };
                            for pattern in target_patterns {
                                let count = pubsub.punsubscribe(id, &pattern);
                                if let Some(tx) = pubsub.subscribers.get(&id) {
                                    let _ = tx.send(PubSubMessage::PUnsubscribe { pattern, count });
                                }
                            }
                        }
                    },
                    _ => {
                        let response = execute_command(
                            Arc::clone(&database),
                            command,
                            &mut client_auth,
                            Some(&pubsub_manager)
                        ).await;

                        let response_json = serde_json::to_string(&response)?;
                        writer.write_all(response_json.as_bytes()).await?;
                        writer.write_all(b"\r\n").await?;
                    }
                }

                writer.flush().await?;
            }
        }
    }

    if let Some(id) = subscriber_id {
        let mut pubsub = pubsub_manager.write().await;
        pubsub.remove_subscriber(id);
    }

    Ok(())
}
