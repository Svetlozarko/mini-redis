use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::sync::Arc;

use crate::db::Database;
use crate::commands::handle_command;

pub async fn run_server(addr: &str, db: Arc<Database>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            handle_client(stream, db).await;
        });
    }
}

async fn handle_client(stream: TcpStream, db: Arc<Database>) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut buffer = String::new();

    loop {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer).await.unwrap();
        if bytes == 0 {
            break;
        }

        let response = handle_command(&buffer, &db);
        if writer.write_all(response.as_bytes()).await.is_err() {
            break;
        }
    }
}
