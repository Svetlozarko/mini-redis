use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::time::Duration;
use std::process::{Command, Child};
use std::sync::{Arc, Mutex};

static SERVER_HANDLE: Mutex<Option<Child>> = Mutex::new(None);

fn start_server() {
    let mut handle = SERVER_HANDLE.lock().unwrap();
    if handle.is_none() {
        println!("Starting Redis server on port 6380...");
        let child = Command::new("cargo")
            .args(&["run", "--release"])
            .spawn()
            .expect("Failed to start Redis server");

        *handle = Some(child);

        // Wait for server to be ready
        std::thread::sleep(Duration::from_secs(2));
        println!("Server started successfully!");
    }
}

// Helper function to send RESP commands and read responses
async fn send_command(stream: &mut TcpStream, command: &str) -> String {
    // Send command
    stream.write_all(command.as_bytes()).await.unwrap();

    // Read response
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer[..n]).to_string()
}

// Helper to create a connection
async fn create_connection() -> TcpStream {
    for attempt in 0..5 {
        match TcpStream::connect("127.0.0.1:6380").await {
            Ok(stream) => return stream,
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("Failed to connect to Redis server after 5 attempts: {}", e),
        }
    }
    unreachable!()
}

fn bench_network_set(c: &mut Criterion) {
    start_server();

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("network_set_small", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut stream = create_connection().await;
                let command = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
                let _ = black_box(send_command(&mut stream, command).await);
            })
        });
    });
}

fn bench_network_get(c: &mut Criterion) {
    start_server();

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Insert a key first
    rt.block_on(async {
        let mut stream = create_connection().await;
        let command = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
        send_command(&mut stream, command).await;
    });

    c.bench_function("network_get_existing", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut stream = create_connection().await;
                let command = "*2\r\n$3\r\nGET\r\n$8\r\ntest_key\r\n";
                let _ = black_box(send_command(&mut stream, command).await);
            })
        });
    });
}

fn bench_network_delete(c: &mut Criterion) {
    start_server();

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("network_delete", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Setup: Insert key
                let mut stream = create_connection().await;
                let set_cmd = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
                send_command(&mut stream, set_cmd).await;

                // Delete it
                let del_cmd = "*2\r\n$3\r\nDEL\r\n$8\r\ntest_key\r\n";
                let _ = black_box(send_command(&mut stream, del_cmd).await);
            })
        });
    });
}

fn bench_network_exists(c: &mut Criterion) {
    start_server();

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Insert a key first
    rt.block_on(async {
        let mut stream = create_connection().await;
        let command = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
        send_command(&mut stream, command).await;
    });

    c.bench_function("network_exists", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut stream = create_connection().await;
                let command = "*2\r\n$6\r\nEXISTS\r\n$8\r\ntest_key\r\n";
                let _ = black_box(send_command(&mut stream, command).await);
            })
        });
    });
}

fn bench_network_bulk_operations(c: &mut Criterion) {
    start_server();

    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("network_bulk_set");

    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let mut stream = create_connection().await;
                    for i in 0..size {
                        let command = format!(
                            "*3\r\n$3\r\nSET\r\n${}r\nkey_{}\r\n${}r\nvalue_{}\r\n",
                            (i.to_string().len() + 4), i,
                            (i.to_string().len() + 6), i
                        );
                        let _ = black_box(send_command(&mut stream, &command).await);
                    }
                })
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_network_set,
    bench_network_get,
    bench_network_delete,
    bench_network_exists,
    bench_network_bulk_operations
);
criterion_main!(benches);
