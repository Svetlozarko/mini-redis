use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::time::Duration;

async fn create_connection() -> TcpStream {
    for attempt in 0..5 {
        match TcpStream::connect("127.0.0.1:6380").await {
            Ok(stream) => return stream,
            Err(e) if attempt < 4 => {
                eprintln!("Connection attempt {} failed: {}. Retrying...", attempt + 1, e);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("Failed to connect to Redis server after 5 attempts: {}", e),
        }
    }
    unreachable!()
}


async fn send_command(stream: &mut TcpStream, command: &str) -> String {
    stream.write_all(command.as_bytes()).await.unwrap();
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer[..n]).to_string()
}

fn bench_network_set(c: &mut Criterion) {
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
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("network_delete", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut stream = create_connection().await;

                // Setup: Insert key
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
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup key
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
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("network_bulk_set");

    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let mut stream = create_connection().await;
                    for i in 0..size {
                        let command = format!(
                            "*3\r\n$3\r\nSET\r\n${}\r\nkey_{}\r\n${}\r\nvalue_{}\r\n",
                            5 + i.to_string().len(), i,
                            7 + i.to_string().len(), i
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
