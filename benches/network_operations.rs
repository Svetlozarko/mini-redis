use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Read a full RESP reply (simple but correct for single-line or bulk replies)
async fn read_resp(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

async fn send_resp(stream: &mut TcpStream, cmd: &str) {
    stream.write_all(cmd.as_bytes()).await.unwrap();
    let _ = read_resp(stream).await;
}

/// Establish a single reusable connection
async fn new_conn() -> TcpStream {
    TcpStream::connect("127.0.0.1:6380").await.unwrap()
}

//
// ──────────────────────────────────────────────────────────────
//   Single-operation benchmarks
// ──────────────────────────────────────────────────────────────
//

fn bench_set(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut conn = rt.block_on(new_conn());

    c.bench_function("SET_small", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cmd = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
                send_resp(&mut conn, cmd).await;
            })
        });
    });
}

fn bench_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut conn = rt.block_on(new_conn());

    // setup
    rt.block_on(async {
        let cmd = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
        send_resp(&mut conn, cmd).await;
    });

    c.bench_function("GET_existing", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cmd = "*2\r\n$3\r\nGET\r\n$8\r\ntest_key\r\n";
                send_resp(&mut conn, cmd).await;
            })
        });
    });
}

fn bench_del(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut conn = rt.block_on(new_conn());

    c.bench_function("DEL_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let set_cmd = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
                send_resp(&mut conn, set_cmd).await;

                let del_cmd = "*2\r\n$3\r\nDEL\r\n$8\r\ntest_key\r\n";
                send_resp(&mut conn, del_cmd).await;
            })
        });
    });
}

fn bench_exists(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut conn = rt.block_on(new_conn());

    // Setup
    rt.block_on(async {
        let cmd = "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n";
        send_resp(&mut conn, cmd).await;
    });

    c.bench_function("EXISTS_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cmd = "*2\r\n$6\r\nEXISTS\r\n$8\r\ntest_key\r\n";
                send_resp(&mut conn, cmd).await;
            })
        });
    });
}

//
// ──────────────────────────────────────────────────────────────
//   BULK / PIPELINED BENCHMARKS (Correct Redis-style)
// ──────────────────────────────────────────────────────────────
//

fn bench_bulk_set(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("BULK_SET");

    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut conn = rt.block_on(new_conn());

            b.iter(|| {
                rt.block_on(async {
                    // Pipeline N SET commands at once
                    let mut batch = String::with_capacity(size * 50);
                    for i in 0..size {
                        batch.push_str(&format!(
                            "*3\r\n$3\r\nSET\r\n${}\r\nkey_{}\r\n${}\r\nvalue_{}\r\n",
                            4 + i.to_string().len(), i,
                            6 + i.to_string().len(), i
                        ));
                    }

                    // Write everything at once → real Redis-style bulk test
                    conn.write_all(batch.as_bytes()).await.unwrap();

                    // Read all responses
                    for _ in 0..size {
                        let _ = read_resp(&mut conn).await;
                    }
                })
            });
        });
    }

    group.finish();
}

//
// ──────────────────────────────────────────────────────────────
//   Criterion boilerplate
// ──────────────────────────────────────────────────────────────
//

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_del,
    bench_exists,
    bench_bulk_set
);
criterion_main!(benches);
