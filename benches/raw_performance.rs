use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rust_redis::database::{RedisDatabase, Database};
use rust_redis::data_types::RedisValue;
use rust_redis::memory::MemoryManager;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;

fn create_test_db() -> Database {
    let memory_manager = MemoryManager::new(None, "noeviction".to_string());
    let db = RedisDatabase {
        data: std::collections::HashMap::new(),
        expires: std::collections::HashMap::new(),
        memory_manager,
    };
    Arc::new(RwLock::new(db))
}

fn bench_set_operation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("set_small_string", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                let mut db_write = db.write().await;
                db_write.set(
                    black_box("key".to_string()),
                    black_box(RedisValue::String("value".to_string()))
                )
            })
        });
    });

    c.bench_function("set_large_string", |b| {
        let large_value = "x".repeat(10000);
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                let mut db_write = db.write().await;
                db_write.set(
                    black_box("key".to_string()),
                    black_box(RedisValue::String(large_value.clone()))
                )
            })
        });
    });
}

fn bench_get_operation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("get_existing_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                {
                    let mut db_write = db.write().await;
                    let _ = db_write.set("key".to_string(), RedisValue::String("value".to_string()));
                }
                let mut db_read = db.write().await;
                black_box(db_read.get(black_box("key")))
            })
        });
    });

    c.bench_function("get_nonexistent_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                let mut db_read = db.write().await;
                black_box(db_read.get(black_box("nonexistent")))
            })
        });
    });
}

fn bench_delete_operation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("delete_existing_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                {
                    let mut db_write = db.write().await;
                    let _ = db_write.set("key".to_string(), RedisValue::String("value".to_string()));
                }
                let mut db_write = db.write().await;
                black_box(db_write.delete(black_box("key")))
            })
        });
    });
}

fn bench_exists_operation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("exists_check", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                {
                    let mut db_write = db.write().await;
                    let _ = db_write.set("key".to_string(), RedisValue::String("value".to_string()));
                }
                let mut db_read = db.write().await;
                black_box(db_read.exists(black_box("key")))
            })
        });
    });
}

fn bench_expiry_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("set_with_expiry", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                let mut db_write = db.write().await;
                db_write.set_with_expiry(
                    black_box("key".to_string()),
                    black_box(RedisValue::String("value".to_string())),
                    black_box(Duration::from_secs(60))
                )
            })
        });
    });

    c.bench_function("check_ttl", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = create_test_db();
                {
                    let mut db_write = db.write().await;
                    let _ = db_write.set_with_expiry(
                        "key".to_string(),
                        RedisValue::String("value".to_string()),
                        Duration::from_secs(60)
                    );
                }
                let mut db_read = db.write().await;
                black_box(db_read.ttl(black_box("key")))
            })
        });
    });
}

fn bench_bulk_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("bulk_set");
    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async move {
                    let db = create_test_db();
                    let mut db_write = db.write().await;
                    for i in 0..size {
                        let _ = db_write.set(
                            format!("key_{}", i),
                            RedisValue::String(format!("value_{}", i))
                        );
                    }
                })
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_set_operation,
    bench_get_operation,
    bench_delete_operation,
    bench_exists_operation,
    bench_expiry_operations,
    bench_bulk_operations
);
criterion_main!(benches);
