use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::fs;
use std::process::Command;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct MyDb {
    data: HashMap<String, String>,
}

impl MyDb {
    fn load(file: &str) -> Self {
        let content = fs::read_to_string(file).unwrap_or_else(|_| "{}".to_string());
        let data: HashMap<String, String> = serde_json::from_str(&content).unwrap();
        Self { data }
    }
}

fn persistence_crash_test(c: &mut Criterion) {
    let db_file = "db.json";

    c.bench_function("db_persistence_crash", |b| {
        b.iter(|| {
            let _ = fs::remove_file(db_file);
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("simulate_crash")
                .status()
                .expect("Failed to spawn child process");

            assert!(status.code().is_some());
            let db = MyDb::load(db_file);
            match db.data.get("key1") {
                Some(v) => assert_eq!(v, "value1"),
                None => panic!("key1 missing after recovery"),
            }
        })
    });
}

criterion_group!(tests, persistence_crash_test);
criterion_main!(tests);
