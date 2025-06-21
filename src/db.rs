use dashmap::DashMap;
use std::sync::Arc;

pub struct Database {
    store: DashMap<String, String>,
}

impl Database {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            store: DashMap::new(),
        })
    }

    pub fn set(&self, key: String, value: String) {
        self.store.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).map(|v| v.value().clone())
    }

    pub fn del(&self, key: &str) -> bool {
        self.store.remove(key).is_some()
    }
}
