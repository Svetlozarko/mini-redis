# Redis-Clone: A Redis-like Database in Rust

A high-performance, Redis-compatible database implementation written in Rust with support for multiple data types, key expiration, and concurrent client connections.

## Features

### Data Types
- **Strings**: Basic key-value storage with string values
- **Integers**: Numeric values with increment/decrement operations
- **Lists**: Ordered collections with push/pop operations from both ends
- **Sets**: Unordered collections of unique elements
- **Hashes**: Key-value pairs within a single key

### Core Functionality
- **Key Expiration**: TTL support with automatic cleanup
- **Thread Safety**: Concurrent access using `Arc<RwLock<>>`
- **TCP Server**: Async server built with Tokio
- **Redis-Compatible Commands**: Familiar Redis command syntax
- **Memory Management**: Efficient in-memory storage

## Installation

### Prerequisites
- Rust 1.70+ (with Cargo)
- Tokio runtime

### Build from Source
```bash
git clone <repository-url>
cd rust-redis
cargo build --release
