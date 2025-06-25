# 🚀 Redis-Clone: A High-Performance Redis Implementation in Rust

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A blazingly fast, Redis-compatible database server written in Rust. Built for performance, reliability, and ease of use with full support for Redis commands, data persistence, and concurrent client connections.

## ✨ Features

### 🗄️ **Complete Data Type Support**
- **Strings** - Key-value storage with string and numeric values
- **Lists** - Ordered collections with LPUSH, RPUSH, LPOP, RPOP operations
- **Sets** - Unordered collections of unique elements with set operations
- **Hashes** - Field-value pairs within keys for structured data
- **Integers** - Atomic increment/decrement operations

### 🔐 **Security & Authentication**
- **Password Protection** - Secure your database with AUTH command
- **Session Management** - Per-client authentication state
- **Access Control** - Command filtering for unauthenticated clients

### 💾 **Data Persistence**
- **Memory-Mapped Files** - High-performance disk I/O
- **JSON Serialization** - Human-readable data format
- **Automatic Backups** - Background saves every 60 seconds
- **Crash Recovery** - Automatic data restoration on startup

### ⚡ **Performance & Concurrency**
- **Async I/O** - Built on Tokio for maximum throughput
- **Thread Safety** - Concurrent access with `Arc<RwLock<>>`
- **Memory Efficient** - Optimized data structures
- **Connection Pooling** - Handle thousands of concurrent clients

### 🛠️ **Redis Compatibility**
- **Familiar Commands** - Full Redis command syntax support
- **TTL Support** - Key expiration with automatic cleanup
- **Memory Analytics** - Built-in memory usage reporting
- **Info Commands** - Server statistics and diagnostics

## 🚀 Quick Start

### Prerequisites
- **Rust 1.70+** - [Install Rust](https://rustup.rs/)
- **Git** - [Install Git](https://git-scm.com/downloads)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/redis-clone.git
cd redis-clone

# Build in release mode
cargo build --release

# Start the server
./target/release/redis-clone --password mypass --dbfilename data.db
