

# *💡 Why Mini_Redis?*

Mini_Redis is my first larger low-level systems project. The idea came spontaneously while working on **BidCommerce**, a web project where I wanted a high-performance, low-latency real-time bidding platform with full e-commerce features. I needed a database that was fast, reliable, and easy to scale - Redis was perfect.  

I loved Redis so much that I decided to recreate it to gain a deeper understanding of its functionality. I chose **Rust** as a programing language, because it offers system-level control, memory safety, and high performance, making it ideal for building efficient low-level systems.

## 📦 Features

- In-memory key-value store  
- Write-Ahead Logging (WAL) for durability  
- Pub/Sub messaging system  
- Memory management with LRU/LFU eviction policies  
- Crash recovery with snapshots and WAL replay  
- TCP-based client-server communication  

## ⚙️ How Mini_Redis Works
#### 1. Connection Flow
Client connects → TCP Server (server.rs)
                ↓
        Protocol Parser (protocol.rs)
                ↓
        Command Executor (commands.rs)
                ↓
        Database Operations (database.rs)
                ↓
        Response to Client

#### 2. Write-Ahead Logging (WAL)
Every write operation follows this sequence:
1. **Log to WAL**: Operation is written to append-only log file
2. **Execute**: Operation is performed on in-memory database
3. **Acknowledge**: Success response sent to client
4. **Background Save**: Periodic snapshots to disk (every 60 seconds)

On crash recovery:
1. Load last valid snapshot
2. Replay WAL entries since snapshot
3. Verify integrity with checksums
4. Resume normal operations

#### 3. Pub/Sub System
The pub/sub system maintains three core data structures:
- **Channels Map**: `HashMap<String, HashSet<SubscriberId>>` - tracks exact channel subscriptions
- **Patterns Map**: `HashMap<String, HashSet<SubscriberId>>` - tracks pattern subscriptions
- **Subscribers Map**: `HashMap<SubscriberId, MessageQueue>` - message queues for each subscriber

When a message is published:
1. Find all exact channel subscribers
2. Match against all pattern subscriptions
3. Send message to all matching subscriber queues
4. Return count of recipients

#### 4. Memory Management
The memory manager tracks:
- Total memory usage (approximate)
- Access frequency (LFU counter)
- Access recency (LRU timestamp)
- Expiry status

When memory limit is reached, the configured eviction policy determines which keys to remove.

### Mini_Redis Workflow
```text
              ┌─────────────┐
              │   Client    │
              └─────┬───────┘
                    │
                    ▼
             ┌─────────────┐
             │ TCP Server  │  
             └─────┬───────┘
                    │
                    ▼
             ┌─────────────┐
             │ Protocol    │  
             │ Parser      │
             └─────┬───────┘
                    │
                    ▼
             ┌─────────────┐
             │ Command     │ 
             │ Executor    │
             └─────┬───────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
┌─────────────┐           ┌─────────────┐
│ Database    │           │ WAL Logging │
│ Operations  │           |_____________|             
│             | ─────────────┘
└─────┬───────┘                 │
      │                         │
      ▼                         ▼
┌─────────────┐           ┌─────────────┐
│ Memory Mgmt │           │ Crash Recov │
│             │           │ & Backups   │
└─────┬───────┘           └─────────────┘
      │
      ▼
┌─────────────┐
│ Pub/Sub     │  
│ Messaging   │
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Response to │
│ Client      │
└─────────────┘



