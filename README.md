Zero-Copy RESP Datastore Engine

An in-memory datastore built entirely from scratch in asynchronous Rust. This engine implements RESP (the REdis Serialization Protocol) and is designed to safely handle thousands of concurrent connections.

This project is not just a datastore; it is the documentation of my journey deep into systems programming, memory architecture, and idiomatic Rust.

🚀 Running It Locally

If you want to pull this down and see where the engine is currently at, here is how you can run it on your system:


# Clone the repository
git clone https://github.com/tsrisabari/zero-copy-resp-datastore-engine.git

# Navigate into the project
cd zero-copy-resp-datastore-engine

# Build and run the engine
cargo run --release



(Note: Use redis-cli or netcat to connect to 127.0.0.1:6379 and issue standard commands like SET, GET, DEL, and EXIST.)

🧠 Core Architecture

This datastore is engineered for high-throughput and minimal memory overhead. Here are the core architectural decisions driving the engine:

Lock-Sharded Memory Vault: Instead of a single global lock, the database uses 64 independent HashMap shards. Keys are routed using Modulo Hashing (DefaultHasher). This means user:100 and user:101 live in completely different physical shards, bypassing global lock contention and thread starvation.

Highly Concurrent RwLock: By utilizing Read-Write Locks instead of standard Mutexes, thousands of users can simultaneously read data from the same shard, while write locks are isolated only to the specific shard being modified.

Zero-Copy Network Boundary: Data duplication is avoided. By utilizing bytes::BytesMut and .freeze(), the engine stores lightweight pointers to network memory. The Encoder/Decoder traits directly read these pointers to construct the TCP payload, eliminating heap allocation overhead for values.

Active & Lazy GC Engine: Time-To-Live (TTL) expiration is handled via a two-pronged approach. A background tokio::spawn loop actively sweeps and purges expired keys every 5 seconds to prevent memory leaks, while a lazy check guarantees stale data is never returned on a GET.

Asynchronous Write-Ahead Log (WAL): Disk persistence is achieved through a custom AOF engine. The engine streams bytes to disk and features a boot-time replay mechanism to perfectly reconstruct the RAM state upon server restart.

📖 The Engineering Devlog
This project is my primary learning ground. Here is the documentation of how my mental models are evolving as I build:

Week 1: Memory Vaults & The Tokio Hurdle

The Misunderstanding: Coming from a traditional mindset, I initially thought related data needed to be stored together in a JSON-like blob. I realized that Key-Value namespacing is vastly superior for write performance because it avoids parsing and rewriting entire objects.

The Breakthrough: Understanding Bytes::freeze over .clone(). I realized I am not actually moving strings around; I am moving 8-byte pointers.

The Struggle: Connecting my custom RespFrame enums to Tokio's network stream using the Encoder/Decoder traits was a massive conceptual hurdle.

Week 2: TCP Physics, Idiomatic Rust, & Systems Tradeoffs

The Eye-Opener (TCP Fragmentation): I learned the hard way that TCP is a stream of water, not a conveyor belt of neat packages. Handling split packets (like receiving $5\r\nhel and waiting for lo\r\n) completely changed how I view network buffers. I had to build a two-pass parser that correctly yields Ok(None) to instruct Tokio's Framed stream to wait for more physical bytes before executing.

The Struggle (The Strictness of Idiomatic Rust): Getting the compiler to compile is one thing; getting it to pass cargo clippy with zero warnings is another. Coming from a C-background, my instinct was manual for loops and if (ptr != null). I spent this week wrestling with expression-based returns, if let unwrapping, and mapping closures like .map(|t| Instant::now() + ...) to achieve true idiomatic, functional Rust
.
The Breakthrough (The I/O Hostage Situation): While analyzing my SET command latency, I realized a massive flaw: I was writing to the AOF disk before returning the +OK response. Even though my lock was dropped, I was holding the client hostage to the speed of my SSD, completely neutralizing my RAM speed. This realization laid the exact groundwork for Phase 2.

🗺️ The Rust Roadmap

Here is my clean plan for where this engine is going in the upcoming months:

Phase 1 (Completed)

[x] Build the core RESP serialization/deserialization framework.
[x] Concurrency Upgrade: Lock-Sharded RwLock for high-throughput parallel reads.
[x] Command Implementation: GET, SET (with EX expiration), DEL, EXIST.
[x] Networking & Ports: Fully connect TCP listeners and manage asynchronous client dropping/handling.
[x] AOF Boot Replay: Reconstruct exact database state from disk on startup.

Phase 2 (Active Next Steps)

[ ] Background I/O (mpsc channels): Decouple AOF disk writes from the main network event loop to achieve true in-memory response latency while maintaining durability.
[ ] True Zero-Copy Keys: Transition HashMap<String, ...> to HashMap<Bytes, ...> to eliminate the final heap allocation bottleneck (String parsing) during command routing.
[ ] Lock Elision on Boot: Bypass RwLock overhead entirely during AOF replay to drastically cut CPU usage and reduce server boot time.
[ ] LRU Eviction (OOM Defense): Build an eviction algorithm to protect the server from Out-Of-Memory crashes when processing millions of non-expiring keys.

🤝 A Note on Contributions & Mentorship
Because this project is in heavy, active development and serves as my primary learning ground, I am not currently looking for major code pull requests. I want to write the foundational code myself to ensure I truly learn it.

However, I am actively seeking guidance and mentorship.
My ultimate goal is to become an elite systems programmer and work at an incredible company like Fly.io, Cloudflare, or similar environments where low-level, high-performance engineering thrives. If you are a senior engineer, a Rustacean, or someone who has walked this path before:

Let's Connect: I am always looking to surround myself with builders and people who share this passion. Please feel free to reach out and connect with me on [LinkedIn](https://www.linkedin.com/in/sri-sabari-t-62b989427).Just mention you saw this repo!

Code Reviews: I would gladly welcome architectural advice, or pointers on where my logic can improve. Feel free to open an Issue just to leave feedback or point me toward resources that will help me fly.
I know I have a long way to go, so I gotta only go forward.

