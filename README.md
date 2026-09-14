Zero-Copy RESP Datastore Engine
An in-memory datastore built entirely from scratch in asynchronous Rust. This engine implements RESP (the REdis Serialization Protocol) and is designed to safely handle thousands of concurrent connections.

This project is not just a datastore; it is the documentation of my journey deep into systems programming, memory architecture, physical hardware constraints, and idiomatic Rust.

Running It Locally

If you want to pull this down and see where the engine is currently at, here is how you can run it on your system:

Clone the repository

  git clone https://github.com/tsrisabari/zero-copy-resp-datastore-engine.git 

Navigate into the project

  cd zero-copy-resp-datastore-engine 

Build and run the engine
 
           cargo run --release



(Note: Use redis-cli or netcat to connect to 127.0.0.1:6379 and issue standard commands like SET, GET, DEL, and EXIST. You can also boot with --appendfsync=always or --appendfsync=everysec to test persistence physics).

Core Architecture
This datastore is engineered for high-throughput and minimal memory overhead. Here are the core architectural decisions driving the engine:

Lock-Sharded Memory Vault: Instead of a single global lock, the database uses 64 independent HashMap shards. Keys are routed using Modulo Hashing. This means user:100 and user:101 live in completely different physical shards, bypassing global lock contention and thread starvation.


Highly Concurrent RwLock: By utilizing Read-Write Locks instead of standard Mutexes, thousands of users can simultaneously read data from the same shard, while write locks are isolated only to the specific shard being modified.


Zero-Copy Network Boundary: Data duplication is avoided. By utilizing bytes::BytesMut and .freeze(), the engine stores lightweight pointers to network memory. The Encoder/Decoder traits directly read these pointers to construct the TCP payload, eliminating heap allocation overhead for values.


Decoupled MPSC Persistence: Disk I/O is completely separated from the network loop. Commands are passed through a bounded mpsc channel (1,000 capacity) acting as a shock-absorber. A background worker drains this channel into the OS Page Cache, allowing the network to process requests at pure RAM speed without being held hostage by the SSD.


Active & Lazy GC Engine: Time-To-Live (TTL) expiration is handled via a two-pronged approach. A background tokio::spawn loop actively sweeps and purges expired keys every 5 seconds to prevent memory leaks, while a lazy check guarantees stale data is never returned on a GET.


The Engineering Devlog
This project is my primary learning ground. Here is the documentation of how my mental models are evolving as I build:

Week 1: Memory Vaults & The Tokio Hurdle

The Misunderstanding: Coming from a traditional mindset, I initially thought related data needed to be stored together in a JSON-like blob. I realized that Key-Value namespacing is vastly superior for write performance because it avoids parsing and rewriting entire objects.


The Breakthrough: Understanding Bytes::freeze over .clone(). I realized I am not actually moving strings around; I am moving 8-byte fat pointers.


The Struggle: Connecting my custom RespFrame enums to Tokio's network stream using the Encoder/Decoder traits was a massive conceptual hurdle.

Week 2: TCP Physics, Idiomatic Rust, & Systems Tradeoffs

The Eye-Opener (TCP Fragmentation): I learned the hard way that TCP is a stream of water, not a conveyor belt of neat packages. Handling split packets completely changed how I view network buffers. I had to build a two-pass parser that correctly yields Ok(None) to instruct Tokio's Framed stream to wait for more physical bytes before executing.


The Struggle (The Strictness of Rust): Getting the compiler to compile is one thing; getting it to pass cargo clippy with zero warnings is another. I spent this week wrestling with expression-based returns, if let unwrapping, and mapping closures to achieve true idiomatic, functional Rust.


The Breakthrough (The I/O Hostage Situation): While analyzing my SET command latency, I realized a massive flaw: I was writing to the AOF disk before returning the +OK response. I was holding the client hostage to the speed of my SSD, completely neutralizing my RAM speed.


Week 3: Backpressure, OS Page Cache, & Crash Resilience

The Physics of I/O: I built an asynchronous background worker using mpsc channels to fix the I/O bottleneck. During benchmarking, I realized the critical difference between the OS RAM Buffer (write_all) and the physical SSD flash (sync_data).


Backpressure in Action: By implementing a bounded channel of 1,000 messages, I successfully implemented network backpressure. Testing with --appendfsync=always immediately dropped my throughput to ~300 RPS, proving the shock-absorber protects the RAM from overflowing when the hardware can't keep up.


The Result: Testing on --appendfsync=everysec, the database hit the TCP loopback limit on my machine (~60,000 RPS). To prove durability, I violently killed the server mid-process. On restart, the engine successfully read a 37MB Write-Ahead Log from the physical disk, parsing and reconstructing the entire state back into the 64 RAM shards without losing a single byte.


The Rust Roadmap
Here is my plan for where this engine is going in the upcoming months:

Phase 1 & 2 (Completed)
[x] Build the core RESP serialization/deserialization framework.


[x] Concurrency Upgrade: Lock-Sharded RwLock for high-throughput parallel reads.


[x] Background I/O (mpsc channels): Decouple AOF disk writes from the main network event loop.


[x] Boot-Time Replay: Reconstruct exact database state from disk on startup.


[x] Configurable Persistence: Implement EverySec and Always sync modes.




Phase 3 (Active Next Steps)
[ ] True Zero-Copy Keys: Transition HashMap<String, ...> to HashMap<Bytes, ...> to eliminate the final heap allocation bottleneck (String parsing) during command routing.


[ ] Lock Elision on Boot: Bypass RwLock overhead entirely during AOF replay to drastically cut CPU usage and reduce server boot time.


[ ] LRU Eviction (OOM Defense): Build an eviction algorithm to protect the server from Out-Of-Memory crashes when processing millions of non-expiring keys.


A Note on Contributions & Mentorship

Because this project is in heavy, active development and serves as my primary learning ground, I am not currently looking for major code pull requests. I want to write the foundational code myself to ensure I truly learn it.

However, I am actively seeking guidance and mentorship. My ultimate goal is to become an elite systems programmer and work at an infrastructure company like Fly.io, Cloudflare, or similar environments where low-level, high-performance engineering thrives. If you are a senior engineer, a Rustacean, or someone who has walked this path before:

Let's Connect: I am always looking to surround myself with builders and people who share this passion. Please feel free to reach out and connect with me on[LinkedIn](https://www.linkedin.com/in/sri-sabari-t-62b989427).Just mention you saw this repo.


Code Reviews: I would gladly welcome architectural advice, or pointers on where my logic can improve. Feel free to open an Issue just to leave feedback or point me toward resources that will help me build better systems.


I know I have a long way to go, so I gotta only go forward.

