Zero-Copy RESP Datastore Engine
An in-memory datastore built entirely from scratch in asynchronous Rust. This engine implements RESP (the REdis Serialization Protocol) and is designed to safely handle thousands of concurrent connections.

This project is not just a datastore; it is the documentation of my journey deep into systems programming and Rust.

Running It Locally
If you want to pull this down and see where the engine is currently at, here is how you can run it on your system:

#Clone the repository
git clone https://github.com/tsrisabari/zero-copy-resp-datastore-engine.git

#Navigate into the project
cd zero-copy-resp-datastore-engine

#Build and run the engine
cargo run

(Note: As the networking layer is still actively being connected, you might need to use a tool like redis-cli or netcat on the configured port once the TCP listener is fully bound.)

Core Architecture
This datastore is engineered for high-throughput and minimal memory overhead. Here are the core architectural decisions driving the engine:

Lock-Sharded Memory Vault: Instead of a single global lock, the database uses 64 independent HashMap shards. Keys are routed using Modulo Hashing. This means user:100:name and user:100:age might live in completely different physical shards, allowing parallel execution without thread starvation.

Highly Concurrent RwLock: By utilizing Read-Write Locks instead of standard Mutexes, thousands of users can simultaneously read data from the same shard, while write locks are isolated only to the specific shard being modified.

Zero-Copy Network Boundary: Data duplication is avoided entirely. By using bytes::Bytes::freeze(), the engine stores lightweight pointers to memory. When a client requests data, the Encoder directly reads these pointers to construct the TCP payload, completely eliminating heap allocation overhead.

Lazy Deletion Engine: Time-To-Live (TTL) expiration is handled lazily. Instead of running a heavy background thread to constantly clear memory, the engine checks the Instant metadata at the exact moment of a GET request, keeping CPU cycles focused on live traffic.

Asynchronous Write-Ahead Log (WAL): Disk persistence is achieved through a custom AOF (Append-Only File) engine using tokio::fs::OpenOptions. The disk I/O phase is deliberately executed after the memory RwLock is dropped to prevent the physical hard drive from blocking RAM access.

The Engineering Devlog
This project is my primary learning ground. Here is the documentation of how my mental models are evolving as I build:

Week 1 (Current): Memory Vaults, Zero-Copy, & Tokio Integrations

The Misunderstanding: Coming from a traditional mindset, I initially thought related data (like a user's name and age) needed to be stored together in a JSON-like blob. I realized that Key-Value namespacing (routing user:100:name and user:100:age independently) is vastly superior for write performance because it avoids parsing and rewriting entire objects.

The Breakthrough: Understanding Bytes::freeze over .clone(). I realized I am not actually moving strings around; I am moving 8-byte pointers.

The Struggle: Connecting my custom RespFrame enums to Tokio's network stream using the Encoder/Decoder traits was a massive conceptual hurdle, as was figuring out array cursor read-ahead logic. Furthermore, shifting from C-style error handling (trying an action and reacting to a failure) to Rust's declarative OpenOptions (defining the rules of the file upfront before accessing it) twisted my brain, but ultimately proved much safer.

The Rust Roadmap
Here is my clean plan for where this engine is going in the upcoming months:

[x] Build the core RESP serialization/deserialization framework.

[x] Concurrency Upgrade: Transition the central state from Arc<Mutex> to a Lock-Sharded RwLock to handle highly concurrent reads.

[x] Command Implementation: Build out the core datastore commands (GET, SET with EXPIRE) to process the parsed frames.

[ ] AOF Binary Upgrade: Transition the Write-Ahead Log from human-readable text to raw binary RESP bytes by feeding file I/O directly through the network Decoder/Encoder to prevent allocation overhead during boot recovery.

[ ] Networking & Ports: Fully connect the TCP listeners and manage asynchronous connection dropping/handling.

[ ] Testing & Stability: Implement stress testing and proptesting to ensure it can store and perform live data connections in a constantly changing environment.

A Note on Contributions & Mentorship

Because this project is in heavy, active development and serves as my primary learning ground, I am not currently looking for major code pull requests. I want to write the foundational code myself to ensure I truly learn it.

However, I am actively seeking guidance and mentorship.

My ultimate goal is to become good enough at systems programming to work at an incredible company like Fly.io (or similar environments where this kind of low-level, high-performance engineering thrives). If you are a senior engineer, a Rustacean, or someone who has walked this path before:

I would gladly welcome code reviews, architectural advice, or pointers on where my logic can improve. 
Let's Connect: I am always looking to surround myself with builders and people who share this passion. Please feel free to reach out and connect with me on [LinkedIn](https://www.linkedin.com/in/sri-sabari-t-62b989427). Just mention you saw this repo!

Feel free to open an Issue just to leave feedback or point me toward resources that will help me fly.

I know I have a long way to go, so I gotta only go forward.
