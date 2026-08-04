Zero-Copy RESP Datastore Engine

An in-memory datastore built entirely from scratch in asynchronous Rust. This engine implements RESP (the REdis Serialization Protocol) and is designed to safely handle thousands of concurrent connections.

This project is not just a datastore; it is the documentation of my journey deep into systems programming and Rust.

Running It Locally

If you want to pull this down and see where the engine is currently at, here is how you can run it on your system:

# Clone the repository
git clone https://github.com/yourusername/zero-copy-resp-engine.git

# Navigate into the project
cd zero-copy-resp-engine

# Build and run the engine
cargo run


(Note: As the networking layer is still actively being connected, you might need to use a tool like redis-cli or netcat on the configured port once the TCP listener is fully bound.)

The Journey So Far

This project is currently in active development. The foundational months focused heavily on ownership and borrowing concepts. My background in C made it a little easier, but the Rust compiler sure lives up to its name. I still have a long way to go, but this has twisted my brain in ways I could have never imagined.

Learning Rust is different—it feels more like learning a new skill that is both as hard as a mountain, but as unique as a mountain.

Technical Evolution

I have built the RESP framework and am able to parse data and connect it to my frames. Right now, my core focus is on building an incredibly strong datastore engine first, before I fully dive into connecting ports and managing the wider network architecture.

As I build the program, there is a constant need to change the code and evolve my understanding of how things work under the hood:

Memory & Bytes: I am experimenting heavily with how raw bytes perform, specifically analyzing the difference between .to_vec() and Bytes::freeze(), and determining which is superior based on specific use cases to achieve true zero-copy.

The Network Bridge: TCP doesn't inherently understand my code, so I am using the tokio::codec crate (Encoder/Decoder). As data is received in raw bytes over the network, it passes through my RESP code to be parsed and used at the exact needed time.

The Rust Roadmap

Here is my clean plan for where this engine is going in the upcoming months:

[x] Build the core RESP serialization/deserialization framework.

[ ] Concurrency Upgrade: Transition the central state from Arc<Mutex> to RwLock to handle highly concurrent reads more efficiently.

[ ] Command Implementation: Build out the core datastore commands (GET, SET, etc.) to process the parsed frames.

[ ] Networking & Ports: Fully connect the TCP listeners and manage asynchronous connection dropping/handling.

[ ] Testing & Stability: Implement stress testing and proptesting to ensure it can store and perform live data connections in a constantly changing environment.

A Note on Contributions & Mentorship

Because this project is in heavy, active development and serves as my primary learning ground, I am not currently looking for major code pull requests. I want to write the foundational code myself to ensure I truly learn it.

However, I am actively seeking guidance and mentorship.

My ultimate goal is to become good enough at systems programming to work at an incredible company like Fly.io (or similar environments where this kind of low-level, high-performance engineering thrives). If you are a senior engineer, a Rustacean, or someone who has walked this path before:

I would gladly welcome code reviews, architectural advice, or pointers on where my logic can improve. Let's Connect: I am always looking to surround myself with builders and people who share this passion. Please feel free to reach out and connect with me on [LinkedIn](https://www.linkedin.com/in/sri-sabari-t-62b989427). Just mention you saw this repo!

Feel free to open an Issue just to leave feedback or point me toward resources that will help me fly.

I know I have a long way to go, so I gotta only go forward.
