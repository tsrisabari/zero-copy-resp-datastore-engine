use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::codec::{Decoder, Encoder};
use tracing::Instrument;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
//use tracing_subscriber::prelude::*;
use tracing_subscriber::fmt::format::FmtSpan;
//use tracing_flame::FlameLayer;

mod cmd;
mod frame;
use crate::cmd::Command;
use crate::frame::{RespCodec, RespFrame};

const NUM_SHARDS: usize = 64;

type DbData = HashMap<Bytes, (Arc<RespFrame>, Option<Instant>)>;
type DbShard = RwLock<DbData>;

pub struct ShardedDb {
    pub shards: Vec<DbShard>,
}

impl Default for ShardedDb {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedDb {
    pub fn new() -> ShardedDb {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        ShardedDb { shards }
    }

    pub fn get_shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        (hash_value as usize) % NUM_SHARDS
    }
}
#[derive(Debug, Clone, Copy)]
pub enum PersistenceMode {
    Always,
    EverySec,
}

#[derive(Debug)]
pub enum DbMessage {
    WriteBytes(bytes::Bytes),
    ExecuteAtomicSwap,
}

async fn replay_aof(db: Arc<ShardedDb>) {
    let file_result = File::open("database_aof").await;

    match file_result {
        Ok(mut file) => {
            let mut buffer = BytesMut::new();
            let mut chunk = [0; 4096];
            let mut codec = RespCodec;
            loop {
                let bytes_read = file.read(&mut chunk).await.unwrap();
                if bytes_read == 0 {
                    tracing::debug!("AOF Replay Complete");
                    break;
                }
                buffer.extend_from_slice(&chunk[..bytes_read]);
                loop {
                    match codec.decode(&mut buffer) {
                        Ok(Some(frame)) => {
                            if let Ok(Command::Set { key, value, time }) =
                                Command::from_frame(frame)
                            {
                                let expiration_time =
                                    time.map(|t| Instant::now() + Duration::from_secs(t));
                                let room = db.get_shard_index(&key);
                                {
                                    let mut pen = db.shards[room].write().unwrap();
                                    pen.insert(key, (Arc::new(value.clone()), expiration_time));
                                }
                            };
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::error!(error=%e,"AOF Parsing Error");
                            break;
                        }
                    }
                }
            }
        }
        Err(_) => {
            tracing::info!("No AOF file found. Starting with a fresh database.");
        }
    }
}
#[tokio::main]

async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut persistence = PersistenceMode::EverySec;
    // ============================================================================
    // TELEMETRY CONFIGURATION
    // Terminal I/O heavily throttles the Tokio reactor during high-load benchmarks.
    // We default to WARN-only logging to achieve 90,000+ RPS.
    //
    // -> TO GENERATE A FLAMEGRAPH: Comment out Block A, and uncomment Block B.
    // ============================================================================

    // --- BLOCK A: High-Performance Production Mode (Default) ---
    tracing_subscriber::registry()
        .with(fmt::layer().with_span_events(FmtSpan::CLOSE))
        //-> (If you want every log info and active timer stamps uncomment this line and comment the below line)
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        //.with(EnvFilter::from_default_env().add_directive(tracing::Level::WARN.into()))
        .init();

    /*
    // --- BLOCK B: Flamegraph Profiling Mode ---
    // Generates `tracing.folded` to visualize lock contention and thread starvation.
    // Process the output using inferno: `cat tracing.folded | inferno-flamegraph > perf-profile.svg`
    let (flame_layer, _guard) = tracing_flame::FlameLayer::with_file("tracing.folded").unwrap();
    tracing_subscriber::registry()
        .with(flame_layer)
        .init();*/

    let (tx, mut rx) = mpsc::channel::<DbMessage>(100_000);

    if args.contains(&"--appendfsync=always".to_string()) {
        persistence = PersistenceMode::Always;
        tracing::info!(
            "WARNING: Persistence mode set to ALWAYS. Expect severe performance degradation."
        );
    } else {
        tracing::info!("Persistence mode set to EVERYSEC.");
    }

    let db = Arc::new(ShardedDb::new());
    let db_bg_clone = Arc::clone(&db);
    let tx_bg_clone = tx.clone();
    tokio::spawn(async move {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("database_aof")
            .await
            .expect("Fatal Error: Background worker failed to open AOF file.");

        let mut writer = BufWriter::with_capacity(8192, file);

        let mut flush_interval = tokio::time::interval(Duration::from_secs(1));
        let mut bgrwriter_interval = tokio::time::interval(Duration::from_secs(30));
        let mut is_rewriting = false;
        let mut aof_rewrite_buffer: Vec<bytes::Bytes> = Vec::new();

        loop {
            tokio::select! {
                      Some(msg) = rx.recv() => {
                          match msg {
                          DbMessage::WriteBytes(payload) => {

                              writer.write_all(&payload).await.unwrap();

                              if is_rewriting {
                              aof_rewrite_buffer.push(payload.clone());
                               }
                              if rx.is_empty() {
                                     writer.flush().await.unwrap();
                                  }


                              if let PersistenceMode::Always = persistence{
                                  writer.flush().await.unwrap();
                              if let Err(e) = writer.get_ref().sync_data().await {
                                      tracing::error!(error=%e,"CRITICAL: physical disk sync failed during ALWAYS mode");
                                  }
                              }
                              }
                           DbMessage::ExecuteAtomicSwap=>{
                           writer.flush().await.unwrap();
                           let  mut temp_file = OpenOptions::new().append(true).open("temp_aof").await.unwrap();


                          for buffered_msg in &aof_rewrite_buffer {
                              tokio::io::AsyncWriteExt::write_all(&mut temp_file, buffered_msg).await.unwrap();
                          }
                          temp_file.sync_data().await.unwrap();
                          tokio::fs::rename("temp_aof", "database_aof").await.unwrap();
                          let new_file = OpenOptions::new()
                                  .create(true)
                                  .append(true)
                                  .open("database_aof")
                                  .await
                                  .unwrap();
                                  writer = tokio::io::BufWriter::with_capacity(8192, new_file);
                                  is_rewriting = false;
                                  aof_rewrite_buffer.clear();
                           }
                          }
                      }

                      _ = flush_interval.tick() => {

                          if let PersistenceMode::EverySec = persistence {
                                  writer.flush().await.unwrap();
                                  if let Err(e) = writer.get_ref().sync_data().await {
                                  tracing::error!(error=%e,"CRITICAL: physical disk sync failed");
                              }
                          }
                      }



                      _ = bgrwriter_interval.tick() => {
                      is_rewriting = true;
                      let db_detached = Arc::clone(&db_bg_clone);
                      let tx_detached = tx_bg_clone.clone();
                      let compaction_span=tracing::info_span!("AOF_compaction_process");
                      tokio::spawn(async move {


                      tracing::info!("starting background AOF writer");

                          let temp_file = OpenOptions::new()
                              .create(true)
                              .write(true)
                              .truncate(true)
                              .open("temp_aof")
                              .await
                              .unwrap();

                          let mut temp_writer = tokio::io::BufWriter::with_capacity(8192, temp_file);


                          for current_shard_index in 0..64 {
                              let mut shard_buffer = BytesMut::new();
                              {
                              let read_span = tracing::info_span!("compactor_read_lock",shard=current_shard_index);
                              let _compaction_guard = read_span.entered();

                              let finder = db_detached.shards[current_shard_index].read().unwrap();

                              for (k, v) in finder.iter() {
                                  let is_valid = match v.1 {
                                      Some(time) => time > Instant::now(),
                                      None => true,
                                  };

                                  if is_valid {

                                       let mut aof_array = vec![
                                          RespFrame::BulkString(bytes::Bytes::from("SET")),
                                          RespFrame::BulkString(k.clone()),
                                          (*v.0).clone(),
                                      ];

                                      if let Some(expiration_instant) = v.1 {
                                      let time_left = expiration_instant.duration_since(Instant::now()).as_secs();
                                      aof_array.push(RespFrame::BulkString(bytes::Bytes::from("EX")));
                                      aof_array.push(RespFrame::BulkString(bytes::Bytes::from(time_left.to_string())));
                                  }

                                      let encoder_value = RespFrame::Array(aof_array);
                                      let mut codec = RespCodec;
                                      codec.encode(encoder_value, &mut shard_buffer).unwrap();

                                  }
                              }
                              }

                          if !shard_buffer.is_empty() {
                          temp_writer.write_all(&shard_buffer).await.unwrap();
                           }
                           }
                          temp_writer.flush().await.unwrap();
                          temp_writer.get_ref().sync_data().await.unwrap();
                          tx_detached.send(DbMessage::ExecuteAtomicSwap).await.unwrap();

                  }.instrument(compaction_span));
                  }

            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    tracing::info!(port = 6379, "Data store engine is running");

    replay_aof(db.clone()).await;

    let db_del_clone = Arc::clone(&db);

    tokio::spawn(async move {
        let mut current_shard_index = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            for _ in 0..16 {
                let target_shards = current_shard_index % 64;
                let mut keys_to_delete: Vec<Bytes> = Vec::new();
                {
                    let mut pen = db_del_clone.shards[target_shards].write().unwrap();
                    for (k, v) in pen.iter() {
                        if let Some(time) = v.1
                            && time < Instant::now()
                        {
                            keys_to_delete.push(k.clone());
                        }
                    }
                    for k in keys_to_delete {
                        pen.remove(&k);
                    }
                }
                current_shard_index += 1;
            }
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::info!(current_ip = %addr,"New connection established ");
        socket.set_nodelay(true).unwrap();

        let db_clone = Arc::clone(&db);
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, RespCodec);

            while let Some(result) = framed.next().await {
                match result {
                    Ok(frame) => {
                        let response = match Command::from_frame(frame) {
                            Ok(Command::Ping) => RespFrame::SimpleString("PONG".to_string()),

                            Ok(Command::Config) => RespFrame::SimpleString("OK".to_string()),

                            Ok(Command::Set { key, value, time }) => {
                                let expiration_time =
                                    time.map(|t| Instant::now() + Duration::from_secs(t));
                                let mut buffer = BytesMut::new();
                                let mut aof_array = vec![
                                    RespFrame::BulkString(bytes::Bytes::from("SET")),
                                    RespFrame::BulkString(key.clone()),
                                    value.clone(),
                                ];

                                if let Some(time) = time {
                                    aof_array.push(RespFrame::BulkString(bytes::Bytes::from("EX")));
                                    aof_array.push(RespFrame::BulkString(bytes::Bytes::from(
                                        time.to_string(),
                                    )));
                                }

                                let encoder_value = RespFrame::Array(aof_array);
                                let mut codec = RespCodec;
                                codec.encode(encoder_value, &mut buffer).unwrap();
                                let msg = DbMessage::WriteBytes(buffer.freeze());
                                let room = db_clone.get_shard_index(&key);

                                {
                                    let lock_span =
                                        tracing::info_span!("acquire_write_lock", shard = room);
                                    let _guard = lock_span.entered();
                                    let mut pen = db_clone.shards[room].write().unwrap();
                                    pen.insert(
                                        key.clone(),
                                        (Arc::new(value.clone()), expiration_time),
                                    );
                                }

                                if let Err(e) = tx_clone.send(msg).await {
                                    tracing::error!(error = %e,"Failed to send to background worker");
                                }
                                RespFrame::SimpleString("OK".to_string())
                            }
                            Ok(Command::Get { key }) => {
                                let mut is_expired = false;
                                let mut return_frame = RespFrame::SimpleString("Null".to_string());
                                let room = db_clone.get_shard_index(&key);

                                {
                                    let read_span =
                                        tracing::info_span!("acquire_read_lock", shard = room);
                                    let _guard = read_span.entered();
                                    let finder = db_clone.shards[room].read().unwrap();
                                    if let Some(data) = finder.get(&key) {
                                        if let Some(time_limit) = data.1 {
                                            if Instant::now() > time_limit {
                                                is_expired = true;
                                            } else {
                                                return_frame = (*data.0).clone();
                                            }
                                        } else {
                                            return_frame = (*data.0).clone();
                                        }
                                    }
                                }
                                if is_expired {
                                    let lock_span =
                                        tracing::info_span!("acquire_write_lock", shard = room);
                                    let _guard = lock_span.entered();
                                    let mut hunter = db_clone.shards[room].write().unwrap();

                                    hunter.remove(&key);
                                }

                                return_frame
                            }
                            Ok(Command::Del { key }) => {
                                let room = db_clone.get_shard_index(&key);
                                {
                                    let lock_span =
                                        tracing::info_span!("acquire_write_lock", shard = room);
                                    let _guard = lock_span.entered();
                                    let mut hunter = db_clone.shards[room].write().unwrap();
                                    match hunter.remove(&key) {
                                        Some(_) => RespFrame::Integer(1),
                                        None => RespFrame::Integer(0),
                                    }
                                }
                            }
                            Ok(Command::Exist { key }) => {
                                let room = db_clone.get_shard_index(&key);
                                {
                                    let read_span =
                                        tracing::info_span!("acquire_read_lock", shard = room);
                                    let _guard = read_span.entered();
                                    let finder = db_clone.shards[room].read().unwrap();
                                    match finder.contains_key(&key) {
                                        true => RespFrame::Integer(1),
                                        false => RespFrame::Integer(0),
                                    }
                                }
                            }

                            Ok(Command::Unknown) => {
                                tracing::info!("Received an unknown or unsupported command.");
                                RespFrame::Error("ERR unknown command".to_string())
                            }
                            Err(err) => {
                                tracing::error!(error = %err,"Protocol Error");
                                RespFrame::Error(err)
                            }
                        };

                        if let Err(e) = framed.send(response).await {
                            tracing::error!(error = %e,"Failed to send response");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e,"Error parsing network frame");
                        break;
                    }
                }
            }

            tracing::info!(client_ip = %addr,"Client disconnected.");
        });
    }
}
