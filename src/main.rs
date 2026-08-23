use bytes::BytesMut;
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
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tokio_util::codec::{Decoder, Encoder};

mod cmd;
mod frame;
use crate::cmd::Command;
use crate::frame::{RespCodec, RespFrame};

const NUM_SHARDS: usize = 64;

type DbData = HashMap<String, (Arc<RespFrame>, Option<Instant>)>;
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

    pub fn get_shard_index(&self, key: &String) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        (hash_value as usize) % NUM_SHARDS
    }
}
async fn write_to_aof(command_bytes: &BytesMut) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("database_aof")
        .await?;
    file.write_all(command_bytes).await?;
    Ok(())
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
                    println!("AOF Replay Complete. Database restored into RAM.");
                    break;
                }
                buffer.extend_from_slice(&chunk[..bytes_read]);
                loop {
                    match codec.decode(&mut buffer) {
                        Ok(Some(frame)) => {
                             if let Ok(Command::Set { key, value, time }) = Command::from_frame(frame) {
                                 let expiration_time = time.map(|t| Instant::now() + Duration::from_secs(t));
                                let room = db.get_shard_index(&key);
                              {
                                   let mut pen = db.shards[room].write().unwrap();
                                   pen.insert(key, (Arc::new(value.clone()), expiration_time));
                              }
                          };

                        }
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("AOF Parsing Error: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }
        Err(_) => {
            println!("No AOF file found. Starting with a fresh database.");
        }
    }
}
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Data store engine is running on port 6379...");

    let db = Arc::new(ShardedDb::new());
    replay_aof(db.clone()).await;

    let db_del_clone = Arc::clone(&db);
    tokio::spawn(async move {
        let mut current_shard_index = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            for _ in 0..16 {
                let target_shards = current_shard_index % 64;
                let mut keys_to_delete: Vec<String> = Vec::new();
                {
                    let mut pen = db_del_clone.shards[target_shards].write().unwrap();
                    for (k, v) in pen.iter() {
                       if let Some(time) = v.1
                     && time < Instant::now() {
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
        println!("New connection established from: {}", addr);

        let db_clone = Arc::clone(&db);

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, RespCodec);

            while let Some(result) = framed.next().await {
                match result {
                    Ok(frame) => {
                     let response =  match Command::from_frame(frame) {
                            Ok(Command::Set { key, value, time }) => {
                               let expiration_time = time.map(|t| Instant::now() + Duration::from_secs(t));
                                let mut buffer = BytesMut::new();
                                let mut aof_array = vec![
                                    RespFrame::BulkString(bytes::Bytes::from("SET")),
                                    RespFrame::BulkString(bytes::Bytes::from(key.clone())),
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
                                let room = db_clone.get_shard_index(&key);

                                {
                                    let mut pen = db_clone.shards[room].write().unwrap();
                                    pen.insert(key, (Arc::new(value.clone()), expiration_time));
                                }

                                write_to_aof(&buffer).await.unwrap();
                                RespFrame::SimpleString("OK".to_string())
                            }
                            Ok(Command::Get { key }) => {
                                let mut is_expired = false;
                                let mut return_frame = RespFrame::SimpleString("Null".to_string());
                                let room = db_clone.get_shard_index(&key);

                                {
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
                                    let mut hunter = db_clone.shards[room].write().unwrap();

                                    hunter.remove(&key);
                                }

                                return_frame
                            }
                            Ok(Command::Del { key }) => {
                                let room = db_clone.get_shard_index(&key);
                                {
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
                                    let finder = db_clone.shards[room].read().unwrap();
                                    match finder.contains_key(&key) {
                                        true => RespFrame::Integer(1),
                                        false => RespFrame::Integer(0),
                                    }
                                }
                            }

                            Ok(Command::Unknown) => {
                                println!("Received an unknown or unsupported command.");
                                RespFrame::Error("ERR unknown command".to_string())
                            }
                            Err(err) => {
                                println!("Protocol Error: {}", err);
                                RespFrame::Error(err)
                            }
                        };

                        if let Err(e) = framed.send(response).await {
                            println!("Failed to send response: {:?}", e);
                        }
                    }
                    Err(e) => {
                        println!("Error parsing network frame: {:?}", e);
                        break;
                    }
                }
            }

            println!("Client {} disconnected.", addr);
        });
    }
}
