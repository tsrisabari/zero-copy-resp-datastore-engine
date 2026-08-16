use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::{StreamExt, SinkExt};
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

mod cmd;
mod frame;
use crate::frame::{RespCodec, RespFrame};
use crate::cmd::Command; 

const NUM_SHARDS: usize = 64;

pub struct ShardedDb {
    pub shards: Vec<RwLock<HashMap<String, (Arc<RespFrame>,Option<Instant>)>>>,
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
async fn write_to_aof(command_str:&str)-> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("database_aof")
        .await?;
        file.write_all(command_str.as_bytes()).await?;
        Ok(())
        
        }
        
        
      

async fn replay_aof(db: Arc<ShardedDb>) {
    let file_result = File::open("database.aof").await;
    
    match file_result {
        Ok(file) => {
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

          
            while let Some(line) = lines.next_line().await.unwrap_or(None) {
                
               
                let parts: Vec<&str> = line.split_whitespace().collect();
                
             
                if parts.len() >= 3 && parts[0] == "SET" {
                    let key = parts[1].to_string();
                    let value = RespFrame::SimpleString(parts[2].to_string());
                    
                    let mut expiration_time = None;
                    
                    
                    if parts.len() == 5 && parts[3] == "EX" {
                        if let Ok(secs) = parts[4].parse::<u64>() {
                            expiration_time = Some(Instant::now() + Duration::from_secs(secs));
                        }
                    }

                    
                    let room = db.get_shard_index(&key);
                    {
                        let mut pen = db.shards[room].write().unwrap();
                        pen.insert(key, (Arc::new(value), expiration_time));
                    }
                }
            }
            println!("AOF Replay Complete. Database restored into RAM.");
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
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection established from: {}", addr);
        
        let db_clone = Arc::clone(&db);
        
        tokio::spawn(async move {
            let mut framed = Framed::new(socket, RespCodec);
            
            while let Some(result) = framed.next().await {
                match result {
                    Ok(frame) => {
                      
                        let response = match Command::from_frame(frame) {
                            Ok(Command::Set { key, value ,time}) => {
                               let expiration_time= match time{
                               Some(time)=>Some(Instant::now() + Duration::from_secs(time)),
                               None=>None,
                               };
                                let log_string=match time{
                                    Some(t)=>format!("{} {:?} EX {}\n",key,value,t),
                                    None=>format!("{} {:?}\n",key,value),
                                    };
                               let room = db_clone.get_shard_index(&key);
                               
                               {
                                let mut pen=db_clone.shards[room].write().unwrap();
                                pen.insert(key,(Arc::new(value),expiration_time));
                                }
                               
                                 write_to_aof(&log_string).await.unwrap();
                                RespFrame::SimpleString("OK".to_string())
                            }
                          Ok(Command::Get { key }) => {
                                 let mut is_expired = false;
                                 let mut return_frame = RespFrame::SimpleString("Null".to_string());
                                 let room = db_clone.get_shard_index(&key);

  
                                  {
                                  let finder = db_clone.shards[room].read().unwrap();
                                      match finder.get(&key) {
                                     Some(data) => {
                                     match data.1 {
                                    Some(time_limit) => {
                                      if Instant::now() > time_limit {
                                      is_expired = true;
                                 } else {
                            return_frame = (*data.0).clone();
                        }
                    }
                    None => {
                        
                        return_frame = (*data.0).clone();
                    }
                }
            }
             None => {
               
            }
        }
       }
    if is_expired {
        let mut hunter = db_clone.shards[room].write().unwrap();
        
        hunter.remove(&key);
    }


    return_frame
}
                            Ok(Command::Del { key }) =>{
                                let room =db_clone.get_shard_index(&key);
                                {
                                let mut hunter = db_clone.shards[room].write().unwrap();
                                match hunter.remove(&key){
                                Some(_)=>
                                RespFrame::Integer(1),
                                None=>
                                RespFrame::Integer(0),
                                }
                                }
                                }
                                Ok(Command::Exist { key }) => {
                                let room = db_clone.get_shard_index(&key);
                                {
                                let finder = db_clone.shards[room].read().unwrap();
                                match finder.contains_key(&key){
                                true=>
                                RespFrame::Integer(1),
                                false=>
                                RespFrame::Integer(0), 
                                }
                            }}
                                    
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
