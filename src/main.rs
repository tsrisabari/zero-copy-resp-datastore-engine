use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::{StreamExt,SinkExt};
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::hash::{Hash,Hasher};
use std::collections::hash_map::DefaultHasher;
use std::process::Command;


mod frame;
use frame::{RespCodec,RespFrame};
const NUM_SHARDS:usize=64;
pub struct ShardedDb
{
    pub shards:Vec<Mutex<HashMap<String,RespFrame>>>,
    }
impl ShardedDb{
        pub fn new()->ShardedDb{
        let mut shards= Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS{
            shards.push(Mutex::new(HashMap::new()));
            }
            ShardedDb{shards}
            }
        pub fn get_shard_index(&self,key:&String)->usize{
        let mut hasher=DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_value=hasher.finish();
        (hash_value as usize)%NUM_SHARDS
        }
    }
#[tokio::main]
async fn main()->std::io::Result<()>{
    let listener=TcpListener::bind("127.0.0.1:6379").await?;
    println!("Data store engine is running on port 6379...");
    let db = Arc::new(ShardedDb::new());
    loop{
        let(socket,addr)=listener.accept().await?;
        println!("New connection established from:{}",addr);
        let db_clone=Arc::clone(&db);
        tokio::spawn(async move {
            let mut framed=Framed::new(socket,RespCodec);
            while let Some(result)=framed.next().await{
            match result{
                Ok(frame)=>{
                    let command=Command::from_frame(frame);
                    println!("Executing:{}",command);
                    let response = match command{
                        Command::PING=>{
                        RespFrame::SimpleString("PONG".to_string())
                        }
                        Command::Set(key,value)=>
                        {
                            let room_num=db_clone.get_shard_index(&key);
                            let mut map=db.clone.shards[room_num].lock().unwrap();
                            map.insert(key,value);
                            RespFrame::SimpleString("Ok".to_string())
                            }
                        Command::Get(key)=>{
                            let room_num=db_clone.get_shard_index(&key);
                            let map=db_clone.shards[room_num].lock().unwrap();
                            match map.get(&key)=>{
                            Some(stored_value)=>stored_value.clone()
                            }
                            None=>{
                            RespFrame::Null
                            }
                            }
                            
                            Command::unknown=>{
                                RespFrame::Error("Err unknown command".to_string())
                                
                                }
                                };
                            
                            if let Err(e)=framed.send(response).await{
                        println!("Failed to send response:{:?}",e);
                        }}
                        
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
                    
