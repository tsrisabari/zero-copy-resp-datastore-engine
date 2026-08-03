use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::{StreamExt,SinkExt};
use bytes::BytesMut;

mod frame;
use frame::{RespCodec,RespFrame};
#[tokio::main]
async fn main()->std::io::Result<()>{
    let listener=TcpListener::bind("127.0.0.1:6379").await?;
    println!("Data store engine is running on port 6379...");
    loop{
        let(socket,addr)=listener.accept().await?;
        println!("New connection established from:{}",addr);
        tokio::spawn(async move {
            let mut framed=Framed::new(socket,RespCodec);
            while let Some(result)=framed.next().await{
            match result{
                Ok(frame)=>{
                    println!("Received Command:{:?}",framed);
                    let response = RespFrame::SimpleString("PONG".to_string());
                    if let Err(e)=framed.send(response).await{
                        println!("Failed to send response:{:?}",e);
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
                    
