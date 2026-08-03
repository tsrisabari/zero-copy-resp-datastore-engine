use bytes::Bytes;
use std::io;
use tokio_util::codec::Encoder;
use tokio_util::codec::Decoder;

#[derive(Debug,Clone,PartialEq)]

pub enum RespFrame{
        SimpleString(String),
        Error(String),
        Integer(i64),
        BulkString(Bytes),
        Array(Vec<RespFrame>),
        Null,
        }
        
        
use bytes::BytesMut;

pub enum RespError{
    Incomplete,
    InvalidProtocol,
    }
    

    pub fn decode(buf:&mut BytesMut) -> Result<RespFrame,RespError> {
    if buf.is_empty(){
    return Err(RespError::Incomplete)
    }
    match buf[0]{
        b'+'=>{
            let _=buf.split_to(1);
            let line = get_line(buf)?;
            let string=String::from_utf8(line.to_vec()).map_err(|_| RespError::InvalidProtocol)?;
            Ok(RespFrame::SimpleString(string))
        },
        b'-'=>{
            let _=buf.split_to(1);
            let line = get_line(buf)?;
            let string=String::from_utf8(line.to_vec()).map_err(|_| RespError::InvalidProtocol)?;
            Ok(RespFrame::Error(string))
        },
        b':'=>{
            let _=buf.split_to(1);
            let line=get_line(buf)?;
            let string=String::from_utf8(line.to_vec()).map_err(|_| RespError::InvalidProtocol)?;
            let number=string.parse::<i64>().map_err(|_| RespError::InvalidProtocol)?;
            Ok(RespFrame::Integer(number))
        },
        b'$'=>{
            let _=buf.split_to(1);
            let byte_len=get_line(buf)?;
            let len_string=String::from_utf8(byte_len.to_vec()).map_err(|_| RespError::InvalidProtocol)?;
            let len = len_string.parse::<i64>().map_err(|_| RespError::InvalidProtocol)?;
            if len == -1 {
                return Ok(RespFrame::Null);
            }
            let len= len as usize;
            if buf.len()<len+2{
              return  Err(RespError::Incomplete);
            }
            let data=buf.split_to(len).freeze();
            let _=buf.split_to(2);
            Ok(RespFrame::BulkString(data))
        
        },
        b'*'=>{
            let _=buf.split_to(1);
            let byte_len=get_line(buf)?;
            let string=String::from_utf8(byte_len.to_vec()).map_err(|_| RespError::InvalidProtocol)?;
            let len=string.parse::<i64>().map_err(|_| RespError::InvalidProtocol)?;
            if len==-1{
            return Ok(RespFrame::Null);
            }
            let mut frames=Vec::with_capacity(len as usize);
            for _ in 0..len{
                frames.push(decode(buf)?);
               } 
            Ok(RespFrame::Array(frames))
        },
        _=>Err(RespError::InvalidProtocol),
    }
    }
    
    impl RespFrame{
        pub fn encode(&self,buf:&mut BytesMut){
        match self{
            RespFrame::SimpleString(s)=>{
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            
            }
            RespFrame::Error(e)=>{
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(e.as_bytes());
                buf.extend_from_slice(b"\r\n");
            
            
            }
              RespFrame::Integer(i)=>{
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            
            
            }
            RespFrame::BulkString(b)=>{
                buf.extend_from_slice(b"$");
                buf.extend_from_slice(b.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(b);
                buf.extend_from_slice(b"\r\n");
                },
            RespFrame::Array(frames)=>{
                buf.extend_from_slice(b"*");
                buf.extend_from_slice(frames.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for frame in frames{
                    frame.encode(buf);
                    }
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Null => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            }
            }
            }
    
    
    
    fn find_crlf(buf: &[u8]) -> Option<usize> {
    if buf.len()==0{return None;}
    if buf.len()>0{
    for i in 0..(buf.len())-1{
        if buf[i]==b'\r' && buf[i+1]==b'\n'{
        return Some(i);
        }
        }
        } 
      return None;
}

fn get_line(buf: &mut BytesMut)->Result<BytesMut,RespError> {
    
    let get=find_crlf(buf);
    
   match get {
        
        Some(index) => {
            
            let line = buf.split_to(index);
            
            
            let _=buf.split_to(2);
            
            
            Ok(line)
        }
        
        None => {
            
            Err(RespError::Incomplete)
        }
    }
}
#[derive(Debug)]
pub struct RespCodec;
impl Encoder<RespFrame> for RespCodec{
    type Error=io::Error;
    
    fn encode(&mut self,item:RespFrame,dst:&mut BytesMut)-> Result<(), std::io::Error>
    {
    item.encode(dst);
    Ok(())
    }
    }
impl Decoder for RespCodec{
    type Error=io::Error;
    type Item=RespFrame;
    
    fn decode(&mut self,src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>{
        match decode(src){
            Ok(frame)=>Ok(Some(frame)),
            Err(_) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Protocol Error")),
        }
    }
}
