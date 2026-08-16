use bytes::{Bytes, BytesMut, Buf};
use std::io::Cursor;
use tokio_util::codec::{Encoder, Decoder};
use std::io;

#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes), 
    Array(Vec<RespFrame>),
    Null,
}

pub enum RespError {
    Incomplete,
    InvalidProtocol,
}

fn parse_frame(src: &mut BytesMut) -> Result<Option<RespFrame>, std::io::Error> {
    if src.is_empty() {
        return Ok(None);
    }
    
    let mut cursor = Cursor::new(&src[..]);
    
    match cursor.get_u8() {
        
        b'+' => {
            let chunk = cursor.chunk();
            if let Some(crlf_pos) = find_crlf(chunk) {
                let text_bytes = &chunk[..crlf_pos];
                let text = String::from_utf8_lossy(text_bytes).into_owned();
                
                cursor.advance(crlf_pos + 2); // 
                let total_bytes_moved = cursor.position() as usize;
                src.advance(total_bytes_moved);
                
                return Ok(Some(RespFrame::SimpleString(text)));
            } else {
                return Ok(None);
            }
        }
        
        
        b':' => {
            let chunk = cursor.chunk();
            if let Some(crlf_pos) = find_crlf(chunk) {
                let num_bytes = &chunk[..crlf_pos];
                let num_str = String::from_utf8_lossy(num_bytes);
                
                let number: i64 = match num_str.parse() {
                    Ok(n) => n,
                    Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid Integer")),
                };
                
                cursor.advance(crlf_pos + 2); 
                let total_bytes_moved = cursor.position() as usize;
                src.advance(total_bytes_moved);
                
                return Ok(Some(RespFrame::Integer(number)));
            } else {
                return Ok(None);
            }
        }
        
        
        b'*' => {
            let chunk = cursor.chunk(); 
            if let Some(crlf_pos) = find_crlf(chunk) {
                let num_bytes = &chunk[..crlf_pos]; 
                let num_str = String::from_utf8_lossy(num_bytes);
                let array_len: usize = match num_str.parse() {
                    Ok(num) => num,
                    Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid Array Len")),
                };
                
                cursor.advance(crlf_pos + 2);
                
              
                for _ in 0..array_len {
                    if !cursor.has_remaining() {
                        return Ok(None);
                    }
                    
                   
                    match cursor.get_u8() { 
                        b'$' => {
                            let chunk = cursor.chunk();
                            if let Some(crlf_pos) = find_crlf(chunk) {
                                let len_str = String::from_utf8_lossy(&chunk[..crlf_pos]);
                                let str_len: usize = match len_str.parse() {
                                    Ok(num) => num,
                                    Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid String Len")),
                                };
                                
                                cursor.advance(crlf_pos + 2);
                                
                                if cursor.remaining() < str_len + 2 {
                                    return Ok(None);
                                }
                                cursor.advance(str_len + 2);
                            } else {
                                return Ok(None);
                            }
                        }
                        _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Expected BulkString")),
                    }
                }
            
           
            let header_len = find_crlf(&src[..]).unwrap();
            src.advance(header_len + 2);
            
            let mut frames = Vec::new();
            for _ in 0..array_len {
                src.advance(1); 
                
                let len_pos = find_crlf(&src[..]).unwrap(); 
                let len_str = String::from_utf8_lossy(&src[..len_pos]); 
                let str_len: usize = len_str.parse().unwrap();
                
                src.advance(len_pos + 2); 
                
                
                let zero_copy_text = src.split_to(str_len).freeze();
                frames.push(RespFrame::BulkString(zero_copy_text));
                
                src.advance(2); 
            }
            
            return Ok(Some(RespFrame::Array(frames)));
        } 
        else {
                return Ok(None);
            }
        }
        
        _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid Protocol")),
    }
}


fn find_crlf(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() { return None; }
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

impl RespFrame {
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            RespFrame::SimpleString(s) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Error(e) => {
                buf.extend_from_slice(b"-"); 
                buf.extend_from_slice(e.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Integer(i) => {
                buf.extend_from_slice(b":"); 
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::BulkString(b) => {
                buf.extend_from_slice(b"$");
                buf.extend_from_slice(b.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(b);
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Array(frames) => {
                buf.extend_from_slice(b"*");
                buf.extend_from_slice(frames.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for frame in frames {
                    frame.encode(buf);
                }
            }
            RespFrame::Null => {
                buf.extend_from_slice(b"$-1\r\n");
            }
        }
    }
}

#[derive(Debug)]
pub struct RespCodec;

impl Encoder<RespFrame> for RespCodec {
    type Error = io::Error;
    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), std::io::Error> {
        item.encode(dst);
        Ok(())
    }
}

impl Decoder for RespCodec {
    type Error = io::Error;
    type Item = RespFrame;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        parse_frame(src)
    }
}
