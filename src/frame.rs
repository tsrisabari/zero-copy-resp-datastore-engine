#![allow(dead_code)]
use bytes::{Buf, Bytes, BytesMut};
use std::io;
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespFrame>),
    Null,
}


#[derive(Debug)]
pub enum RespError {
    Incomplete,
    InvalidProtocol(String),
    Io(std::io::Error),
}


impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::Incomplete => write!(f, "Incomplete frame"),
            RespError::InvalidProtocol(msg) => write!(f, "Invalid Protocol: {}", msg),
            RespError::Io(err) => write!(f, "IO Error: {}", err),
        }
    }
}


impl std::error::Error for RespError {}


impl From<std::io::Error> for RespError {
    fn from(err: std::io::Error) -> RespError {
        RespError::Io(err)
    }
}


fn parse_frame(src: &mut BytesMut) -> Result<Option<RespFrame>, RespError> {
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

                cursor.advance(crlf_pos + 2); 
                let total_bytes_moved = cursor.position() as usize;
                src.advance(total_bytes_moved);

                Ok(Some(RespFrame::SimpleString(text)))
            } else {
                Ok(None)
            }
        }

        b':' => {
            let chunk = cursor.chunk();
            if let Some(crlf_pos) = find_crlf(chunk) {
                let num_bytes = &chunk[..crlf_pos];
                let num_str = String::from_utf8_lossy(num_bytes);

                let number: i64 = match num_str.parse() {
                    Ok(n) => n,
                    Err(_) => {
                        
                        return Err(RespError::InvalidProtocol("Invalid Integer".to_string()));
                    }
                };

                cursor.advance(crlf_pos + 2);
                let total_bytes_moved = cursor.position() as usize;
                src.advance(total_bytes_moved);

                Ok(Some(RespFrame::Integer(number)))
            } else {
                Ok(None)
            }
        }

        b'*' => {
            let chunk = cursor.chunk();
            if let Some(crlf_pos) = find_crlf(chunk) {
                let num_bytes = &chunk[..crlf_pos];
                let num_str = String::from_utf8_lossy(num_bytes);
                let array_len: usize = match num_str.parse() {
                    Ok(num) => num,
                    Err(_) => {
                        return Err(RespError::InvalidProtocol("Invalid Array Len".to_string()));
                    }
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
                                    Err(_) => {
                                        return Err(RespError::InvalidProtocol("Invalid String Len".to_string()));
                                    }
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
                        _ => {
                            return Err(RespError::InvalidProtocol("Expected BulkString".to_string()));
                        }
                    };
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

                Ok(Some(RespFrame::Array(frames)))
            } else {
                Ok(None)
            }
        }

        _ => Err(RespError::InvalidProtocol("Invalid Protocol".to_string()))
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    (0..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
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
    type Error = RespError; 

    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

impl Decoder for RespCodec {
    type Error = RespError; 
    type Item = RespFrame;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        parse_frame(src)
    }
}
