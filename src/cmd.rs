use crate::frame::RespFrame;

    


#[derive(Debug)]
pub enum Command {
Get { key: String },
Set { key: String, value: RespFrame,time:Option<u64> },
Del { key: String },
Exist { key: String },
Unknown,
}

impl Command {

pub fn from_frame(frame: RespFrame) -> Result<Command, String> {

    let array = match frame {
        RespFrame::Array(arr) => arr,
        _ => return Err("ERR expected an array of strings".to_string()),
    };

    if array.is_empty() {
        return Err("ERR empty command".to_string());
    }

   
    let verb = match &array[0] {
       
        RespFrame::BulkString(bytes) => {
            String::from_utf8_lossy(bytes).to_uppercase()
        }
        
        RespFrame::SimpleString(s) => s.to_uppercase(),
        _ => return Err("ERR invalid command format".to_string()),
    };

   
    match verb.as_str() {
        "GET" => {
            
            if array.len() != 2 {
                return Err("ERR wrong number of arguments for 'get' command".to_string());
            }
            
                      let key = extract_string(&array[1])?;
            Ok(Command::Get { key })
        }
        "SET" => {
            
            if array.len() != 3 && array.len() != 5 {
                return Err("ERR wrong number of arguments for 'set' command".to_string());
            }
            if array.len()==5{
                let string=extract_string(&array[3])?;
                match string.to_uppercase().as_str(){
                    "EX"=>{
                    let second_string=extract_string(&array[4])?;
                    let second : u64 = match second_string.parse() {
                    Ok(n) => n,
                    Err(_) => return Err("Invalid integer for EX".to_string()),
                   };
                    let key = extract_string(&array[1])?;
                    let value = array[2].clone();
                    return Ok(Command::Set { key, value, time:Some(second) });
                    }
                    _=> return Ok(Command::Unknown),
                    }
                    }
            let key = extract_string(&array[1])?;
            let value = array[2].clone();
            Ok(Command::Set { key, value,time:None })
        }
        
        "DEL" => {
            
            if array.len() != 2 {
                return Err("ERR wrong number of arguments for 'del' command".to_string());
            }
            let key = extract_string(&array[1])?;
            Ok(Command::Del { key })
            }
            
        "EXIST" => {
            
            if array.len() != 2 {
                return Err("ERR wrong number of arguments for 'exist' command".to_string());
            }
            let key = extract_string(&array[1])?;
            Ok(Command::Exist { key })
        }
        _ => Ok(Command::Unknown),
    }
}


}


fn extract_string(frame: &RespFrame) -> Result<String, String> {
match frame {
RespFrame::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
RespFrame::SimpleString(s) => Ok(s.clone()),
_ => Err("ERR expected string argument".to_string()),
}
}
