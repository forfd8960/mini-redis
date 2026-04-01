use crate::{
    command::{
        Command, GenericCommand, ListCommand, StringCommand, is_generic_command, is_hash_command,
        is_list_command, is_set_command, is_sorted_set_command, is_string_command,
    },
    errors::RedisError,
    protocol::list::decode_list_command,
    storage::{SetCondition, SetOptions, SetTTL},
};

use redis_protocol::resp2::types::OwnedFrame as Frame;

// decode commands from the Redis protocol
pub fn decode_frame(frame: Frame) -> Result<Command, RedisError> {
    let args = extract_args_from_frame(frame)?;
    if args.is_empty() {
        return Err(RedisError::ProtocolError("Empty command".to_string()));
    }

    let cmd_name = args[0].to_uppercase();
    match cmd_name.as_str() {
        _ if is_generic_command(cmd_name.as_str()) => decode_generic_command(args.as_slice()),
        _ if is_string_command(cmd_name.as_str()) => decode_string_command(args.as_slice()),
        _ if is_hash_command(cmd_name.as_str()) => decode_hash_command(args.as_slice()),
        _ if is_list_command(cmd_name.as_str()) => decode_list_command(args.as_slice()),
        _ if is_set_command(cmd_name.as_str()) => decode_set_command(args.as_slice()),
        _ if is_sorted_set_command(cmd_name.as_str()) => decode_sorted_set_command(args.as_slice()),
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown command: {}",
            cmd_name
        ))),
    }
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/
/// Clients send commands to a Redis server as an array of bulk strings.
/// The first (and sometimes also the second) bulk string in the array is the command's name. Subsequent elements of the array are the arguments for the command.
fn extract_args_from_frame(frame: Frame) -> Result<Vec<String>, RedisError> {
    match frame {
        Frame::Array(arr) => {
            let mut args = Vec::new();
            for cmd in arr {
                match cmd {
                    Frame::BulkString(bs) => {
                        let arg = String::from_utf8(bs)
                            .map_err(|e| RedisError::ProtocolError(e.to_string()))?;
                        args.push(arg);
                    }
                    Frame::SimpleString(ss) => {
                        let arg = String::from_utf8(ss)
                            .map_err(|e| RedisError::ProtocolError(e.to_string()))?;
                        args.push(arg);
                    }
                    _ => {
                        return Err(RedisError::ProtocolError(format!(
                            "Unsupported frame type in array: {:?}",
                            cmd
                        )));
                    }
                }
            }

            Ok(args)
        }
        _ => Err(RedisError::ProtocolError(format!(
            "Expected array frame, got: {:?}",
            frame
        ))),
    }
}

fn decode_generic_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];
    validate_generic_command_args(cmd_name.as_str(), args)?;

    match cmd_name.as_str() {
        "PING" => {
            if args.is_empty() {
                Ok(Command::Generic(GenericCommand::Ping(None)))
            } else {
                Ok(Command::Generic(GenericCommand::Ping(Some(
                    args[0].clone(),
                ))))
            }
        }
        "ECHO" => Ok(Command::Generic(GenericCommand::Echo(args[0].clone()))),
        "EXISTS" => Ok(Command::Generic(GenericCommand::Exists(args.to_vec()))),
        "EXPIRE" => {
            let key = args[0].clone();
            let ttl = args[1].parse::<u64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid TTL value for EXPIRE: {}", e))
            })?;
            Ok(Command::Generic(GenericCommand::Expire(key, ttl)))
        }
        "DEL" => Ok(Command::Generic(GenericCommand::Del(args[0].clone()))),
        "TTL" => Ok(Command::Generic(GenericCommand::TTL(args[0].clone()))),
        "KEYS" => Ok(Command::Generic(GenericCommand::Keys(args[0].clone()))),
        "TYPE" => Ok(Command::Generic(GenericCommand::Type(args[0].clone()))),
        "SCAN" => build_scan_command(args),
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown generic command: {}",
            cmd_name
        ))),
    }
}

fn build_scan_command(args: &[String]) -> Result<Command, RedisError> {
    let cursor = args[0]
        .parse::<i64>()
        .map_err(|e| RedisError::ProtocolError(format!("Invalid cursor value for SCAN: {}", e)))?;
    let pattern = if args.len() > 1 && args[1].to_uppercase() == "MATCH" {
        Some(args[2].clone())
    } else {
        None
    };
    let count = if args.len() > 3 && args[3].to_uppercase() == "COUNT" {
        Some(args[4].parse::<usize>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid count value for SCAN: {}", e))
        })?)
    } else {
        None
    };
    let scan_type = if args.len() > 5 && args[5].to_uppercase() == "TYPE" {
        Some(args[6].clone())
    } else {
        None
    };

    Ok(Command::Generic(GenericCommand::Scan(
        cursor, pattern, count, scan_type,
    )))
}

fn validate_generic_command_args(cmd_name: &str, args: &[String]) -> Result<(), RedisError> {
    match cmd_name {
        "PING" | "SCAN" => Ok(()),
        "EXISTS" => {
            if args.is_empty() {
                return Err(RedisError::ProtocolError(format!(
                    "EXISTS requires at least 1 argument"
                )));
            }
            Ok(())
        }
        "EXPIRE" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "EXPIRE requires exactly 2 arguments"
                )));
            }
            Ok(())
        }
        "ECHO" | "DEL" | "TTL" | "KEYS" | "TYPE" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires exactly 1 argument",
                    cmd_name
                )));
            }
            Ok(())
        }
        _ => {
            return Err(RedisError::ProtocolError(format!(
                "Unknown generic command: {}",
                cmd_name
            )));
        }
    }
}

fn decode_string_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        "GET" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "GET requires exactly 1 argument"
                )));
            }
            Ok(Command::String(StringCommand::Get(args[0].clone())))
        }

        "SET" => build_set_command(args),

        "INCR" | "DECR" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires exactly 1 argument",
                    cmd_name
                )));
            }
            let key = args[0].clone();
            if cmd_name == "INCR" {
                Ok(Command::String(StringCommand::Incr(key)))
            } else {
                Ok(Command::String(StringCommand::Decr(key)))
            }
        }

        "INCRBY" | "DECRBY" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires exactly 2 arguments",
                    cmd_name
                )));
            }
            let key = args[0].clone();
            let increment = args[1].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!(
                    "Invalid increment value for {}: {}",
                    cmd_name, e
                ))
            })?;
            if cmd_name == "INCRBY" {
                Ok(Command::String(StringCommand::IncrBy { key, increment }))
            } else {
                Ok(Command::String(StringCommand::DecrBy {
                    key,
                    decrement: increment,
                }))
            }
        }

        "MGET" => {
            if args.is_empty() {
                return Err(RedisError::ProtocolError(format!(
                    "MGET requires at least 1 argument"
                )));
            }
            Ok(Command::String(StringCommand::Mget {
                keys: args.to_vec(),
            }))
        }

        "MSET" => {
            if args.len() % 2 != 0 || args.is_empty() {
                return Err(RedisError::ProtocolError(format!(
                    "MSET requires an even number of arguments (key-value pairs)"
                )));
            }
            let mut pairs = Vec::new();
            for i in (0..args.len()).step_by(2) {
                pairs.push((args[i].clone(), args[i + 1].clone()));
            }
            Ok(Command::String(StringCommand::Mset { pairs }))
        }

        "GETRANGE" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "GETRANGE requires exactly 3 arguments"
                )));
            }
            let key = args[0].clone();
            let start = args[1].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid start value for GETRANGE: {}", e))
            })?;
            let end = args[2].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid end value for GETRANGE: {}", e))
            })?;
            Ok(Command::String(StringCommand::GetRange { key, start, end }))
        }

        "SETRANGE" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "SETRANGE requires exactly 3 arguments"
                )));
            }
            let key = args[0].clone();
            let offset = args[1].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid offset value for SETRANGE: {}", e))
            })?;
            let value = args[2].clone();
            Ok(Command::String(StringCommand::SetRange {
                key,
                offset,
                value,
            }))
        }

        "APPEND" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "APPEND requires exactly 2 arguments",
                )));
            }
            let key = args[0].clone();
            let value = args[1].clone();
            Ok(Command::String(StringCommand::Append { key, value }))
        }

        "STRLEN" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "STRLEN requires exactly 1 argument",
                )));
            }
            Ok(Command::String(StringCommand::StrLen {
                key: args[0].clone(),
            }))
        }

        _ => Err(RedisError::ProtocolError(format!(
            "Unknown string command: {}",
            cmd_name
        ))),
    }
}

fn build_set_command(args: &[String]) -> Result<Command, RedisError> {
    let key = args[0].clone();
    let value = args[1].clone();
    let mut options = SetOptions::default();

    let mut i = 2;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing seconds value for EX option".to_string(),
                    ));
                }
                options.ttl = Some(SetTTL::EX(args[i].parse::<u64>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid seconds value for EX: {}", e))
                })?));
            }
            "PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing milliseconds value for PX option".to_string(),
                    ));
                }
                options.ttl = Some(SetTTL::PX(args[i].parse::<u64>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid milliseconds value for PX: {}", e))
                })?));
            }
            "EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing timestamp-seconds value for EXAT option".to_string(),
                    ));
                }
                options.ttl = Some(SetTTL::EXAT(args[i].parse::<u64>().map_err(|e| {
                    RedisError::ProtocolError(format!(
                        "Invalid timestamp-seconds value for EXAT: {}",
                        e
                    ))
                })?));
            }
            "PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing timestamp-milliseconds value for PXAT option".to_string(),
                    ));
                }

                options.ttl = Some(SetTTL::PXAT(args[i].parse::<u64>().map_err(|e| {
                    RedisError::ProtocolError(format!(
                        "Invalid timestamp-milliseconds value for PXAT: {}",
                        e
                    ))
                })?));
            }
            "KEEPTTL" => options.ttl = Some(SetTTL::KeepTTL),
            "NX" => options.condition = Some(SetCondition::NX),
            "XX" => options.condition = Some(SetCondition::XX),
            "GET" => options.get = true,
            _ => {
                return Err(RedisError::ProtocolError(format!(
                    "Unknown option for SET command: {}",
                    args[i]
                )));
            }
        }
        i += 1;
    }

    Ok(Command::String(StringCommand::Set {
        key,
        value,
        options,
    }))
}

fn decode_hash_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown hash command: {}",
            cmd_name
        ))),
    }
}

fn decode_set_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown set command: {}",
            cmd_name
        ))),
    }
}

fn decode_sorted_set_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown sorted set command: {}",
            cmd_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::value::{ListInsertPivot, ListMoveDirection};

    use super::*;

    #[test]
    fn test_extract_args_from_frame() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"SET".to_vec()),
            Frame::BulkString(b"mykey".to_vec()),
            Frame::BulkString(b"myvalue".to_vec()),
        ]);

        let args = extract_args_from_frame(frame).unwrap();
        assert_eq!(
            args,
            vec![
                "SET".to_string(),
                "mykey".to_string(),
                "myvalue".to_string()
            ]
        );
    }

    #[test]
    fn test_decode_generic_command_ping() {
        let frame = Frame::Array(vec![Frame::BulkString(b"PING".to_vec())]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(cmd, Command::Generic(GenericCommand::Ping(None)));

        let frame = Frame::Array(vec![
            Frame::BulkString(b"PING".to_vec()),
            Frame::BulkString(b"Hello".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Generic(GenericCommand::Ping(Some("Hello".to_string())))
        );
    }

    #[test]
    fn test_decode_generic_command_scan() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"SCAN".to_vec()),
            Frame::BulkString(b"0".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Generic(GenericCommand::Scan(0, None, None, None))
        );

        let frame = Frame::Array(vec![
            Frame::BulkString(b"SCAN".to_vec()),
            Frame::BulkString(b"0".to_vec()),
            Frame::BulkString(b"MATCH".to_vec()),
            Frame::BulkString(b"user:*".to_vec()),
            Frame::BulkString(b"COUNT".to_vec()),
            Frame::BulkString(b"10".to_vec()),
            Frame::BulkString(b"TYPE".to_vec()),
            Frame::BulkString(b"hash".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Generic(GenericCommand::Scan(
                0,
                Some("user:*".to_string()),
                Some(10),
                Some("hash".to_string())
            ))
        );
    }

    #[test]
    fn test_decode_string_command_set() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"SET".to_vec()),
            Frame::BulkString(b"mykey".to_vec()),
            Frame::BulkString(b"myvalue".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::String(StringCommand::Set {
                key: "mykey".to_string(),
                value: "myvalue".to_string(),
                options: SetOptions::default(),
            })
        );
    }

    #[test]
    fn test_decode_string_command_set_with_options() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"SET".to_vec()),
            Frame::BulkString(b"mykey".to_vec()),
            Frame::BulkString(b"myvalue".to_vec()),
            Frame::BulkString(b"EX".to_vec()),
            Frame::BulkString(b"60".to_vec()),
            Frame::BulkString(b"NX".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::String(StringCommand::Set {
                key: "mykey".to_string(),
                value: "myvalue".to_string(),
                options: SetOptions {
                    ttl: Some(SetTTL::EX(60)),
                    condition: Some(SetCondition::NX),
                    get: false,
                },
            })
        );
    }

    #[test]
    fn test_decode_list_command_lpush() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LPUSH".to_vec()),
            Frame::BulkString(b"mylist".to_vec()),
            Frame::BulkString(b"value1".to_vec()),
            Frame::BulkString(b"value2".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::List(ListCommand::Lpush(
                "mylist".to_string(),
                vec!["value1".to_string(), "value2".to_string()]
            ))
        );
    }

    #[test]
    fn test_decode_list_command_lpop() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LPOP".to_vec()),
            Frame::BulkString(b"mylist".to_vec()),
            Frame::BulkString(b"2".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::List(ListCommand::Lpop("mylist".to_string(), 2))
        );
    }

    #[test]
    fn test_decode_list_command_lrange() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LRANGE".to_vec()),
            Frame::BulkString(b"mylist".to_vec()),
            Frame::BulkString(b"0".to_vec()),
            Frame::BulkString(b"-1".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::List(ListCommand::Lrange("mylist".to_string(), 0, -1))
        );
    }

    #[test]
    fn test_decode_list_insert() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LINSERT".to_vec()),
            Frame::BulkString(b"mylist".to_vec()),
            Frame::BulkString(b"BEFORE".to_vec()),
            Frame::BulkString(b"value".to_vec()),
            Frame::BulkString(b"newvalue".to_vec()),
        ]);

        let cmd = decode_frame(frame);
        println!("Decoded command: {:?}", cmd);

        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();

        assert_eq!(
            cmd,
            Command::List(ListCommand::LInsert {
                key: "mylist".to_string(),
                position: ListInsertPivot::Before,
                pivot: "value".to_string(),
                value: "newvalue".to_string(),
            })
        );
    }

    #[test]
    fn test_decode_list_move() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LMOVE".to_vec()),
            Frame::BulkString(b"source".to_vec()),
            Frame::BulkString(b"destination".to_vec()),
            Frame::BulkString(b"LEFT".to_vec()),
            Frame::BulkString(b"RIGHT".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::List(ListCommand::LMove {
                src: "source".to_string(),
                dest: "destination".to_string(),
                source_side: ListMoveDirection::Left,
                dest_side: ListMoveDirection::Right,
            })
        );
    }

    #[test]
    fn test_decode_list_blmove_with_timeout() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"BLMOVE".to_vec()),
            Frame::BulkString(b"source".to_vec()),
            Frame::BulkString(b"destination".to_vec()),
            Frame::BulkString(b"LEFT".to_vec()),
            Frame::BulkString(b"RIGHT".to_vec()),
            Frame::BulkString(b"5".to_vec()),
        ]);

        let cmd_res = decode_frame(frame);
        assert!(cmd_res.is_ok());
        let cmd = cmd_res.unwrap();

        assert_eq!(
            cmd,
            Command::List(ListCommand::BLmove {
                src: "source".to_string(),
                dest: "destination".to_string(),
                source_side: ListMoveDirection::Left,
                dest_side: ListMoveDirection::Right,
                timeout: 5,
            })
        );
    }

    #[test]
    fn test_decode_list_lrem() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LREM".to_vec()),
            Frame::BulkString(b"mylist".to_vec()),
            Frame::BulkString(b"value".to_vec()),
            Frame::BulkString(b"2".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::List(ListCommand::Lrem(
                "mylist".to_string(),
                "value".to_string(),
                2
            ))
        );
    }

    #[test]
    fn test_decode_list_ltrim() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"LTRIM".to_vec()),
            Frame::BulkString(b"mylist".to_vec()),
            Frame::BulkString(b"1".to_vec()),
            Frame::BulkString(b"3".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::List(ListCommand::LTrim("mylist".to_string(), 1, 3))
        );
    }
}
