use crate::{
    command::{
        Command, GenericCommand, SetOptions, StringCommand, is_generic_command, is_hash_command,
        is_list_command, is_set_command, is_sorted_set_command, is_string_command,
    },
    errors::RedisError,
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
                Ok(Command::Generic(GenericCommand::Ping("".to_string())))
            } else {
                Ok(Command::Generic(GenericCommand::Ping(args[0].clone())))
            }
        }
        "ECHO" => Ok(Command::Generic(GenericCommand::Echo(args[0].clone()))),
        "EXISTS" => Ok(Command::Generic(GenericCommand::Exists(args[0].clone()))),
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
        "EXPIRE" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "EXPIRE requires exactly 2 arguments"
                )));
            }
            Ok(())
        }
        "ECHO" | "EXISTS" | "DEL" | "TTL" | "KEYS" | "TYPE" => {
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
    unimplemented!()
}

fn decode_hash_command(parts: &[String]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_list_command(parts: &[String]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_set_command(parts: &[String]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_sorted_set_command(parts: &[String]) -> Result<Command, RedisError> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
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
        assert_eq!(cmd, Command::Generic(GenericCommand::Ping("".to_string())));

        let frame = Frame::Array(vec![
            Frame::BulkString(b"PING".to_vec()),
            Frame::BulkString(b"Hello".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Generic(GenericCommand::Ping("Hello".to_string()))
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
}
