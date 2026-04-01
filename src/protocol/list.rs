use crate::{command::{Command, ListCommand, ListInsertPivot}, errors::RedisError};

pub fn decode_list_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        "LPUSH" | "RPUSH" => {
            if args.len() < 2 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires at least 2 arguments",
                    cmd_name
                )));
            }
            let key = args[0].clone();
            let values = args[1..].to_vec();
            if cmd_name == "LPUSH" {
                Ok(Command::List(ListCommand::Lpush(key, values)))
            } else {
                Ok(Command::List(ListCommand::Rpush(key, values)))
            }
        }
        "LPOP" | "RPOP" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires exactly 2 arguments",
                    cmd_name
                )));
            }
            let key = args[0].clone();
            let count = args[1].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid count value for {}: {}", cmd_name, e))
            })?;
            if cmd_name == "LPOP" {
                Ok(Command::List(ListCommand::Lpop(key, count)))
            } else {
                Ok(Command::List(ListCommand::Rpop(key, count)))
            }
        }
        "LRANGE" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "LRANGE requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let start = args[1].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid start value for LRANGE: {}", e))
            })?;
            let stop = args[2].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid stop value for LRANGE: {}", e))
            })?;
            Ok(Command::List(ListCommand::Lrange(key, start, stop)))
        }

        "LREM" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "LREM requires exactly 3 arguments",
                )));
            }       let key = args[0].clone();
            let value = args[1].clone();
            let count = args[2].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid count value for LREM: {}", e))
            })?;
            Ok(Command::List(ListCommand::Lrem(key, value, count)))
        }

        "LTRIM" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "LTRIM requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let start = args[1].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid start value for LTRIM: {}", e))
            })?;
            let stop = args[2].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid stop value for LTRIM: {}", e))
            })?;
            Ok(Command::List(ListCommand::LTrim(key, start, stop)))
        }

        "LINSERT" => {
            if args.len() != 4 {
                return Err(RedisError::ProtocolError(format!(
                    "LINSERT requires exactly 4 arguments",
                )));
            }
            let key = args[0].clone();
            let position = args[1].to_uppercase();
            let pivot = args[2].clone();
            let value = args[3].clone();
            if position != "BEFORE" && position != "AFTER" {
                return Err(RedisError::ProtocolError(format!(
                    "Invalid position value for LINSERT: {}",
                    position
                )));
            }
            let position_enum = if position == "BEFORE" {
                ListInsertPivot::Before
            } else {
                ListInsertPivot::After
            };
            Ok(Command::List(ListCommand::LInsert{key, position: position_enum, pivot, value}))
        }

        _ => Err(RedisError::ProtocolError(format!(
            "Unknown list command: {}",
            cmd_name
        ))),
    }
}