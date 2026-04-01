use crate::{
    command::{Command, ListCommand},
    errors::RedisError,
    value::{ListInsertPivot, ListMoveDirection},
};

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
            let start = args[1].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid start value for LRANGE: {}", e))
            })?;
            let stop = args[2].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid stop value for LRANGE: {}", e))
            })?;
            Ok(Command::List(ListCommand::Lrange(key, start, stop)))
        }

        "LREM" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "LREM requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let value = args[1].clone();
            let count = args[2].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid count value for LREM: {}", e))
            })?;
            Ok(Command::List(ListCommand::Lrem(key, value, count)))
        }

        "LSET" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "LSET requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let index = args[1].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid index value for LSET: {}", e))
            })?;
            let value = args[2].clone();
            Ok(Command::List(ListCommand::LSet(key, index, value)))
        }

        "LTRIM" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "LTRIM requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let start = args[1].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid start value for LTRIM: {}", e))
            })?;
            let stop = args[2].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid stop value for LTRIM: {}", e))
            })?;
            Ok(Command::List(ListCommand::LTrim(key, start, stop)))
        }

        "LINSERT" => parse_linsert(args),
        "LMOVE" => parse_lmove(args),
        "BLMOVE" => parse_blmove(args),

        "LLEN" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "LLEN requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::List(ListCommand::Llen(key)))
        }
        "LINDEX" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "LINDEX requires exactly 2 arguments",
                )));
            }
            let key = args[0].clone();
            let index = args[1].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid index value for LINDEX: {}", e))
            })?;
            Ok(Command::List(ListCommand::LIndex(key, index)))
        }

        "BLPOP" | "BRPOP" => parse_bl_br_pop(&cmd_name, args),

        _ => Err(RedisError::ProtocolError(format!(
            "Unknown list command: {}",
            cmd_name
        ))),
    }
}

fn parse_linsert(args: &[String]) -> Result<Command, RedisError> {
    if args.len() != 4 {
        return Err(RedisError::ProtocolError(format!(
            "LINSERT requires exactly 4 arguments",
        )));
    }
    let key = args[0].clone();
    let position_str = args[1].to_uppercase();
    let pivot = args[2].clone();
    let value = args[3].clone();

    let position = match position_str.as_str() {
        "BEFORE" => ListInsertPivot::Before,
        "AFTER" => ListInsertPivot::After,
        _ => {
            return Err(RedisError::ProtocolError(format!(
                "Invalid position for LINSERT: {}",
                position_str
            )));
        }
    };

    Ok(Command::List(ListCommand::LInsert {
        key,
        pivot,
        value,
        position,
    }))
}

fn parse_lmove(args: &[String]) -> Result<Command, RedisError> {
    if args.len() != 4 {
        return Err(RedisError::ProtocolError(format!(
            "LMOVE requires exactly 4 arguments",
        )));
    }
    let src = args[0].clone();
    let dest = args[1].clone();
    let source_side_str = args[2].to_uppercase();
    let dest_side_str = args[3].to_uppercase();

    let source_side = match source_side_str.as_str() {
        "LEFT" => ListMoveDirection::Left,
        "RIGHT" => ListMoveDirection::Right,
        _ => {
            return Err(RedisError::ProtocolError(format!(
                "Invalid source side for LMOVE: {}",
                source_side_str
            )));
        }
    };

    let dest_side = match dest_side_str.as_str() {
        "LEFT" => ListMoveDirection::Left,
        "RIGHT" => ListMoveDirection::Right,
        _ => {
            return Err(RedisError::ProtocolError(format!(
                "Invalid destination side for LMOVE: {}",
                dest_side_str
            )));
        }
    };

    Ok(Command::List(ListCommand::LMove {
        src,
        dest,
        source_side,
        dest_side,
    }))
}

fn parse_blmove(args: &[String]) -> Result<Command, RedisError> {
    if args.len() != 5 {
        return Err(RedisError::ProtocolError(format!(
            "BLMOVE requires exactly 5 arguments",
        )));
    }
    let src = args[0].clone();
    let dest = args[1].clone();
    let source_side_str = args[2].to_uppercase();
    let dest_side_str = args[3].to_uppercase();
    let timeout = args[4].parse::<u64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid timeout value for BLMOVE: {}", e))
    })?;

    let source_side = match source_side_str.as_str() {
        "LEFT" => ListMoveDirection::Left,
        "RIGHT" => ListMoveDirection::Right,
        _ => {
            return Err(RedisError::ProtocolError(format!(
                "Invalid source side for LMOVE: {}",
                source_side_str
            )));
        }
    };

    let dest_side = match dest_side_str.as_str() {
        "LEFT" => ListMoveDirection::Left,
        "RIGHT" => ListMoveDirection::Right,
        _ => {
            return Err(RedisError::ProtocolError(format!(
                "Invalid destination side for LMOVE: {}",
                dest_side_str
            )));
        }
    };

    Ok(Command::List(ListCommand::BLmove {
        src,
        dest,
        source_side,
        dest_side,
        timeout,
    }))
}

fn parse_bl_br_pop(cmd: &str, args: &[String]) -> Result<Command, RedisError> {
    if args.len() < 2 {
        return Err(RedisError::ProtocolError(format!(
            "{} requires at least 2 arguments",
            cmd
        )));
    }
    let keys = args[..args.len() - 1].to_vec();
    let timeout = args[args.len() - 1].parse::<u64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid timeout value for {}: {}", cmd, e))
    })?;

    match cmd {
        "BLPOP" => Ok(Command::List(ListCommand::BLpop(keys, timeout))),
        "BRPOP" => Ok(Command::List(ListCommand::BRpop(keys, timeout))),
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown command: {}",
            cmd
        ))),
    }
}
