use ordered_float::OrderedFloat;

use crate::{
    command::{Command, hash::HashCommand},
    errors::RedisError,
};

pub fn decode_hash_commands(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        "HSET" => {
            if args.len() < 3 || args.len() % 2 == 0 {
                return Err(RedisError::ProtocolError(format!(
                    "HSET requires an odd number of arguments (key field value ...)"
                )));
            }
            let key = args[0].clone();
            let mut field_values = Vec::new();

            for i in (1..args.len()).step_by(2) {
                field_values.push((args[i].clone(), args[i + 1].clone()));
            }
            Ok(Command::Hash(HashCommand::HSet(key, field_values)))
        }

        "HSETNX" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "HSETNX requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let field = args[1].clone();
            let value = args[2].clone();

            Ok(Command::Hash(HashCommand::HSetNX(key, field, value)))
        }

        "HGET" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "HGET requires exactly 2 arguments",
                )));
            }
            let key = args[0].clone();
            let field = args[1].clone();
            Ok(Command::Hash(HashCommand::HGet(key, field)))
        }
        "HMGET" => {
            if args.len() < 2 {
                return Err(RedisError::ProtocolError(format!(
                    "HMGET requires at least 2 arguments",
                )));
            }
            let key = args[0].clone();
            let fields = args[1..].to_vec();
            Ok(Command::Hash(HashCommand::HMGet(key, fields)))
        }

        "HMSET" => {
            if args.len() < 3 || args.len() % 2 == 0 {
                return Err(RedisError::ProtocolError(format!(
                    "HMSET requires an odd number of arguments (key field value ...)"
                )));
            }
            let key = args[0].clone();
            let mut field_values = Vec::new();

            for i in (1..args.len()).step_by(2) {
                field_values.push((args[i].clone(), args[i + 1].clone()));
            }
            Ok(Command::Hash(HashCommand::HMSet(key, field_values)))
        }

        "HGETALL" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "HGETALL requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Hash(HashCommand::HGetAll(key)))
        }

        "HKEYS" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "HKEYS requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Hash(HashCommand::HKeys(key)))
        }

        "HVALS" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "HVALS requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Hash(HashCommand::HVals(key)))
        }
        "HLEN" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "HLEN requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Hash(HashCommand::HLen(key)))
        }

        "HEXISTS" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "HEXISTS requires exactly 2 arguments",
                )));
            }
            let key = args[0].clone();
            let field = args[1].clone();
            Ok(Command::Hash(HashCommand::HExists(key, field)))
        }

        "HINCRBY" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "HINCRBY requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let field = args[1].clone();
            let increment = args[2].parse::<i64>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid increment value for HINCRBY: {}", e))
            })?;
            Ok(Command::Hash(HashCommand::HIncrBy {
                key,
                field,
                increment,
            }))
        }
        "HINCRBYFLOAT" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "HINCRBYFLOAT requires exactly 3 arguments",
                )));
            }
            let key = args[0].clone();
            let field = args[1].clone();
            let increment = args[2].parse::<f64>().map_err(|e| {
                RedisError::ProtocolError(format!(
                    "Invalid increment value for HINCRBYFLOAT: {}",
                    e
                ))
            })?;
            Ok(Command::Hash(HashCommand::HIncrByFloat {
                key,
                field,
                increment: OrderedFloat(increment),
            }))
        }
        "HDEL" => {
            if args.len() < 2 {
                return Err(RedisError::ProtocolError(format!(
                    "HDEL requires at least 2 arguments",
                )));
            }
            let key = args[0].clone();
            let fields = args[1..].to_vec();
            Ok(Command::Hash(HashCommand::HDel { key, fields }))
        }
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown hash command: {}",
            cmd_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_hash_commands() {
        let cmd = decode_hash_commands(&vec![
            "HSET".to_string(),
            "myhash".to_string(),
            "field1".to_string(),
            "value1".to_string(),
            "field2".to_string(),
            "value2".to_string(),
        ])
        .unwrap();

        assert_eq!(
            cmd,
            Command::Hash(HashCommand::HSet(
                "myhash".to_string(),
                vec![
                    ("field1".to_string(), "value1".to_string()),
                    ("field2".to_string(), "value2".to_string())
                ]
            ))
        );
    }

    #[test]
    fn test_decode_all_hash_commands() {
        let cmds = vec![
            "HSET myhash field1 value1 field2 value2",
            "HGET myhash field1",
            "HMGET myhash field1 field2",
            "HGETALL myhash",
            "HKEYS myhash",
            "HVALS myhash",
            "HLEN myhash",
            "HEXISTS myhash field1",
            "HINCRBY myhash field1 5",
            "HINCRBYFLOAT myhash field1 2.5",
            "HDEL myhash field1 field2",
        ];

        let expect_cmds = vec![
            Command::Hash(HashCommand::HSet(
                "myhash".to_string(),
                vec![
                    ("field1".to_string(), "value1".to_string()),
                    ("field2".to_string(), "value2".to_string()),
                ],
            )),
            Command::Hash(HashCommand::HGet(
                "myhash".to_string(),
                "field1".to_string(),
            )),
            Command::Hash(HashCommand::HMGet(
                "myhash".to_string(),
                vec!["field1".to_string(), "field2".to_string()],
            )),
            Command::Hash(HashCommand::HGetAll("myhash".to_string())),
            Command::Hash(HashCommand::HKeys("myhash".to_string())),
            Command::Hash(HashCommand::HVals("myhash".to_string())),
            Command::Hash(HashCommand::HLen("myhash".to_string())),
            Command::Hash(HashCommand::HExists(
                "myhash".to_string(),
                "field1".to_string(),
            )),
            Command::Hash(HashCommand::HIncrBy {
                key: "myhash".to_string(),
                field: "field1".to_string(),
                increment: 5,
            }),
            Command::Hash(HashCommand::HIncrByFloat {
                key: "myhash".to_string(),
                field: "field1".to_string(),
                increment: OrderedFloat(2.5),
            }),
            Command::Hash(HashCommand::HDel {
                key: "myhash".to_string(),
                fields: vec!["field1".to_string(), "field2".to_string()],
            }),
        ];

        for (cmd_str, expected_cmd) in cmds.iter().zip(expect_cmds.iter()) {
            let parts: Vec<String> = cmd_str.split_whitespace().map(|s| s.to_string()).collect();
            let cmd = decode_hash_commands(&parts);
            assert!(cmd.is_ok(), "Failed to decode command: {}", cmd_str);
            assert_eq!(
                &cmd.unwrap(),
                expected_cmd,
                "Decoded command does not match expected command for: {}",
                cmd_str
            );
        }
    }
}
