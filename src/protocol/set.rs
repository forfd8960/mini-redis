use crate::{
    command::{Command, set::SetCommand},
    errors::RedisError,
};

pub fn decode_set_command(parts: &[String]) -> Result<Command, RedisError> {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        "SADD" | "SREM" => {
            if args.len() < 2 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires at least 2 arguments",
                    cmd_name
                )));
            }
            let key = args[0].clone();
            let members = args[1..].to_vec();
            if cmd_name == "SADD" {
                Ok(Command::Set(SetCommand::SAdd(key, members)))
            } else {
                Ok(Command::Set(SetCommand::SRem(key, members)))
            }
        }
        "SPOP" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "SPOP requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Set(SetCommand::SPop(key)))
        }
        "SRANDMEMBER" => {
            if args.len() < 1 || args.len() > 2 {
                return Err(RedisError::ProtocolError(format!(
                    "SRANDMEMBER requires 1 or 2 arguments",
                )));
            }
            let key = args[0].clone();
            let count = if args.len() == 2 {
                Some(args[1].parse::<usize>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid count value for SRANDMEMBER: {}", e))
                })?)
            } else {
                None
            };
            Ok(Command::Set(SetCommand::SRandMember(key, count)))
        }
        "SMEMBERS" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "SMEMBERS requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Set(SetCommand::SMembers(key)))
        }
        "SISMEMBER" => {
            if args.len() != 2 {
                return Err(RedisError::ProtocolError(format!(
                    "SISMEMBER requires exactly 2 arguments",
                )));
            }
            let key = args[0].clone();
            let member = args[1].clone();
            Ok(Command::Set(SetCommand::SIsMember(key, member)))
        }
        "SMISMEMBER" => {
            if args.len() < 2 {
                return Err(RedisError::ProtocolError(format!(
                    "SMISMEMBER requires at least 2 arguments",
                )));
            }
            let key = args[0].clone();
            let members = args[1..].to_vec();
            Ok(Command::Set(SetCommand::SMIsMember(key, members)))
        }
        "SCARD" => {
            if args.len() != 1 {
                return Err(RedisError::ProtocolError(format!(
                    "SCARD requires exactly 1 argument",
                )));
            }
            let key = args[0].clone();
            Ok(Command::Set(SetCommand::SCard(key)))
        }
        "SMOVE" => {
            if args.len() != 3 {
                return Err(RedisError::ProtocolError(format!(
                    "SMOVE requires exactly 3 arguments",
                )));
            }
            let src = args[0].clone();
            let dst = args[1].clone();
            let member = args[2].clone();
            Ok(Command::Set(SetCommand::SMove(src, dst, member)))
        }
        "SUNION" | "SINTER" | "SDIFF" => {
            if args.len() < 2 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires at least 2 arguments",
                    cmd_name
                )));
            }
            let keys = args.to_vec();
            if cmd_name == "SUNION" {
                Ok(Command::Set(SetCommand::SUnion(keys)))
            } else if cmd_name == "SINTER" {
                Ok(Command::Set(SetCommand::SInter(keys)))
            } else {
                Ok(Command::Set(SetCommand::SDiff(keys)))
            }
        }
        "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" => {
            if args.len() < 3 {
                return Err(RedisError::ProtocolError(format!(
                    "{} requires at least 3 arguments",
                    cmd_name
                )));
            }
            let dst = args[0].clone();
            let keys = args[1..].to_vec();
            if cmd_name == "SUNIONSTORE" {
                Ok(Command::Set(SetCommand::SUnionStore(dst, keys)))
            } else if cmd_name == "SINTERSTORE" {
                Ok(Command::Set(SetCommand::SInterStore(dst, keys)))
            } else {
                Ok(Command::Set(SetCommand::SDiffStore(dst, keys)))
            }
        }
        "SINTERCARD" => {
            if args.len() < 3 {
                return Err(RedisError::ProtocolError(format!(
                    "SINTERCARD requires at least 3 arguments",
                )));
            }
            let numkeys = args[0].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid numkeys value for SINTERCARD: {}", e))
            })?;

            if 1 + numkeys > args.len() {
                return Err(RedisError::ProtocolError(format!(
                    "SINTERCARD numkeys value {} exceeds number of provided keys {}",
                    numkeys,
                    args.len() - 1
                )));
            }

            let keys = args[1..1 + numkeys].to_vec();
            let limit = if args.len() > 1 + numkeys && args[1 + numkeys].to_uppercase() == "LIMIT" {
                Some(args[2 + numkeys].parse::<usize>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid limit value for SINTERCARD: {}", e))
                })?)
            } else {
                None
            };
            Ok(Command::Set(SetCommand::SInterCard(numkeys, keys, limit)))
        }
        "SSCAN" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(RedisError::ProtocolError(format!(
                    "SSCAN requires 2 to 4 arguments",
                )));
            }

            let key = args[0].clone();
            let cursor = args[1].parse::<usize>().map_err(|e| {
                RedisError::ProtocolError(format!("Invalid cursor value for SSCAN: {}", e))
            })?;
            let pattern = if args.len() == 4 {
                if args[2].to_uppercase() != "MATCH" {
                    return Err(RedisError::ProtocolError(format!(
                        "Expected MATCH keyword in SSCAN, got: {}",
                        args[2]
                    )));
                }
                Some(args[3].clone())
            } else {
                None
            };
            Ok(Command::Set(SetCommand::SScan(key, cursor, pattern)))
        }
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown set command: {}",
            cmd_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_sadd_command() {
        let cmd = decode_set_command(&vec![
            "SADD".to_string(),
            "myset".to_string(),
            "a".to_string(),
            "b".to_string(),
        ])
        .unwrap();
        assert_eq!(
            cmd,
            Command::Set(SetCommand::SAdd(
                "myset".to_string(),
                vec!["a".to_string(), "b".to_string()]
            ))
        );
    }

    #[test]
    fn test_decode_set_cmd_happy_path() {
        let cmds = vec![
            vec!["SADD".to_string(), "myset".to_string(), "a".to_string()],
            vec!["SREM".to_string(), "myset".to_string(), "a".to_string()],
            vec!["SPOP".to_string(), "myset".to_string()],
            vec!["SRANDMEMBER".to_string(), "myset".to_string()],
            vec![
                "SRANDMEMBER".to_string(),
                "myset".to_string(),
                "2".to_string(),
            ],
            vec!["SMEMBERS".to_string(), "myset".to_string()],
            vec![
                "SISMEMBER".to_string(),
                "myset".to_string(),
                "a".to_string(),
            ],
            vec![
                "SMISMEMBER".to_string(),
                "myset".to_string(),
                "a".to_string(),
                "b".to_string(),
            ],
            vec!["SCARD".to_string(), "myset".to_string()],
            vec![
                "SMOVE".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
                "a".to_string(),
            ],
            vec![
                "SUNION".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
            ],
            vec![
                "SINTER".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
            ],
            vec![
                "SDIFF".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
            ],
            vec![
                "SINTERSTORE".to_string(),
                "destset".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
            ],
            vec![
                "SDIFFSTORE".to_string(),
                "destset".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
            ],
            vec![
                "SINTERCARD".to_string(),
                "2".to_string(),
                "myset1".to_string(),
                "myset2".to_string(),
            ],
            vec![
                "SSCAN".to_string(),
                "myset".to_string(),
                "0".to_string(),
                "MATCH".to_string(),
                "pattern*".to_string(),
            ],
        ];

        let expected_cmds = vec![
            Command::Set(SetCommand::SAdd("myset".to_string(), vec!["a".to_string()])),
            Command::Set(SetCommand::SRem("myset".to_string(), vec!["a".to_string()])),
            Command::Set(SetCommand::SPop("myset".to_string())),
            Command::Set(SetCommand::SRandMember("myset".to_string(), None)),
            Command::Set(SetCommand::SRandMember("myset".to_string(), Some(2))),
            Command::Set(SetCommand::SMembers("myset".to_string())),
            Command::Set(SetCommand::SIsMember("myset".to_string(), "a".to_string())),
            Command::Set(SetCommand::SMIsMember(
                "myset".to_string(),
                vec!["a".to_string(), "b".to_string()],
            )),
            Command::Set(SetCommand::SCard("myset".to_string())),
            Command::Set(SetCommand::SMove(
                "myset1".to_string(),
                "myset2".to_string(),
                "a".to_string(),
            )),
            Command::Set(SetCommand::SUnion(vec![
                "myset1".to_string(),
                "myset2".to_string(),
            ])),
            Command::Set(SetCommand::SInter(vec![
                "myset1".to_string(),
                "myset2".to_string(),
            ])),
            Command::Set(SetCommand::SDiff(vec![
                "myset1".to_string(),
                "myset2".to_string(),
            ])),
            Command::Set(SetCommand::SInterStore(
                "destset".to_string(),
                vec!["myset1".to_string(), "myset2".to_string()],
            )),
            Command::Set(SetCommand::SDiffStore(
                "destset".to_string(),
                vec!["myset1".to_string(), "myset2".to_string()],
            )),
            Command::Set(SetCommand::SInterCard(
                2,
                vec!["myset1".to_string(), "myset2".to_string()],
                None,
            )),
            Command::Set(SetCommand::SScan(
                "myset".to_string(),
                0,
                Some("pattern*".to_string()),
            )),
        ];

        for (i, cmd_parts) in cmds.iter().enumerate() {
            let cmd = decode_set_command(&cmd_parts).unwrap();
            println!("{:?}", cmd);

            assert_eq!(cmd, expected_cmds[i]);
        }
    }
}
