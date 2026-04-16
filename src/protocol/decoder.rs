use crate::{
    command::{
        Command,
        generic::GenericCommand,
        is_generic_command, is_hash_command, is_list_command, is_set_command,
        is_sorted_set_command, is_string_command,
        sorted_set::{
            Aggregate, LexBound, Limit, RangeBy, ScoreBound, SortedSetCommand, ZAddComparison,
            ZAddCondition, ZAddOptions,
        },
        string::StringCommand,
    },
    errors::RedisError,
    protocol::{
        CommandResult, hash::decode_hash_commands, list::decode_list_command,
        set::decode_set_command,
    },
    storage::{SetCondition, SetOptions, SetTTL},
    value::StringValue,
};

use ordered_float::OrderedFloat;
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
        _ if is_hash_command(cmd_name.as_str()) => decode_hash_commands(args.as_slice()),
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
                let val = if let Some(v) = args[i + 1].parse::<i64>().ok() {
                    StringValue::Int(v)
                } else {
                    StringValue::Raw(args[i + 1].clone())
                };

                pairs.push((args[i].clone(), val));
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
    if args.len() < 2 {
        return Err(RedisError::ProtocolError(format!(
            "SET requires at least 2 arguments",
        )));
    }

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

    // if value is integer string, store as Integer variant, otherwise store as Raw string
    if let Some(v) = value.parse::<i64>().ok() {
        Ok(Command::String(StringCommand::Set {
            key,
            value: StringValue::Int(v),
            options,
        }))
    } else {
        Ok(Command::String(StringCommand::Set {
            key,
            value: StringValue::Raw(value),
            options,
        }))
    }
}

fn decode_sorted_set_command(parts: &[String]) -> CommandResult {
    let cmd_name = parts[0].to_uppercase();
    let args = &parts[1..];

    match cmd_name.as_str() {
        "ZADD" => parse_zadd(args),
        "ZREM" => parse_zrem(args),
        "ZINCRBY" => parse_zincrby(args),
        "ZRANGE" => parse_zrange(args),
        "ZCARD" => parse_zcard(args),
        "ZSCORE" => parse_zscore(args),
        "ZMSCORE" => parse_zmscore(args),
        "ZCOUNT" => parse_zcount(args),
        "ZLEXCOUNT" => parse_zlexcount(args),
        "ZRANK" => parse_zrank(args),
        "ZREVRANK" => parse_zrevrank(args),
        "ZPOPMAX" => parse_zpopmax(args),
        "ZPOPMIN" => parse_zpopmin(args),
        "BZPOPMAX" => parse_bzpopmax(args),
        "BZPOPMIN" => parse_bzpopmin(args),
        "ZREMRANGEBYRANK" => parse_zremrangebyrank(args),
        "ZREMRANGEBYSCORE" => parse_zremrangebyscore(args),
        "ZREMRANGEBYLEX" => parse_zremrangebylex(args),
        "ZUNION" => parse_zunion(args),
        "ZUNIONSTORE" => parse_zunionstore(args),
        "ZINTER" => parse_zinter(args),
        "ZINTERSTORE" => parse_zinterstore(args),
        "ZDIFF" => parse_zdiff(args),
        "ZDIFFSTORE" => parse_zdiffstore(args),
        "ZRANDMEMBER" => parse_zrandmember(args),
        "ZSCAN" => parse_zscan(args),
        _ => Err(RedisError::ProtocolError(format!(
            "Unknown sorted set command: {}",
            cmd_name
        ))),
    }
}

fn parse_zadd(args: &[String]) -> Result<Command, RedisError> {
    let key = args[0].clone();
    let mut score_member_pairs: Vec<(OrderedFloat<f64>, String)> = Vec::new();
    let mut options = ZAddOptions::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "NX" => {
                options.condition = ZAddCondition::OnlyNew;
            }

            "XX" => {
                options.condition = ZAddCondition::OnlyExisting;
            }

            "GT" => {
                options.comparison = ZAddComparison::GreaterThan;
            }
            "LT" => {
                options.comparison = ZAddComparison::LessThan;
            }

            "CH" => options.changed = true,
            "INCR" => options.incr = true,

            _ => {
                if i + 1 >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing member for score in ZADD command".to_string(),
                    ));
                }

                let score = args[i].parse::<f64>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid score value for ZADD: {}", e))
                })?;

                let member = args[i + 1].clone();
                score_member_pairs.push((OrderedFloat(score), member));
                i += 1; // skip the member argument
            }
        }
        i += 1;
    }
    Ok(Command::SortedSet(SortedSetCommand::ZAdd {
        key,
        members: score_member_pairs,
        options,
    }))
}

fn parse_zrem(args: &[String]) -> Result<Command, RedisError> {
    let key = args[0].clone();
    let members = args[1..].to_vec();
    Ok(Command::SortedSet(SortedSetCommand::ZRem { key, members }))
}

fn parse_zincrby(args: &[String]) -> Result<Command, RedisError> {
    let key = args[0].clone();
    let increment = args[1].parse::<f64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid increment value for ZINCRBY: {}", e))
    })?;

    let member = args[2].clone();
    Ok(Command::SortedSet(SortedSetCommand::ZIncrBy {
        key,
        increment: OrderedFloat(increment),
        member,
    }))
}

/*
ZRange {
        key: String,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    },

# Rank 0 = lowest score, -1 = highest score
ZRANGE leaderboard 0 -1                 # all members, low → high
ZRANGE leaderboard 0 -1 REV            # all members, high → low
ZRANGE leaderboard 0 -1 WITHSCORES     # include scores in output
ZRANGE leaderboard 0 2                  # top 3 lowest
ZRANGE leaderboard 0 2 REV             # top 3 highest

ZRANGE leaderboard 1000 2000 BYSCORE              # score between 1000–2000
ZRANGE leaderboard 2000 1000 BYSCORE REV          # reversed
ZRANGE leaderboard 1000 2000 BYSCORE LIMIT 0 10   # paginate: skip 0, take 10

# Exclusive bounds with ( prefix
ZRANGE leaderboard (1000 2000 BYSCORE    # score > 1000 and <= 2000
ZRANGE leaderboard -inf +inf BYSCORE     # all members by score

ZRANGE myset "[a" "[m" BYLEX             # members a–m alphabetically
ZRANGE myset "-"  "+"  BYLEX             # all members alphabetically
ZRANGE myset "[a" "[m" BYLEX REV         # reversed

# - means negative infinity (before all), + means positive infinity (after all)
# [ means inclusive,  ( means exclusive
*/
fn parse_zrange(args: &[String]) -> Result<Command, RedisError> {
    let key = args[0].clone();

    let mut args_pos = 0;
    let range_type = if args.len() > 3 && args[3].to_uppercase() == "BYSCORE" {
        args_pos = 4; // skip key, min, max, BYSCORE
        RangeBy::Score {
            min: parse_scorebound(&args[1])?,
            max: parse_scorebound(&args[2])?,
        }
    } else if args.len() > 3 && args[3].to_uppercase() == "BYLEX" {
        let (min, max) = parse_lex_range(&args[1], &args[2]);

        args_pos = 4; // skip key, min, max, BYLEX
        RangeBy::Lex { min, max }
    } else {
        let start = args[1].parse::<i64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid start index for ZRANGE: {}", e))
        })?;
        let stop = args[2].parse::<i64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid end index for ZRANGE: {}", e))
        })?;

        args_pos = 3; // skip key, start, stop
        RangeBy::Rank { start, stop }
    };

    let mut rev = false;
    let mut with_scores = false;
    let mut limit = None;

    let mut i = args_pos;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "REV" => {
                rev = true;
                i += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing offset value for LIMIT option".to_string(),
                    ));
                }
                let offset = args[i].parse::<u64>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid offset value for LIMIT: {}", e))
                })?;

                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing count value for LIMIT option".to_string(),
                    ));
                }

                let count = args[i].parse::<u64>().map_err(|e| {
                    RedisError::ProtocolError(format!("Invalid count value for LIMIT: {}", e))
                })?;

                limit = Some(Limit { offset, count });
                i += 1;
            }
            _ => {
                return Err(RedisError::ProtocolError(format!(
                    "Unknown option for ZRANGE command: {}",
                    args[i]
                )));
            }
        }
    }

    Ok(Command::SortedSet(SortedSetCommand::ZRange {
        key,
        range: range_type,
        rev,
        with_scores,
        limit,
    }))
}

fn parse_lex_range(min: &str, max: &str) -> (LexBound, LexBound) {
    let parse_bound = |s: &str| {
        if s.starts_with('(') {
            LexBound::Exclusive(s[1..].to_string())
        } else if s.starts_with('[') {
            LexBound::Inclusive(s[1..].to_string())
        } else if s == "-" {
            LexBound::NegInf
        } else if s == "+" {
            LexBound::PosInf
        } else {
            LexBound::Inclusive(s.to_string())
        }
    };

    (parse_bound(min), parse_bound(max))
}

fn parse_scorebound(s: &str) -> Result<ScoreBound, RedisError> {
    if s == "-inf" {
        Ok(ScoreBound::NegInf)
    } else if s == "+inf" {
        Ok(ScoreBound::PosInf)
    } else if s.starts_with("(") {
        let value = s[1..].parse::<f64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid exclusive score value: {}", e))
        })?;
        Ok(ScoreBound::Exclusive(OrderedFloat(value)))
    } else if s.starts_with("[") {
        let value = s[1..].parse::<f64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid inclusive score value: {}", e))
        })?;
        Ok(ScoreBound::Inclusive(OrderedFloat(value)))
    } else {
        let value = s
            .parse::<f64>()
            .map_err(|e| RedisError::ProtocolError(format!("Invalid score value: {}", e)))?;
        Ok(ScoreBound::Inclusive(OrderedFloat(value)))
    }
}

fn parse_zcard(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    Ok(Command::SortedSet(SortedSetCommand::ZCard { key }))
}

fn parse_zscore(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let member = args[1].clone();
    Ok(Command::SortedSet(SortedSetCommand::ZScore { key, member }))
}

fn parse_zmscore(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let members = args[1..].to_vec();
    Ok(Command::SortedSet(SortedSetCommand::ZMScore {
        key,
        members,
    }))
}

fn parse_zcount(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let min = parse_scorebound(&args[1])?;
    let max = parse_scorebound(&args[2])?;
    Ok(Command::SortedSet(SortedSetCommand::ZCount {
        key,
        min,
        max,
    }))
}

fn parse_zlexcount(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let (min, max) = parse_lex_range(&args[1], &args[2]);
    Ok(Command::SortedSet(SortedSetCommand::ZLexCount {
        key,
        min,
        max,
    }))
}

fn parse_zrank(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let member = args[1].clone();
    let with_score = args.len() > 2 && args[2].to_uppercase() == "WITHSCORE";

    Ok(Command::SortedSet(SortedSetCommand::ZRank {
        key,
        member,
        with_score,
    }))
}

fn parse_zrevrank(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let member = args[1].clone();
    let with_score = args.len() > 2 && args[2].to_uppercase() == "WITHSCORE";

    Ok(Command::SortedSet(SortedSetCommand::ZRevRank {
        key,
        member,
        with_score,
    }))
}

fn parse_zpopmax(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let count = if args.len() > 1 {
        Some(args[1].parse::<u64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid count value for ZPOPMAX: {}", e))
        })?)
    } else {
        None
    };
    Ok(Command::SortedSet(SortedSetCommand::ZPopMax { key, count }))
}

fn parse_zpopmin(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let count = if args.len() > 1 {
        Some(args[1].parse::<u64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid count value for ZPOPMIN: {}", e))
        })?)
    } else {
        None
    };
    Ok(Command::SortedSet(SortedSetCommand::ZPopMin { key, count }))
}

fn parse_bzpopmax(args: &[String]) -> CommandResult {
    let keys = args[0..args.len() - 1].to_vec();
    let timeout = args[args.len() - 1].parse::<f64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid timeout value for BZPOPMAX: {}", e))
    })?;
    Ok(Command::SortedSet(SortedSetCommand::BZPopMax {
        keys,
        timeout: OrderedFloat(timeout),
    }))
}

fn parse_bzpopmin(args: &[String]) -> CommandResult {
    let keys = args[0..args.len() - 1].to_vec();
    let timeout = args[args.len() - 1].parse::<f64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid timeout value for BZPOPMIN: {}", e))
    })?;
    Ok(Command::SortedSet(SortedSetCommand::BZPopMin {
        keys,
        timeout: OrderedFloat(timeout),
    }))
}

fn parse_zremrangebyrank(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let start = args[1].parse::<i64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid start index for ZREMRANGEBYRANK: {}", e))
    })?;

    let stop = args[2].parse::<i64>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid end index for ZREMRANGEBYRANK: {}", e))
    })?;

    Ok(Command::SortedSet(SortedSetCommand::ZRemRangeByRank {
        key,
        start,
        stop,
    }))
}

fn parse_zremrangebyscore(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let min = parse_scorebound(&args[1])?;
    let max = parse_scorebound(&args[2])?;
    Ok(Command::SortedSet(SortedSetCommand::ZRemRangeByScore {
        key,
        min,
        max,
    }))
}

fn parse_zremrangebylex(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let (min, max) = parse_lex_range(&args[1], &args[2]);
    Ok(Command::SortedSet(SortedSetCommand::ZRemRangeByLex {
        key,
        min,
        max,
    }))
}

// zunion numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
fn parse_zunion(args: &[String]) -> CommandResult {
    let numkeys = args[0].parse::<usize>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid numkeys value for ZUNION: {}", e))
    })?;
    let keys = args[1..1 + numkeys].to_vec();

    let (weights, aggregate, with_scores) =
        parse_weights_aggregate_with_scores(numkeys, &args[1 + numkeys..])?;

    Ok(Command::SortedSet(SortedSetCommand::ZUnion {
        keys,
        weights,
        aggregate,
        with_scores,
    }))
}

fn parse_zunionstore(args: &[String]) -> CommandResult {
    let destkey = args[0].clone();
    let numkeys = args[1].parse::<usize>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid numkeys value for ZUNIONSTORE: {}", e))
    })?;
    let keys = args[2..2 + numkeys].to_vec();

    let (weights, aggregate, _) =
        parse_weights_aggregate_with_scores(numkeys, &args[2 + numkeys..])?;

    Ok(Command::SortedSet(SortedSetCommand::ZUnionStore {
        dst: destkey,
        keys,
        weights,
        aggregate,
    }))
}

fn parse_zinter(args: &[String]) -> CommandResult {
    let numkeys = args[0].parse::<usize>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid numkeys value for ZINTER: {}", e))
    })?;
    let keys = args[1..1 + numkeys].to_vec();

    let (weights, aggregate, with_scores) =
        parse_weights_aggregate_with_scores(numkeys, &args[1 + numkeys..])?;

    Ok(Command::SortedSet(SortedSetCommand::ZInter {
        keys,
        weights,
        aggregate,
        with_scores,
    }))
}

fn parse_zinterstore(args: &[String]) -> CommandResult {
    let destkey = args[0].clone();
    let numkeys = args[1].parse::<usize>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid numkeys value for ZINTERSTORE: {}", e))
    })?;
    let keys = args[2..2 + numkeys].to_vec();

    let (weights, aggregate, _) =
        parse_weights_aggregate_with_scores(numkeys, &args[2 + numkeys..])?;

    Ok(Command::SortedSet(SortedSetCommand::ZInterStore {
        dst: destkey,
        keys,
        weights,
        aggregate,
    }))
}

fn parse_zdiff(args: &[String]) -> CommandResult {
    let numkeys = args[0].parse::<usize>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid numkeys value for ZDIFF: {}", e))
    })?;
    let keys = args[1..1 + numkeys].to_vec();

    let mut i = 1 + numkeys;
    let mut with_scores = false;
    while i < args.len() {
        if args[i].to_uppercase() == "WITHSCORES" {
            with_scores = true;
            i += 1;
        } else {
            return Err(RedisError::ProtocolError(format!(
                "Unknown option for ZDIFF command: {}",
                args[i]
            )));
        }
    }

    Ok(Command::SortedSet(SortedSetCommand::ZDiff {
        keys,
        with_scores,
    }))
}

fn parse_zdiffstore(args: &[String]) -> CommandResult {
    let destkey = args[0].clone();
    let numkeys = args[1].parse::<usize>().map_err(|e| {
        RedisError::ProtocolError(format!("Invalid numkeys value for ZDIFFSTORE: {}", e))
    })?;
    let keys = args[2..2 + numkeys].to_vec();

    Ok(Command::SortedSet(SortedSetCommand::ZDiffStore {
        dst: destkey,
        keys,
    }))
}

fn parse_zrandmember(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let count = if args.len() > 1 {
        Some(args[1].parse::<i64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid count value for ZRANDMEMBER: {}", e))
        })?)
    } else {
        None
    };

    let with_scores = if args.len() > 2 && args[2].to_uppercase() == "WITHSCORES" {
        true
    } else {
        false
    };

    Ok(Command::SortedSet(SortedSetCommand::ZRandMember {
        key,
        count,
        with_scores,
    }))
}

fn parse_zscan(args: &[String]) -> CommandResult {
    let key = args[0].clone();
    let cursor = args[1]
        .parse::<u64>()
        .map_err(|e| RedisError::ProtocolError(format!("Invalid cursor value for ZSCAN: {}", e)))?;

    let pattern = if args.len() > 3 && args[2].to_uppercase() == "MATCH" {
        Some(args[3].clone())
    } else {
        None
    };

    let count = if args.len() > 5 && args[4].to_uppercase() == "COUNT" {
        Some(args[5].parse::<u64>().map_err(|e| {
            RedisError::ProtocolError(format!("Invalid count value for ZSCAN: {}", e))
        })?)
    } else {
        None
    };

    Ok(Command::SortedSet(SortedSetCommand::ZScan {
        key,
        cursor,
        pattern,
        count,
    }))
}

fn parse_weights_aggregate_with_scores(
    numkeys: usize,
    args: &[String],
) -> Result<(Option<Vec<OrderedFloat<f64>>>, Aggregate, bool), RedisError> {
    let mut weights = None;
    let mut aggregate = Aggregate::Sum; // default is SUM
    let mut with_scores = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "WEIGHTS" => {
                i += 1;
                if i + numkeys > args.len() {
                    return Err(RedisError::ProtocolError(
                        "Not enough weight values for WEIGHTS option".to_string(),
                    ));
                }

                let w: Result<Vec<OrderedFloat<f64>>, RedisError> = args[i..i + numkeys]
                    .iter()
                    .map(|s| {
                        s.parse::<f64>().map(OrderedFloat).map_err(|e| {
                            RedisError::ProtocolError(format!(
                                "Invalid weight value for WEIGHTS: {}",
                                e
                            ))
                        })
                    })
                    .collect();
                weights = Some(w?);
                i += numkeys;
            }

            "AGGREGATE" => {
                i += 1;
                if i >= args.len() {
                    return Err(RedisError::ProtocolError(
                        "Missing aggregate type for AGGREGATE option".to_string(),
                    ));
                }

                aggregate = match args[i].to_uppercase().as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => {
                        return Err(RedisError::ProtocolError(format!(
                            "Unknown aggregate type for AGGREGATE: {}",
                            args[i]
                        )));
                    }
                };
                i += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            _ => {
                return Err(RedisError::ProtocolError(format!(
                    "Unknown option: {}",
                    args[i]
                )));
            }
        }
    }

    Ok((weights, aggregate, with_scores))
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::command::list::ListCommand;
    use crate::value::{ListInsertPivot, ListMoveDirection};

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
                value: StringValue::Raw("myvalue".to_string()),
                options: SetOptions::default(),
            })
        );
    }

    #[test]
    fn test_decode_string_command_set_int() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"SET".to_vec()),
            Frame::BulkString(b"mykey".to_vec()),
            Frame::BulkString(b"1000".to_vec()),
        ]);
        let cmd = decode_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::String(StringCommand::Set {
                key: "mykey".to_string(),
                value: StringValue::Int(1000),
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
                value: StringValue::Raw("myvalue".to_string()),
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

    #[test]
    fn test_parse_zrange() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"ZRANGE".to_vec()),
            Frame::BulkString(b"leaderboard".to_vec()),
            Frame::BulkString(b"0".to_vec()),
            Frame::BulkString(b"-1".to_vec()),
            Frame::BulkString(b"REV".to_vec()),
            Frame::BulkString(b"WITHSCORES".to_vec()),
        ]);

        let cmd = decode_frame(frame);
        println!("Decoded command: {:?}", cmd);

        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();

        assert_eq!(
            cmd,
            Command::SortedSet(SortedSetCommand::ZRange {
                key: "leaderboard".to_string(),
                range: RangeBy::Rank { start: 0, stop: -1 },
                rev: true,
                with_scores: true,
                limit: None,
            })
        );
    }

    #[test]
    fn test_parse_zrange_by_score_with_limit() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"ZRANGE".to_vec()),
            Frame::BulkString(b"leaderboard".to_vec()),
            Frame::BulkString(b"1000".to_vec()),
            Frame::BulkString(b"2000".to_vec()),
            Frame::BulkString(b"BYSCORE".to_vec()),
            Frame::BulkString(b"LIMIT".to_vec()),
            Frame::BulkString(b"0".to_vec()),
            Frame::BulkString(b"10".to_vec()),
        ]);

        let cmd = decode_frame(frame);
        println!("Decoded command: {:?}", cmd);

        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();

        assert_eq!(
            cmd,
            Command::SortedSet(SortedSetCommand::ZRange {
                key: "leaderboard".to_string(),
                range: RangeBy::Score {
                    min: ScoreBound::Inclusive(OrderedFloat(1000.0)),
                    max: ScoreBound::Inclusive(OrderedFloat(2000.0)),
                },
                rev: false,
                with_scores: false,
                limit: Some(Limit {
                    offset: 0,
                    count: 10
                }),
            })
        );
    }

    #[test]
    fn test_parse_zrange_by_lex_with_limit() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"ZRANGE".to_vec()),
            Frame::BulkString(b"leaderboard".to_vec()),
            Frame::BulkString(b"(a".to_vec()),
            Frame::BulkString(b"[z".to_vec()),
            Frame::BulkString(b"BYLEX".to_vec()),
            Frame::BulkString(b"REV".to_vec()),
            Frame::BulkString(b"LIMIT".to_vec()),
            Frame::BulkString(b"0".to_vec()),
            Frame::BulkString(b"10".to_vec()),
        ]);

        let cmd = decode_frame(frame);
        println!("Decoded command: {:?}", cmd);

        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();

        assert_eq!(
            cmd,
            Command::SortedSet(SortedSetCommand::ZRange {
                key: "leaderboard".to_string(),
                range: RangeBy::Lex {
                    min: LexBound::Exclusive("a".to_string()),
                    max: LexBound::Inclusive("z".to_string()),
                },
                rev: true,
                with_scores: false,
                limit: Some(Limit {
                    offset: 0,
                    count: 10
                }),
            })
        );
    }

    // ZRANGE leaderboard -inf +inf BYSCORE
    #[test]
    fn parse_zrange_by_score_inf() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"ZRANGE".to_vec()),
            Frame::BulkString(b"leaderboard".to_vec()),
            Frame::BulkString(b"-inf".to_vec()),
            Frame::BulkString(b"+inf".to_vec()),
            Frame::BulkString(b"BYSCORE".to_vec()),
        ]);

        let cmd = decode_frame(frame);
        println!("Decoded command: {:?}", cmd);

        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();

        assert_eq!(
            cmd,
            Command::SortedSet(SortedSetCommand::ZRange {
                key: "leaderboard".to_string(),
                range: RangeBy::Score {
                    min: ScoreBound::NegInf,
                    max: ScoreBound::PosInf,
                },
                rev: false,
                with_scores: false,
                limit: None,
            })
        );
    }

    // ZRANGE myset "-"  "+"  BYLEX
    #[test]
    fn parse_zrange_by_lex_inf() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"ZRANGE".to_vec()),
            Frame::BulkString(b"myset".to_vec()),
            Frame::BulkString(b"-".to_vec()),
            Frame::BulkString(b"+".to_vec()),
            Frame::BulkString(b"BYLEX".to_vec()),
            Frame::BulkString(b"REV".to_vec()),
            Frame::BulkString(b"WITHSCORES".to_vec()),
        ]);

        let cmd = decode_frame(frame);
        println!("Decoded command: {:?}", cmd);

        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();

        assert_eq!(
            cmd,
            Command::SortedSet(SortedSetCommand::ZRange {
                key: "myset".to_string(),
                range: RangeBy::Lex {
                    min: LexBound::NegInf,
                    max: LexBound::PosInf,
                },
                rev: true,
                with_scores: true,
                limit: None,
            })
        );
    }

    fn string_to_args(s: &str) -> Vec<String> {
        s.split_whitespace().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_decode_sorted_set_command() {
        let args = vec![
            string_to_args("ZADD leaderboard NX CH 1 alice 2 bob"),
            string_to_args("ZREM set1 a b c"),
            string_to_args("ZRANK set1 a WITHSCORE"),
            string_to_args("ZREVRANK set1 a WITHSCORE"),
            string_to_args("ZINCRBY leaderboard 100 alice"),
            string_to_args("ZCARD set1"),
            string_to_args("ZSCORE set1 alice"),
            string_to_args("ZMSCORE set1 alice bob"),
            string_to_args("ZCOUNT set1 -inf +inf"),
            string_to_args("ZLEXCOUNT set1 - +"),
            string_to_args("ZPOPMAX set1 2"),
            string_to_args("ZPOPMIN set1"),
            string_to_args("BZPOPMAX set1 set2 1.5"),
            string_to_args("BZPOPMIN set1 set2 2"),
            string_to_args("ZREMRANGEBYRANK set1 0 -1"),
            string_to_args("ZREMRANGEBYSCORE set1 (1 5"),
            string_to_args("ZREMRANGEBYLEX set1 [a (z"),
            string_to_args("ZUNION 2 set1 set2 WEIGHTS 1 2 AGGREGATE MAX WITHSCORES"),
            string_to_args("ZUNIONSTORE out 2 set1 set2 WEIGHTS 1 2 AGGREGATE MIN"),
            string_to_args("ZINTER 2 set1 set2 WITHSCORES"),
            string_to_args("ZINTERSTORE out 2 set1 set2 AGGREGATE SUM"),
            string_to_args("ZDIFF 2 set1 set2 WITHSCORES"),
            string_to_args("ZDIFFSTORE out 2 set1 set2"),
            string_to_args("ZRANDMEMBER set1 3 WITHSCORES"),
            string_to_args("ZSCAN set1 0 MATCH user:* COUNT 10"),
        ];

        let expected_cmd = vec![
            Command::SortedSet(SortedSetCommand::ZAdd {
                key: "leaderboard".to_string(),
                members: vec![
                    (OrderedFloat(1.0), "alice".to_string()),
                    (OrderedFloat(2.0), "bob".to_string()),
                ],
                options: ZAddOptions {
                    condition: ZAddCondition::OnlyNew,
                    comparison: ZAddComparison::None,
                    changed: true,
                    incr: false,
                },
            }),
            Command::SortedSet(SortedSetCommand::ZRem {
                key: "set1".to_string(),
                members: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            }),
            Command::SortedSet(SortedSetCommand::ZRank {
                key: "set1".to_string(),
                member: "a".to_string(),
                with_score: true,
            }),
            Command::SortedSet(SortedSetCommand::ZRevRank {
                key: "set1".to_string(),
                member: "a".to_string(),
                with_score: true,
            }),
            Command::SortedSet(SortedSetCommand::ZIncrBy {
                key: "leaderboard".to_string(),
                increment: OrderedFloat(100.0),
                member: "alice".to_string(),
            }),
            Command::SortedSet(SortedSetCommand::ZCard {
                key: "set1".to_string(),
            }),
            Command::SortedSet(SortedSetCommand::ZScore {
                key: "set1".to_string(),
                member: "alice".to_string(),
            }),
            Command::SortedSet(SortedSetCommand::ZMScore {
                key: "set1".to_string(),
                members: vec!["alice".to_string(), "bob".to_string()],
            }),
            Command::SortedSet(SortedSetCommand::ZCount {
                key: "set1".to_string(),
                min: ScoreBound::NegInf,
                max: ScoreBound::PosInf,
            }),
            Command::SortedSet(SortedSetCommand::ZLexCount {
                key: "set1".to_string(),
                min: LexBound::NegInf,
                max: LexBound::PosInf,
            }),
            Command::SortedSet(SortedSetCommand::ZPopMax {
                key: "set1".to_string(),
                count: Some(2),
            }),
            Command::SortedSet(SortedSetCommand::ZPopMin {
                key: "set1".to_string(),
                count: None,
            }),
            Command::SortedSet(SortedSetCommand::BZPopMax {
                keys: vec!["set1".to_string(), "set2".to_string()],
                timeout: OrderedFloat(1.5),
            }),
            Command::SortedSet(SortedSetCommand::BZPopMin {
                keys: vec!["set1".to_string(), "set2".to_string()],
                timeout: OrderedFloat(2.0),
            }),
            Command::SortedSet(SortedSetCommand::ZRemRangeByRank {
                key: "set1".to_string(),
                start: 0,
                stop: -1,
            }),
            Command::SortedSet(SortedSetCommand::ZRemRangeByScore {
                key: "set1".to_string(),
                min: ScoreBound::Exclusive(OrderedFloat(1.0)),
                max: ScoreBound::Inclusive(OrderedFloat(5.0)),
            }),
            Command::SortedSet(SortedSetCommand::ZRemRangeByLex {
                key: "set1".to_string(),
                min: LexBound::Inclusive("a".to_string()),
                max: LexBound::Exclusive("z".to_string()),
            }),
            Command::SortedSet(SortedSetCommand::ZUnion {
                keys: vec!["set1".to_string(), "set2".to_string()],
                weights: Some(vec![OrderedFloat(1.0), OrderedFloat(2.0)]),
                aggregate: Aggregate::Max,
                with_scores: true,
            }),
            Command::SortedSet(SortedSetCommand::ZUnionStore {
                dst: "out".to_string(),
                keys: vec!["set1".to_string(), "set2".to_string()],
                weights: Some(vec![OrderedFloat(1.0), OrderedFloat(2.0)]),
                aggregate: Aggregate::Min,
            }),
            Command::SortedSet(SortedSetCommand::ZInter {
                keys: vec!["set1".to_string(), "set2".to_string()],
                weights: None,
                aggregate: Aggregate::Sum,
                with_scores: true,
            }),
            Command::SortedSet(SortedSetCommand::ZInterStore {
                dst: "out".to_string(),
                keys: vec!["set1".to_string(), "set2".to_string()],
                weights: None,
                aggregate: Aggregate::Sum,
            }),
            Command::SortedSet(SortedSetCommand::ZDiff {
                keys: vec!["set1".to_string(), "set2".to_string()],
                with_scores: true,
            }),
            Command::SortedSet(SortedSetCommand::ZDiffStore {
                dst: "out".to_string(),
                keys: vec!["set1".to_string(), "set2".to_string()],
            }),
            Command::SortedSet(SortedSetCommand::ZRandMember {
                key: "set1".to_string(),
                count: Some(3),
                with_scores: true,
            }),
            Command::SortedSet(SortedSetCommand::ZScan {
                key: "set1".to_string(),
                cursor: 0,
                pattern: Some("user:*".to_string()),
                count: Some(10),
            }),
        ];

        for (idx, args) in args.iter().enumerate() {
            let cmd = decode_sorted_set_command(args).unwrap();
            assert_eq!(cmd, expected_cmd[idx]);
        }
    }

    #[test]
    fn test_decode_sorted_set_command_failures() {
        let cases = vec![
            (string_to_args("ZUNKNOWN key"), "Unknown sorted set command"),
            (
                string_to_args("ZADD leaderboard NX 1"),
                "Missing member for score in ZADD command",
            ),
            (
                string_to_args("ZADD leaderboard bad alice"),
                "Invalid score value for ZADD",
            ),
            (
                string_to_args("ZINCRBY leaderboard bad alice"),
                "Invalid increment value for ZINCRBY",
            ),
            (
                string_to_args("ZRANGE leaderboard a -1"),
                "Invalid start index for ZRANGE",
            ),
            (
                string_to_args("ZRANGE leaderboard 0 b"),
                "Invalid end index for ZRANGE",
            ),
            (
                string_to_args("ZRANGE leaderboard 0 -1 LIMIT"),
                "Missing offset value for LIMIT option",
            ),
            (
                string_to_args("ZRANGE leaderboard 0 -1 LIMIT x 10"),
                "Invalid offset value for LIMIT",
            ),
            (
                string_to_args("ZRANGE leaderboard 0 -1 LIMIT 0"),
                "Missing count value for LIMIT option",
            ),
            (
                string_to_args("ZRANGE leaderboard 0 -1 LIMIT 0 x"),
                "Invalid count value for LIMIT",
            ),
            (
                string_to_args("ZRANGE leaderboard 0 -1 WHAT"),
                "Unknown option for ZRANGE command",
            ),
            (
                string_to_args("ZCOUNT set1 bad +inf"),
                "Invalid score value",
            ),
            (
                string_to_args("ZPOPMAX set1 bad"),
                "Invalid count value for ZPOPMAX",
            ),
            (
                string_to_args("ZPOPMIN set1 bad"),
                "Invalid count value for ZPOPMIN",
            ),
            (
                string_to_args("BZPOPMAX set1 set2 bad"),
                "Invalid timeout value for BZPOPMAX",
            ),
            (
                string_to_args("BZPOPMIN set1 set2 bad"),
                "Invalid timeout value for BZPOPMIN",
            ),
            (
                string_to_args("ZREMRANGEBYRANK set1 a -1"),
                "Invalid start index for ZREMRANGEBYRANK",
            ),
            (
                string_to_args("ZREMRANGEBYRANK set1 0 b"),
                "Invalid end index for ZREMRANGEBYRANK",
            ),
            (
                string_to_args("ZREMRANGEBYSCORE set1 bad +inf"),
                "Invalid score value",
            ),
            (
                string_to_args("ZUNION bad set1 set2"),
                "Invalid numkeys value for ZUNION",
            ),
            (
                string_to_args("ZUNION 2 set1 set2 WEIGHTS 1"),
                "Not enough weight values for WEIGHTS option",
            ),
            (
                string_to_args("ZUNION 2 set1 set2 WEIGHTS 1 bad"),
                "Invalid weight value for WEIGHTS",
            ),
            (
                string_to_args("ZUNION 2 set1 set2 AGGREGATE"),
                "Missing aggregate type for AGGREGATE option",
            ),
            (
                string_to_args("ZUNION 2 set1 set2 AGGREGATE AVG"),
                "Unknown aggregate type for AGGREGATE",
            ),
            (string_to_args("ZUNION 2 set1 set2 BAD"), "Unknown option"),
            (
                string_to_args("ZUNIONSTORE out bad set1 set2"),
                "Invalid numkeys value for ZUNIONSTORE",
            ),
            (
                string_to_args("ZINTER bad set1 set2"),
                "Invalid numkeys value for ZINTER",
            ),
            (
                string_to_args("ZINTERSTORE out bad set1 set2"),
                "Invalid numkeys value for ZINTERSTORE",
            ),
            (
                string_to_args("ZDIFF bad set1 set2"),
                "Invalid numkeys value for ZDIFF",
            ),
            (
                string_to_args("ZDIFF 2 set1 set2 BAD"),
                "Unknown option for ZDIFF command",
            ),
            (
                string_to_args("ZDIFFSTORE out bad set1 set2"),
                "Invalid numkeys value for ZDIFFSTORE",
            ),
            (
                string_to_args("ZRANDMEMBER set1 bad WITHSCORES"),
                "Invalid count value for ZRANDMEMBER",
            ),
            (
                string_to_args("ZSCAN set1 bad"),
                "Invalid cursor value for ZSCAN",
            ),
            (
                string_to_args("ZSCAN set1 0 MATCH user:* COUNT bad"),
                "Invalid count value for ZSCAN",
            ),
        ];

        for (args, expected_msg) in cases {
            let err = decode_sorted_set_command(&args).unwrap_err();
            match err {
                RedisError::ProtocolError(msg) => assert!(
                    msg.contains(expected_msg),
                    "expected error containing '{expected_msg}', got '{msg}' for args: {:?}",
                    args
                ),
                other => panic!(
                    "expected ProtocolError containing '{expected_msg}', got {other:?} for args: {:?}",
                    args
                ),
            }
        }
    }
}
