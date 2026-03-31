use crate::{
    command::{generic::GenericHandler, string::StringHandler},
    errors::RedisError,
    protocol::encoder::{
        encode_integer, encode_nil, encode_ok, encode_simple_string, encode_string, encode_strings,
    },
    storage::{SetOptions, mem::MemStore},
};
use ordered_float::OrderedFloat;
use redis_protocol::resp2::types::BytesFrame;

pub mod generic; // general commands like PING, ECHO, EXISTS, TTL, EXPIRE, SCAN, KEYS, DEL, etc.
pub mod hash; // hash commands like HSET, HGET, HMGET, HGETALL, etc.
pub mod list; // list commands like LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
pub mod set; // set commands like SADD, SREM, SMEMBERS, etc.
pub mod sorted_set; // sorted set commands like ZADD, ZRANGE, ZSCORE, etc.
pub mod string; // string commands like GET, SET, INCR, DECR, etc. 

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Generic(GenericCommand),
    String(StringCommand),
    List(ListCommand),
    Hash(HashCommand),
    Set(SetCommand),
    SortedSet(SortedSetCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericCommand {
    Ping(Option<String>), // ping [message]
    Echo(String),
    Exists(String),
    TTL(String),
    Expire(String, u64),
    // scan cursor [MATCH pattern] [COUNT count] [TYPE type]
    Scan(i64, Option<String>, Option<usize>, Option<String>),
    Keys(String), // keys pattern
    Type(String), // type key
    Del(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StringCommand {
    Get(String),
    /// https://redis.io/docs/latest/commands/set/
    /// set key value [EX seconds] [PX milliseconds] [EXAT timestamp-seconds]
    ///     [PXAT timestamp-milliseconds] [KEEPTTL] [NX|XX] [GET]
    Set {
        key: String,
        value: String,
        options: SetOptions,
    },
    Incr(String),
    IncrBy {
        key: String,
        increment: i64,
    }, // incrby key increment
    Decr(String),
    DecrBy {
        key: String,
        decrement: i64,
    }, // decrby key decrement
    Mget {
        keys: Vec<String>,
    }, // mget key1 key2 ...
    Mset {
        pairs: Vec<(String, String)>,
    }, // mset key1 value1 key2 value2 ...
    GetRange {
        key: String,
        start: usize,
        end: usize,
    }, // getrange key start end
    SetRange {
        key: String,
        offset: usize,
        value: String,
    }, // setrange key offset value
    Append {
        key: String,
        value: String,
    }, // append key value
    StrLen {
        key: String,
    }, // strlen key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListCommand {
    Lpush(String, Vec<String>),   // lpush key value1 value2 ...
    Rpush(String, Vec<String>),   // rpush key value1 value2 ...
    Lpop(String, usize),          // lpop key count
    Rpop(String, usize),          // rpop key count
    Lrange(String, usize, usize), // lrange key start stop
    Llen(String),                 // llen key
    Lrem(String, String, usize),  // lrem key value count
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashCommand {
    Hset(String, Vec<(String, String)>), // hset key field1 value1 field2 value2 ...
    Hget(String, String),                // hget key field
    Hmget(String, Vec<String>),          // hmget key field1 field2 ...
    Hgetall(String),                     // hgetall key
    Hincrby {
        key: String,
        field: String,
        increment: i64,
    }, // hincrby key field increment
    Hdel {
        key: String,
        fields: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCommand {
    Sadd(String, Vec<String>), // sadd key member1 member2 ...
    Srem(String, Vec<String>), // srem key member1 member2 ...
    Smembers(String),          // smembers key
    Sismember(String, String), // sismember key member
    Scard(String),             // scard key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortedSetCommand {
    Zadd(String, Vec<(String, OrderedFloat<f64>)>), // zadd key score1 member1 score2 member2 ...
    Zrem(String, Vec<String>),                      // zrem key member1 member2 ...
    Zrange(String, usize, usize),                   // zrange key start stop
    ZrangeWithScores(String, usize, usize),         // zrange key start stop withscores
    Zrank(String, String),                          // zrank key member
    Zscore(String, String),                         // zscore key member
}

pub fn is_generic_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "PING" | "ECHO" | "EXISTS" | "TTL" | "EXPIRE" | "SCAN" | "KEYS" | "DEL" | "TYPE"
    )
}

pub fn is_string_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "GET"
            | "SET"
            | "INCR"
            | "INCRBY"
            | "DECR"
            | "DECRBY"
            | "MGET"
            | "MSET"
            | "GETRANGE"
            | "SETRANGE"
            | "APPEND"
            | "STRLEN"
    )
}

pub fn is_hash_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "HSET" | "HGET" | "HMGET" | "HGETALL" | "HINCRBY" | "HDEL"
    )
}

pub fn is_list_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LRANGE" | "LLEN" | "LREM"
    )
}

pub fn is_set_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" | "SCARD"
    )
}

pub fn is_sorted_set_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "ZADD" | "ZREM" | "ZRANGE" | "ZRANGEWITHSCORES" | "ZRANK" | "ZSCORE"
    )
}

pub struct CommandHandler {
    pub mem_storage: MemStore,
}

impl CommandHandler {
    pub fn new(mem_storage: MemStore) -> Self {
        Self { mem_storage }
    }

    pub fn handle_command(&mut self, cmd: Command) -> Result<BytesFrame, RedisError> {
        match cmd {
            Command::Generic(generic_cmd) => self.handle_generic_command(generic_cmd),
            Command::String(string_cmd) => self.handle_string_command(string_cmd),
            _ => Err(RedisError::UnsupportedCommand),
        }
    }

    fn handle_generic_command(&mut self, cmd: GenericCommand) -> Result<BytesFrame, RedisError> {
        match cmd {
            GenericCommand::Ping(msg) => self.ping(msg),
            GenericCommand::Echo(msg) => self.echo(msg.as_str()),
            GenericCommand::Exists(key) => self.exists(&key),
            GenericCommand::TTL(key) => self.ttl(&key),
            GenericCommand::Expire(key, seconds) => self.expire(&key, seconds),
            GenericCommand::Scan(cursor, pattern, count, type_filter) => {
                self.scan(cursor, pattern.as_deref(), count, type_filter.as_deref())
            }
            GenericCommand::Keys(pattern) => self.keys(&pattern),
            GenericCommand::Type(key) => self.get_type(&key),
            GenericCommand::Del(key) => self.del(&key),
        }
    }

    fn handle_string_command(&mut self, cmd: StringCommand) -> Result<BytesFrame, RedisError> {
        match cmd {
            StringCommand::Get(key) => {
                let res = self.get(&key);
                match res {
                    Some(s_v) => Ok(encode_string(s_v)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::Set {
                key,
                value,
                options,
            } => {
                self.set(&key, value, Some(options));
                Ok(encode_ok())
            }
            StringCommand::Incr(key) => {
                let res = self.incr(&key);
                match res {
                    Some(i) => Ok(encode_integer(i)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::IncrBy { key, increment } => {
                let res = self.incrby(&key, increment);
                match res {
                    Some(i) => Ok(encode_integer(i)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::Decr(key) => {
                let res = self.decr(&key);
                match res {
                    Some(i) => Ok(encode_integer(i)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::DecrBy { key, decrement } => {
                let res = self.decrby(&key, decrement);
                match res {
                    Some(i) => Ok(encode_integer(i)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::Mget { keys } => {
                let values = self.mget(keys.iter().map(|k| k.as_str()).collect());
                Ok(encode_strings(values))
            }
            StringCommand::Mset { pairs } => {
                self.mset(pairs);
                Ok(encode_ok())
            }
            StringCommand::GetRange { key, start, end } => {
                let res = self.getrange(&key, start, end);
                match res {
                    Some(s) => Ok(encode_simple_string(s)),
                    None => Ok(encode_simple_string("".to_string())),
                }
            }
            StringCommand::SetRange { key, offset, value } => {
                let res = self.setrange(&key, offset, value);
                match res {
                    Some(i) => Ok(encode_integer(i as i64)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::Append { key, value } => {
                let res = self.append(&key, &value);
                match res {
                    Some(i) => Ok(encode_integer(i as i64)),
                    None => Ok(encode_nil()),
                }
            }
            StringCommand::StrLen { key } => {
                let res = self.strlen(&key);
                match res {
                    Some(i) => Ok(encode_integer(i as i64)),
                    None => Ok(encode_nil()),
                }
            }
        }
    }
}
