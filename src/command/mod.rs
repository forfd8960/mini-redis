use crate::{
    command::{
        generic::GenericHandler,
        hash::{HashCommand, HashHandler},
        list::ListHandler,
        set::SetCommand,
        string::StringHandler,
    },
    errors::RedisError,
    protocol::encoder::{
        encode_integer, encode_nil, encode_ok, encode_simple_string, encode_string, encode_strings,
    },
    storage::{SetOptions, mem::MemStore},
    value::{ListInsertPivot, ListMoveDirection},
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
    Hash(hash::HashCommand),
    Set(SetCommand),
    SortedSet(SortedSetCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericCommand {
    Ping(Option<String>), // ping [message]
    Echo(String),
    Exists(Vec<String>), // exists key1 key2 ...
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
    Lpush(String, Vec<String>), // lpush key value1 value2 ...
    Rpush(String, Vec<String>), // rpush key value1 value2 ...

    /*
    LPOP mylist               # remove & return from left
    RPOP mylist               # remove & return from right
    LPOP mylist 3             # remove & return 3 elements from left
    RPOP mylist 3             # remove & return 3 elements from right
    */
    Lpop(String, usize), // lpop key count
    Rpop(String, usize), // rpop key count

    /*
    LRANGE mylist 0 -1        # get all elements (0 = first, -1 = last)
    LRANGE mylist 0 4         # get first 5 elements
    LRANGE mylist -3 -1       # get last 3 elements
    */
    Lrange(String, i64, i64), // lrange key start stop

    Lrem(String, String, i64), // lrem key value count
    LTrim(String, i64, i64),   // ltrim keep only indices 1–3, delete everything else

    /// LINSERT mylist BEFORE "x" "new"   # insert "new" before "x"
    /// LINSERT mylist AFTER  "x" "new"   # insert "new" after "x"
    LInsert {
        key: String,
        position: ListInsertPivot, // whether to insert before or after the pivot
        pivot: String,
        value: String,
    }, // linsert key BEFORE|AFTER pivot value

    LSet(String, i64, String), // lset key index value

    /// LMOVE src dest LEFT  RIGHT   # pop from src left, push to dest right
    /// LMOVE src dest RIGHT LEFT   # pop from src right, push to dest left
    LMove {
        src: String,
        dest: String,
        source_side: ListMoveDirection, // LEFT or RIGHT
        dest_side: ListMoveDirection,   // LEFT or RIGHT
    }, // lmove source destination LEFT|RIGHT LEFT|RIGHT

    LIndex(String, i64), // lindex key index
    Llen(String),        // llen key

    // # Blocks until an element is available (or timeout expires)
    BLpop(Vec<String>, u64), // blpop key1 key2 ... timeout
    BRpop(Vec<String>, u64), // brpop key1 key2 ... timeout
    BLmove {
        src: String,
        dest: String,
        source_side: ListMoveDirection, // LEFT or RIGHT
        dest_side: ListMoveDirection,   // LEFT or RIGHT
        timeout: u64,
    }, // blmove source destination LEFT|RIGHT LEFT|RIGHT timeout
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
        "HSET"
            | "HSETNX"
            | "HGET"
            | "HMGET"
            | "HMSET"
            | "HGETALL"
            | "HEXISTS"
            | "HKEYS"
            | "HVALS"
            | "HLEN"
            | "HSCAN"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HDEL"
    )
}

pub fn is_list_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LRANGE"
            | "LLEN"
            | "LREM"
            | "LTRIM"
            | "LINSERT"
            | "LINDEX"
            | "LSET"
            | "LMOVE"
            | "BLPOP"
            | "BRPOP"
            | "BLMOVE"
    )
}

pub fn is_set_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name.to_uppercase().as_str(),
        "SADD"
            | "SREM"
            | "SPOP"
            | "SMEMBERS"
            | "SISMEMBER"
            | "SMISMEMBER"
            | "SRANDMEMBER"
            | "SCARD"
            | "SUNION"
            | "SUNIONSTORE"
            | "SINTER"
            | "SINTERSTORE"
            | "SINTERCARD"
            | "SDIFF"
            | "SDIFFSTORE"
            | "SMOVE"
            | "SSCAN"
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
            Command::List(list_cmd) => self.handle_list_commands(list_cmd),
            Command::Hash(hash_cmd) => self.handle_hash_commands(hash_cmd),
            _ => Err(RedisError::UnsupportedCommand),
        }
    }

    fn handle_generic_command(&mut self, cmd: GenericCommand) -> Result<BytesFrame, RedisError> {
        match cmd {
            GenericCommand::Ping(msg) => self.ping(msg),
            GenericCommand::Echo(msg) => self.echo(msg.as_str()),
            GenericCommand::Exists(keys) => self.exists(keys.iter().map(|k| k.as_str()).collect()),
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

    fn handle_list_commands(&mut self, cmd: ListCommand) -> Result<BytesFrame, RedisError> {
        match cmd {
            ListCommand::Lpush(key, values) => self.lpush(&key, &values),
            ListCommand::Rpush(key, values) => self.rpush(&key, &values),
            ListCommand::Lpop(key, count) => self.lpop(&key, count),
            ListCommand::Rpop(key, count) => self.rpop(&key, count),
            ListCommand::Lrange(key, start, stop) => self.lrange(&key, start, stop),
            ListCommand::Lrem(key, value, count) => self.lrem(&key, count as i64, &value),
            ListCommand::LIndex(key, index) => self.lindex(&key, index as i64),
            ListCommand::LTrim(key, start, stop) => self.ltrim(&key, start, stop),
            ListCommand::LInsert {
                key,
                position,
                pivot,
                value,
            } => self.linsert(&key, &pivot, &value, position),
            ListCommand::LSet(key, index, value) => self.lset(&key, index, &value),
            ListCommand::LMove {
                src,
                dest,
                source_side,
                dest_side,
            } => self.lmove(&src, &dest, source_side, dest_side),
            ListCommand::BLpop(keys, timeout) => {
                self.blpop(keys.iter().map(|k| k.as_str()).collect(), timeout)
            }
            _ => Err(RedisError::UnsupportedCommand), // other list commands not implemented yet
        }
    }

    fn handle_hash_commands(&mut self, cmd: HashCommand) -> Result<BytesFrame, RedisError> {
        match cmd {
            HashCommand::HGet(key, field) => self.hget(&key, &field),
            HashCommand::HSet(key, values) => self.hset(&key, values),
            HashCommand::HSetNX(key, field, value) => self.hsetnx(&key, &field, &value),

            HashCommand::HMGet(key, fields) => {
                self.hmget(&key, fields.iter().map(|f| f.as_str()).collect())
            }
            HashCommand::HMSet(key, field_values) => self.hmset(&key, field_values),

            HashCommand::HGetAll(key) => self.hgetall(&key),
            HashCommand::HLen(key) => self.hlen(&key),
            HashCommand::HKeys(key) => self.hkeys(&key),
            HashCommand::HVals(key) => self.hvals(&key),
            HashCommand::HExists(key, field) => self.hexists(&key, &field),

            HashCommand::HScan {
                key,
                cursor,
                pattern,
                count,
            } => self.hscan(&key, cursor, pattern.as_deref(), count),

            HashCommand::HIncrBy {
                key,
                field,
                increment,
            } => self.hincrby(&key, &field, increment),

            HashCommand::HIncrByFloat {
                key,
                field,
                increment,
            } => self.hincrbyfloat(&key, &field, increment),

            HashCommand::HDel { key, fields } => self.hdel(&key, &fields),
        }
    }
}
