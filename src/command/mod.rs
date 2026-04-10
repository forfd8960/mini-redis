use redis_protocol::resp2::types::BytesFrame;

use crate::{
    command::{
        generic::{GenericCommand, GenericHandler},
        hash::{HashCommand, HashHandler},
        list::{ListCommand, ListHandler},
        set::{SetCommand, SetHandler},
        sorted_set::{SortedSetCommand, SortedSetHandler},
        string::{StringCommand, StringHandler},
    },
    errors::RedisError,
    protocol::encoder::{
        encode_integer, encode_nil, encode_ok, encode_simple_string, encode_string, encode_strings,
    },
    storage::mem::MemStore,
};

pub mod generic; // general commands like PING, ECHO, EXISTS, TTL, EXPIRE, SCAN, KEYS, DEL, etc.
pub mod hash; // hash commands like HSET, HGET, HMGET, HGETALL, etc.
pub mod list; // list commands like LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
pub mod set; // set commands like SADD, SREM, SMEMBERS, etc.
pub mod sorted_set; // sorted set commands like ZADD, ZRANGE, ZSCORE, etc.
pub mod string; // string commands like GET, SET, INCR, DECR, etc.

pub type HandlerResult = Result<BytesFrame, RedisError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Generic(generic::GenericCommand),
    String(string::StringCommand),
    List(list::ListCommand),
    Hash(hash::HashCommand),
    Set(set::SetCommand),
    SortedSet(sorted_set::SortedSetCommand),
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
        "ZADD"
            | "ZINCRBY"
            | "ZREM"
            | "ZRANGE"
            | "ZRANGEWITHSCORES"
            | "ZCARD"
            | "ZCOUNT"
            | "ZLEXCOUNT"
            | "ZRANK"
            | "ZREVRANK"
            | "ZSCORE"
            | "ZMSCORE"
            | "ZPOPMAX"
            | "ZPOPMIN"
            | "BZPOPMAX"
            | "BZPOPMIN"
            | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYLEX"
            | "ZUNION"
            | "ZUNIONSTORE"
            | "ZINTER"
            | "ZINTERSTORE"
            | "ZDIFF"
            | "ZDIFFSTORE"
            | "ZRANDMEMBER"
            | "ZSCAN"
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
            Command::Set(set_cmd) => self.handle_set_commands(set_cmd),
            Command::SortedSet(sorted_set_cmd) => self.handle_sorted_set_commands(sorted_set_cmd),
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

    fn handle_set_commands(&mut self, cmd: SetCommand) -> Result<BytesFrame, RedisError> {
        match cmd {
            SetCommand::SAdd(key, members) => self.sadd(
                key.as_str(),
                members.iter().map(|v| v.as_str()).collect::<Vec<&str>>(),
            ),
            SetCommand::SRem(key, members) => self.srem(
                key.as_str(),
                members.iter().map(|v| v.as_str()).collect::<Vec<&str>>(),
            ),
            SetCommand::SCard(key) => self.scard(&key),
            SetCommand::SMembers(key) => self.smembers(&key),
            SetCommand::SIsMember(key, member) => self.sismember(&key, &member),
            SetCommand::SMIsMember(key, members) => self.smismember(
                &key,
                members.iter().map(|v| v.as_str()).collect::<Vec<&str>>(),
            ),
            SetCommand::SPop(key, count) => self.spop(&key, count),
            SetCommand::SRandMember(key, count) => self.srandmember(&key, count),
            SetCommand::SMove(src, dst, member) => self.smove(&src, &dst, &member),
            SetCommand::SUnion(keys) => {
                self.sunion(keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>())
            }
            SetCommand::SInter(keys) => {
                self.sinter(keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>())
            }
            SetCommand::SDiff(keys) => {
                self.sdiff(keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>())
            }
            SetCommand::SUnionStore(dst, keys) => {
                self.sunionstore(&dst, keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>())
            }
            SetCommand::SInterStore(dst, keys) => {
                self.sinterstore(&dst, keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>())
            }
            SetCommand::SDiffStore(dst, keys) => {
                self.sdiffstore(&dst, keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>())
            }
            SetCommand::SInterCard(numkeys, keys, limit) => self.sintercard(
                numkeys,
                keys.iter().map(|v| v.as_str()).collect::<Vec<&str>>(),
                limit,
            ),
            SetCommand::SScan(key, cursor, pattern) => self.sscan(&key, cursor, pattern),

            _ => Err(RedisError::UnsupportedCommand),
        }
    }

    fn handle_sorted_set_commands(
        &mut self,
        cmd: SortedSetCommand,
    ) -> Result<BytesFrame, RedisError> {
        match cmd {
            SortedSetCommand::ZAdd {
                key,
                members,
                options,
            } => self.zadd(&key, members, &options),
            SortedSetCommand::ZCard { key } => self.zcard(&key),
            SortedSetCommand::ZRem { key, members } => self.zrem(&key, members),
            SortedSetCommand::ZRange {
                key,
                range,
                rev,
                limit,
                with_scores,
            } => unimplemented!(),
            SortedSetCommand::ZRangeStore {
                dst,
                src,
                range,
                rev,
                limit,
            } => self.zrange_store(&dst, &src, range, rev, limit),
            SortedSetCommand::ZRank {
                key,
                member,
                with_score,
            } => self.zrank(&key, &member, with_score),
            SortedSetCommand::ZScore { key, member } => self.zscore(&key, &member),
            SortedSetCommand::ZMScore { key, members } => self.zmscore(&key, members.as_slice()),
            _ => Err(RedisError::UnsupportedCommand), // other sorted set commands not implemented yet
        }
    }
}
