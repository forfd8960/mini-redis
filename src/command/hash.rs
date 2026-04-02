use std::hash::Hash;

use crate::{
    protocol::encoder::{
        encode_float, encode_hash, encode_nil, encode_simple_string, encode_simple_strings,
    },
    storage::Storage,
    value::HashEntry,
};
use ordered_float::OrderedFloat;
use redis_protocol::resp2::types::BytesFrame;

use crate::{command::CommandHandler, errors::RedisError, protocol::encoder::encode_integer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashCommand {
    HSet(String, Vec<(String, String)>), // hset key field1 value1 field2 value2 ...

    /// set only if field does NOT already exist
    HSetNX(String, String, String), // hsetnx key field value

    HGet(String, String),                 // hget key field
    HMGet(String, Vec<String>),           // hmget key field1 field2 ...
    HMSet(String, Vec<(String, String)>), // hmset key field1 value1 field2 value2 ...
    HGetAll(String),                      // hgetall key
    HKeys(String),                        // hkeys key
    HVals(String),                        // hvals key
    HLen(String),                         // hlen key

    /// does field exist?   → 1 (yes) or 0 (no)
    HExists(String, String), // hexists key field,

    HIncrBy {
        key: String,
        field: String,
        increment: i64,
    }, // hincrby key field increment

    HIncrByFloat {
        key: String,
        field: String,
        increment: OrderedFloat<f64>,
    }, // hincrbyfloat key field increment

    HScan {
        key: String,
        cursor: i64,
        pattern: Option<String>,
        count: Option<usize>,
    }, // hscan key cursor [MATCH pattern] [COUNT count]

    HDel {
        key: String,
        fields: Vec<String>,
    },
}

pub trait HashHandler {
    fn hset(&mut self, key: &str, values: Vec<(String, String)>) -> Result<BytesFrame, RedisError>;
    fn hsetnx(&mut self, key: &str, field: &str, value: &str) -> Result<BytesFrame, RedisError>;
    fn hget(&self, key: &str, field: &str) -> Result<BytesFrame, RedisError>;
    fn hmget(&self, key: &str, fields: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn hmset(
        &mut self,
        key: &str,
        field_values: Vec<(String, String)>,
    ) -> Result<BytesFrame, RedisError>;
    fn hgetall(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn hkeys(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn hvals(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn hlen(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn hexists(&self, key: &str, field: &str) -> Result<BytesFrame, RedisError>;
    fn hscan(
        &self,
        key: &str,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<BytesFrame, RedisError>;

    // HINCRBY
    fn hincrby(&mut self, key: &str, field: &str, increment: i64)
    -> Result<BytesFrame, RedisError>;
    fn hincrbyfloat(
        &mut self,
        key: &str,
        field: &str,
        increment: OrderedFloat<f64>,
    ) -> Result<BytesFrame, RedisError>;

    // HDEL
    fn hdel(&mut self, key: &str, fields: &[String]) -> Result<BytesFrame, RedisError>;
}

impl HashHandler for CommandHandler {
    fn hset(&mut self, key: &str, values: Vec<(String, String)>) -> Result<BytesFrame, RedisError> {
        let updated = self.mem_storage.hset(key, values);
        Ok(encode_integer(if updated { 1 } else { 0 }))
    }

    fn hsetnx(&mut self, key: &str, field: &str, value: &str) -> Result<BytesFrame, RedisError> {
        let updated = self.mem_storage.hsetnx(key, field, value);
        Ok(encode_integer(if updated { 1 } else { 0 }))
    }

    fn hget(&self, key: &str, field: &str) -> Result<BytesFrame, RedisError> {
        match self.mem_storage.hget(key, field) {
            Some(entry) => Ok(encode_simple_string(entry.1)),
            None => Ok(encode_nil()),
        }
    }

    fn hmget(&self, key: &str, fields: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let entries = self.mem_storage.hmget(key, fields);
        Ok(encode_hash(entries))
    }

    fn hgetall(&self, key: &str) -> Result<BytesFrame, RedisError> {
        match self.mem_storage.hgetall(key) {
            Some(entries) => Ok(encode_hash(entries)),
            None => Ok(encode_nil()),
        }
    }

    fn hkeys(&self, key: &str) -> Result<BytesFrame, RedisError> {
        match self.mem_storage.hkeys(key) {
            Some(keys) => Ok(encode_simple_strings(keys)),
            None => Ok(encode_nil()),
        }
    }

    fn hvals(&self, key: &str) -> Result<BytesFrame, RedisError> {
        match self.mem_storage.hvals(key) {
            Some(vals) => Ok(encode_simple_strings(vals)),
            None => Ok(encode_nil()),
        }
    }

    fn hlen(&self, key: &str) -> Result<BytesFrame, RedisError> {
        let len = self.mem_storage.hlen(key).unwrap_or(0);
        Ok(encode_integer(len as i64))
    }

    fn hexists(&self, key: &str, field: &str) -> Result<BytesFrame, RedisError> {
        let exists = self.mem_storage.hexists(key, field);
        Ok(encode_integer(if exists { 1 } else { 0 }))
    }

    fn hscan(
        &self,
        key: &str,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn hincrby(
        &mut self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.hincrby(key, field, increment);
        match res {
            Some(i) => Ok(encode_integer(i)),
            None => Ok(encode_nil()),
        }
    }

    fn hincrbyfloat(
        &mut self,
        key: &str,
        field: &str,
        increment: OrderedFloat<f64>,
    ) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.hincrbyfloat(key, field, increment);
        match res {
            Some(i) => Ok(encode_float(i)),
            None => Ok(encode_nil()),
        }
    }

    fn hdel(&mut self, key: &str, fields: &[String]) -> Result<BytesFrame, RedisError> {
        let deleted_count = self.mem_storage.hdel(key, fields);
        Ok(encode_integer(deleted_count as i64))
    }

    fn hmset(&mut self, key: &str, field_values: Vec<HashEntry>) -> Result<BytesFrame, RedisError> {
        let updated = self.mem_storage.hmset(key, field_values);
        Ok(encode_integer(if updated { 1 } else { 0 }))
    }
}
