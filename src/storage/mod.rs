use crate::{
    errors::RedisError,
    value::{HashEntry, ListInsertPivot, ListMoveDirection, StringValue},
};

pub mod mem;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetOptions {
    pub ttl: Option<SetTTL>,
    pub condition: Option<SetCondition>,
    pub get: bool, // whether to return the old value
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetTTL {
    EX(u64),   // expire time in seconds
    PX(u64),   // expire time in milliseconds
    EXAT(u64), // expire time as Unix timestamp in seconds
    PXAT(u64), // expire time as Unix timestamp in milliseconds
    KeepTTL,   // keep the existing TTL,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCondition {
    NX, // Only set the key if it does not already exist.
    XX, // Only set the key if it already exists.
}

pub trait Storage {
    fn get_type(&self, key: &str) -> Option<String>;
    fn ttl(&self, key: &str) -> Option<i64>; // return TTL in seconds, -1 if no TTL, -2 if key does not exist
    fn expire(&mut self, key: &str, ttl: i64) -> bool; // set TTL in seconds, return true if successful
    fn scan(
        &self,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
        type_filter: Option<&str>,
    ) -> (i64, Vec<String>);
    fn keys(&self, pattern: &str) -> Vec<String>;
    fn exists(&self, key: &str) -> bool;
    fn del(&mut self, key: &str) -> bool;

    fn get(&self, key: &str) -> Option<StringValue>;
    fn set(&mut self, key: &str, value: StringValue, opts: Option<SetOptions>) -> bool;
    fn incr(&mut self, key: &str) -> Option<i64>;
    fn incrby(&mut self, key: &str, increment: i64) -> Option<i64>;
    fn decr(&mut self, key: &str) -> Option<i64>;
    fn decrby(&mut self, key: &str, decrement: i64) -> Option<i64>;
    fn mget(&self, keys: Vec<&str>) -> Vec<Option<StringValue>>;
    fn mset(&mut self, pairs: Vec<(String, String)>) -> bool;
    fn getrange(&self, key: &str, start: usize, stop: usize) -> Option<String>;
    fn setrange(&mut self, key: &str, offset: usize, value: String) -> Option<usize>;
    fn append(&mut self, key: &str, value: &str) -> Option<usize>;
    fn strlen(&self, key: &str) -> Option<usize>;

    fn lpush(&mut self, key: &str, values: Vec<String>) -> Result<usize, RedisError>;
    fn rpush(&mut self, key: &str, values: Vec<String>) -> Result<usize, RedisError>;
    fn lpop(&mut self, key: &str, count: usize) -> Result<Option<Vec<String>>, RedisError>;
    fn rpop(&mut self, key: &str, count: usize) -> Result<Option<Vec<String>>, RedisError>;
    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Option<Vec<String>>, RedisError>;
    fn llen(&self, key: &str) -> Result<usize, RedisError>;
    fn lrem(&mut self, key: &str, count: i64, value: &str) -> Result<usize, RedisError>;
    fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, RedisError>;
    fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<bool, RedisError>;
    fn linsert(
        &mut self,
        key: &str,
        position: ListInsertPivot,
        pivot: &str,
        value: &str,
    ) -> Result<bool, RedisError>;
    fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<(), RedisError>;
    fn lmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<Option<String>, RedisError>;
    fn blpop(
        &mut self,
        keys: Vec<&str>,
        timeout: u64,
    ) -> Result<Option<(String, String)>, RedisError>;
    fn brpop(
        &mut self,
        keys: Vec<&str>,
        timeout: u64,
    ) -> Result<Option<(String, String)>, RedisError>;
    fn blmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
        timeout: u64,
    ) -> Result<Option<String>, RedisError>;

    fn hset(&mut self, key: &str, values: Vec<HashEntry>) -> bool;
    fn hget(&self, key: &str, field: &str) -> Option<HashEntry>;
    fn hmget(&self, key: &str, fields: Vec<&str>) -> Vec<HashEntry>;
    fn hgetall(&self, key: &str) -> Option<Vec<HashEntry>>;
    // HINCRBY
    fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> Option<i64>;
    fn hdel(&mut self, key: &str, field: &str) -> bool;
}
