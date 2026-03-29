use std::time::Duration;

use crate::value::{Entry, HashEntry, RedisValue};


pub trait Storage: Send + Sync {
    fn set(&mut self, key: String, value: RedisValue, ttl: Option<Duration>);
    fn get(&self, key: &str) -> Option<&RedisValue>;
    fn exists(&self, key: &str) -> bool;
    fn del(&mut self, key: &str) -> bool;

    fn lpush(&mut self, key: String, values: Vec<Entry>) -> usize;
    fn rpush(&mut self, key: String, values: Vec<Entry>) -> usize;
    fn lpop(&mut self, key: &str, count: usize) -> Option<Vec<Entry>>;
    fn rpop(&mut self, key: &str, count: usize) -> Option<Vec<Entry>>;
    fn lrange(&self, key: &str, start: usize, stop: usize) -> Option<Vec<Entry>>;
    fn llen(&self, key: &str) -> usize;
    fn lrem(&mut self, key: &str, value: &Entry, count: usize) -> usize;

    fn hset(&mut self, key: &str, values: Vec<HashEntry>) -> bool;
    fn hget(&self, key: &str, field: &str) -> Option<HashEntry>;
    fn hmget(&self, key: &str, fields: Vec<&str>) -> Vec<HashEntry>;
    fn hgetall(&self, key: &str) -> Option<Vec<HashEntry>>;    
    // HINCRBY
    fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> Option<i64>;
    fn hdel(&mut self, key: &str, field: &str) -> bool;
}