use redis_protocol::resp2::types::BytesFrame;

use crate::protocol::encoder::{encode_integer, encode_nil, encode_simple_strings};
use crate::storage::Storage;
use crate::{command::CommandHandler, errors::RedisError, protocol::encoder::encode_simple_string};

pub trait GenericHandler {
    fn ping(&self, msg: Option<String>) -> Result<BytesFrame, RedisError>;
    fn echo(&self, message: &str) -> Result<BytesFrame, RedisError>;
    fn exists(&self, key: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn ttl(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn expire(&mut self, key: &str, ttl: u64) -> Result<BytesFrame, RedisError>;
    fn scan(
        &self,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
        type_filter: Option<&str>,
    ) -> Result<BytesFrame, RedisError>;
    fn keys(&self, pattern: &str) -> Result<BytesFrame, RedisError>;
    fn del(&mut self, key: &str) -> Result<BytesFrame, RedisError>;
    fn get_type(&self, key: &str) -> Result<BytesFrame, RedisError>;
}

impl GenericHandler for CommandHandler {
    fn ping(&self, msg: Option<String>) -> Result<BytesFrame, RedisError> {
        match msg {
            Some(m) => Ok(encode_simple_string(m.into())),
            None => Ok(encode_simple_string("PONG".into())),
        }
    }

    fn echo(&self, message: &str) -> Result<BytesFrame, RedisError> {
        Ok(encode_simple_string(message.into()))
    }

    fn exists(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let mut count = 0;
        for key in keys {
            if self.mem_storage.exists(key) {
                count += 1;
            }
        }
        Ok(encode_integer(count))
    }

    fn ttl(&self, key: &str) -> Result<BytesFrame, RedisError> {
        Ok(encode_integer(self.mem_storage.ttl(key)))
    }

    fn expire(&mut self, key: &str, ttl: u64) -> Result<BytesFrame, RedisError> {
        match self.mem_storage.expire(key, ttl as i64) {
            true => Ok(encode_integer(1)),
            false => Ok(encode_integer(0)),
        }
    }

    fn scan(
        &self,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
        type_filter: Option<&str>,
    ) -> Result<BytesFrame, RedisError> {
        let (next_cursor, keys) = self.mem_storage.scan(cursor, pattern, count, type_filter);
        let mut arr = Vec::with_capacity(keys.len() + 1);

        arr.push(encode_simple_string(format!("{}", next_cursor)));
        for key in keys {
            arr.push(BytesFrame::BulkString(key.into()));
        }
        Ok(BytesFrame::Array(arr))
    }

    fn keys(&self, pattern: &str) -> Result<BytesFrame, RedisError> {
        let keys = self.mem_storage.keys(pattern);
        Ok(encode_simple_strings(keys))
    }

    fn del(&mut self, key: &str) -> Result<BytesFrame, RedisError> {
        let deleted = self.mem_storage.del(key);
        match deleted {
            true => Ok(encode_integer(1)),
            false => Ok(encode_integer(0)),
        }
    }

    fn get_type(&self, key: &str) -> Result<BytesFrame, RedisError> {
        let type_name = self.mem_storage.get_type(key);
        match type_name {
            Some(t) => Ok(encode_simple_string(t)),
            None => Ok(encode_nil()),
        }
    }
}
