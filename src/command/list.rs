use redis_protocol::resp2::types::BytesFrame;

use crate::{
    command::CommandHandler,
    errors::RedisError,
    protocol::encoder::{encode_error, encode_ok},
    storage::{ListInsertPivot, ListMoveDirection, Storage},
};

pub trait ListHandler {
    fn lpush(&mut self, key: &str, values: &[String]) -> Result<BytesFrame, RedisError>;
    fn rpush(&mut self, key: &str, values: &[String]) -> Result<BytesFrame, RedisError>;
    fn lpop(&mut self, key: &str, count: usize) -> Result<BytesFrame, RedisError>;
    fn rpop(&mut self, key: &str, count: usize) -> Result<BytesFrame, RedisError>;
    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<BytesFrame, RedisError>;
    fn lrem(&mut self, key: &str, count: i64, value: &str) -> Result<BytesFrame, RedisError>;
    fn lindex(&self, key: &str, index: i64) -> Result<BytesFrame, RedisError>;
    fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<BytesFrame, RedisError>;
    fn linsert(
        &self,
        key: &str,
        pivot: &str,
        value: &str,
        position: ListInsertPivot,
    ) -> Result<BytesFrame, RedisError>;

    fn lset(&self, key: &str, index: i64, value: &str) -> Result<BytesFrame, RedisError>;

    fn lmove(
        &self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<BytesFrame, RedisError>;

    fn blpop(&self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError>;
    fn brpop(&self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError>;
    fn blmove(
        &self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
        timeout: u64,
    ) -> Result<BytesFrame, RedisError>;
}

impl ListHandler for CommandHandler {
    fn lpush(&mut self, key: &str, values: &[String]) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.lpush(key, values.to_vec())?;
        Ok(BytesFrame::Integer(count as i64))
    }

    fn rpush(&mut self, key: &str, values: &[String]) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.rpush(key, values.to_vec())?;
        Ok(BytesFrame::Integer(count as i64))
    }

    fn lpop(&mut self, key: &str, count: usize) -> Result<BytesFrame, RedisError> {
        let result = self.mem_storage.lpop(key, count)?;
        match result {
            Some(items) => Ok(BytesFrame::Array(
                items
                    .into_iter()
                    .map(|s| BytesFrame::BulkString(s.into()))
                    .collect(),
            )),
            None => Ok(BytesFrame::Null),
        }
    }

    fn rpop(&mut self, key: &str, count: usize) -> Result<BytesFrame, RedisError> {
        let result = self.mem_storage.rpop(key, count)?;
        match result {
            Some(items) => Ok(BytesFrame::Array(
                items
                    .into_iter()
                    .map(|s| BytesFrame::BulkString(s.into()))
                    .collect(),
            )),
            None => Ok(BytesFrame::Null),
        }
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<BytesFrame, RedisError> {
        let result = self.mem_storage.lrange(key, start, stop)?;
        match result {
            Some(items) => Ok(BytesFrame::Array(
                items
                    .into_iter()
                    .map(|s| BytesFrame::BulkString(s.into()))
                    .collect(),
            )),
            None => Ok(BytesFrame::Null),
        }
    }

    fn lrem(&mut self, key: &str, count: i64, value: &str) -> Result<BytesFrame, RedisError> {
        let result = self.mem_storage.lrem(key, count, value)?;
        Ok(BytesFrame::Integer(result as i64))
    }

    fn lindex(&self, key: &str, index: i64) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.lindex(key, index)?;
        match res {
            Some(item) => Ok(BytesFrame::BulkString(item.into())),
            None => Ok(BytesFrame::Null),
        }
    }

    fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<BytesFrame, RedisError> {
        let result = self.mem_storage.ltrim(key, start, stop)?;
        let response = if result {
            encode_ok()
        } else {
            encode_error("Fail to trim list")
        };
        Ok(response)
    }

    fn linsert(
        &self,
        key: &str,
        pivot: &str,
        value: &str,
        position: ListInsertPivot,
    ) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn lset(&self, key: &str, index: i64, value: &str) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn lmove(
        &self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn blpop(&self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn brpop(&self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn blmove(
        &self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
        timeout: u64,
    ) -> Result<BytesFrame, RedisError> {
        todo!()
    }
}
