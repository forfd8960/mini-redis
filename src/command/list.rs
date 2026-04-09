use redis_protocol::resp2::types::BytesFrame;

use crate::{
    command::CommandHandler,
    errors::RedisError,
    protocol::encoder::{encode_error, encode_nil, encode_ok, encode_simple_string},
    storage::Storage,
    value::{ListInsertPivot, ListMoveDirection},
};

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
        &mut self,
        key: &str,
        pivot: &str,
        value: &str,
        position: ListInsertPivot,
    ) -> Result<BytesFrame, RedisError>;

    fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<BytesFrame, RedisError>;

    fn lmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<BytesFrame, RedisError>;

    fn blpop(&mut self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError>;
    fn brpop(&mut self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError>;
    fn blmove(
        &mut self,
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
        &mut self,
        key: &str,
        pivot: &str,
        value: &str,
        position: ListInsertPivot,
    ) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.linsert(key, position, pivot, value)?;
        let response = if res {
            encode_ok()
        } else {
            encode_error("Pivot not found or key does not exist")
        };
        Ok(response)
    }

    fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<BytesFrame, RedisError> {
        let _ = self.mem_storage.lset(key, index, value)?;
        Ok(encode_ok())
    }

    fn lmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.lmove(src, dest, source_side, dest_side)?;
        match res {
            Some(v) => Ok(encode_simple_string(v)),
            None => Ok(encode_nil()),
        }
    }

    fn blpop(&mut self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn brpop(&mut self, keys: Vec<&str>, timeout: u64) -> Result<BytesFrame, RedisError> {
        todo!()
    }

    fn blmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
        timeout: u64,
    ) -> Result<BytesFrame, RedisError> {
        todo!()
    }
}
