use redis_protocol::resp2::types::BytesFrame;

use crate::errors::RedisError;
use crate::protocol::encoder::encode_simple_strings;
use crate::{command::CommandHandler, storage::set::SetStore};

/*
SADD myset "a" "b"          # add members
SREM myset "a"              # remove member
SMEMBERS myset              # get all members
SCARD myset                 # count members
SISMEMBER myset "a"         # membership check → 0 or 1
SMISMEMBER myset "a" "b"    # check multiple → [1, 1]
SPOP myset                  # remove & return random
SRANDMEMBER myset 3         # random sample, no removal
SMOVE src dst "a"           # atomic move between sets
SUNION s1 s2                # all members from both
SINTER s1 s2                # only shared members
SDIFF  s1 s2                # in s1 but not s2
SUNIONSTORE dst s1 s2       # union → stored in dst
SINTERSTORE dst s1 s2       # intersection → stored in dst
SDIFFSTORE  dst s1 s2       # difference → stored in dst
SINTERCARD 2 s1 s2          # count of intersection (7.0+)
SSCAN myset 0 MATCH "u:*"   # cursor-based scan
*/
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCommand {
    SAdd(String, Vec<String>),          // sadd key member1 member2 ...
    SRem(String, Vec<String>),          // srem key member1 member2 ...
    SMembers(String),                   // smembers key
    SCard(String),                      // count members
    SIsMember(String, String),          // sismember key member
    SMIsMember(String, Vec<String>),    // smismember key member1 member2 ...
    SPop(String),                       // spop key
    SRandMember(String, Option<usize>), // srandmember key [count]
    SMove(String, String, String),      // smove src dst member
    SUnion(Vec<String>),                // sunion s1 s2 ...
    SInter(Vec<String>),                // sinter s1 s2 ...
    SDiff(Vec<String>),                 // sdiff s1 s2 ...
    SUnionStore(String, Vec<String>),   // sunionstore dst s1 s2 ...
    SInterStore(String, Vec<String>),   // sinterstore dst s1 s2 ...
    SDiffStore(String, Vec<String>),    // sdiffstore dst s1 s2 ...
    SInterCard(usize, Vec<String>, Option<usize>), // sintercard numkeys s1 s2 ... [LIMIT count]
    SScan(String, usize, Option<String>), // sscan key cursor [match pattern]
}

pub trait SetHandler {
    fn sadd(&mut self, key: &str, members: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn srem(&mut self, key: &str, members: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn smembers(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn scard(&self, key: &str) -> Result<BytesFrame, RedisError>;
    fn sismember(&self, key: &str, member: &str) -> Result<BytesFrame, RedisError>;
    fn smismember(&self, key: &str, members: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn spop(&mut self, key: &str, count: Option<usize>) -> Result<BytesFrame, RedisError>;
    fn srandmember(&self, key: &str, count: Option<usize>) -> Result<BytesFrame, RedisError>;
    fn smove(&mut self, src: &str, dst: &str, member: &str) -> Result<BytesFrame, RedisError>;
    fn sunion(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn sinter(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn sdiff(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn sunionstore(&mut self, dst: &str, keys: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn sinterstore(&mut self, dst: &str, keys: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn sdiffstore(&mut self, dst: &str, keys: Vec<&str>) -> Result<BytesFrame, RedisError>;
    fn sintercard(
        &self,
        numkeys: usize,
        keys: Vec<&str>,
        limit: Option<usize>,
    ) -> Result<BytesFrame, RedisError>;

    fn sscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<String>,
    ) -> Result<BytesFrame, RedisError>;
}

impl SetHandler for CommandHandler {
    fn sadd(&mut self, key: &str, members: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.sadd(key, members);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn srem(&mut self, key: &str, members: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.srem(key, members);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn smembers(&self, key: &str) -> Result<BytesFrame, RedisError> {
        let members = self.mem_storage.smembers(key);
        if members.is_none() {
            return Ok(BytesFrame::Null);
        }
        Ok(encode_simple_strings(members.unwrap()))
    }

    fn scard(&self, key: &str) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.scard(key);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn sismember(&self, key: &str, member: &str) -> Result<BytesFrame, RedisError> {
        let is_member = self.mem_storage.sismember(key, member);
        Ok(BytesFrame::Integer(if is_member { 1 } else { 0 }))
    }

    fn smismember(&self, key: &str, members: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let results = self.mem_storage.smismember(key, members);
        if results.is_none() {
            return Ok(BytesFrame::Null);
        }

        Ok(BytesFrame::Array(
            results
                .unwrap()
                .into_iter()
                .map(|b| BytesFrame::Integer(b))
                .collect(),
        ))
    }

    fn spop(&mut self, key: &str, count: Option<usize>) -> Result<BytesFrame, RedisError> {
        let members = self.mem_storage.spop(key, count);
        match members {
            Some(ms) => Ok(encode_simple_strings(ms)),
            None => Ok(BytesFrame::Null),
        }
    }

    fn srandmember(&self, key: &str, count: Option<usize>) -> Result<BytesFrame, RedisError> {
        let members = self.mem_storage.srandmember(key, count);
        if members.is_none() {
            return Ok(BytesFrame::Null);
        }

        Ok(encode_simple_strings(members.unwrap()))
    }

    fn smove(&mut self, src: &str, dst: &str, member: &str) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.smove(src, dst, member);
        Ok(BytesFrame::Integer(if res { 1 } else { 0 }))
    }

    fn sunion(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let members = self.mem_storage.sunion(keys);
        if members.is_none() {
            return Ok(BytesFrame::Null);
        }

        Ok(BytesFrame::Array(
            members
                .unwrap()
                .into_iter()
                .map(|m| BytesFrame::BulkString(m.into()))
                .collect(),
        ))
    }

    fn sinter(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.sinter(keys);
        if res.is_none() {
            return Ok(BytesFrame::Null);
        }

        Ok(BytesFrame::Array(
            res.unwrap()
                .into_iter()
                .map(|m| BytesFrame::BulkString(m.into()))
                .collect(),
        ))
    }

    fn sdiff(&self, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let res = self.mem_storage.sdiff(keys);
        if res.is_none() {
            return Ok(BytesFrame::Null);
        }

        Ok(BytesFrame::Array(
            res.unwrap()
                .into_iter()
                .map(|m| BytesFrame::BulkString(m.into()))
                .collect(),
        ))
    }

    fn sunionstore(&mut self, dst: &str, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.sunionstore(dst, keys);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn sinterstore(&mut self, dst: &str, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.sinterstore(dst, keys);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn sdiffstore(&mut self, dst: &str, keys: Vec<&str>) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.sdiffstore(dst, keys);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn sintercard(
        &self,
        numkeys: usize,
        keys: Vec<&str>,
        limit: Option<usize>,
    ) -> Result<BytesFrame, RedisError> {
        let count = self.mem_storage.sintercard(keys, limit);
        Ok(BytesFrame::Integer(count as i64))
    }

    fn sscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<String>,
    ) -> Result<BytesFrame, RedisError> {
        unimplemented!()
    }
}
