use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use dashmap::DashMap;

use crate::{
    errors::RedisError,
    storage::{SetOptions, SetTTL, Storage},
    value::{ListInsertPivot, ListMoveDirection, RedisValue, StringValue, Value},
    value::list::ListValue,
};

pub struct MemStore {
    // key -> (value, expire_time)
    pub data: DashMap<String, Value>,
}

impl MemStore {
    pub fn new(cap: usize) -> Self {
        Self {
            data: DashMap::with_capacity(cap),
        }
    }
}

impl Storage for MemStore {
    fn get_type(&self, key: &str) -> Option<String> {
        self.data.get(key).map(|v| match &v.value {
            RedisValue::String(_) => "string".to_string(),
            RedisValue::List(_) => "list".to_string(),
            RedisValue::Hash(_) => "hash".to_string(),
            RedisValue::Set(_) => "set".to_string(),
            RedisValue::SortedSet(_) => "zset".to_string(),
            RedisValue::Nil => "nil".to_string(),
        })
    }

    /*
        A positive integer — seconds remaining (e.g. 47)
    -1 — the key exists but has no expiry set
    -2 — the key does not exist
        */
    fn ttl(&self, key: &str) -> Option<i64> {
        self.data.get(key).map(|v| {
            if let Some(expire_time) = v.expire_time {
                let now = Instant::now();
                if expire_time > now {
                    (expire_time - now).as_secs() as i64
                } else {
                    -2 // expired
                }
            } else {
                -1 // no TTL
            }
        })
    }

    fn expire(&mut self, key: &str, ttl: i64) -> bool {
        if let Some(mut v) = self.data.get_mut(key) {
            if ttl > 0 {
                v.expire_time = Some(Instant::now() + Duration::from_secs(ttl as u64));
            } else {
                v.expire_time = None; // remove TTL
            }
            true
        } else {
            false
        }
    }

    fn append(&mut self, key: &str, value: &str) -> Option<usize> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Raw(ref mut existing)) = v.value {
                existing.push_str(value);
                Some(existing.len())
            } else {
                None
            }
        } else {
            None
        }
    }

    // SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
    fn scan(
        &self,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
        type_filter: Option<&str>,
    ) -> (i64, Vec<String>) {
        let mut res = Vec::new();
        let mut next_cursor = 0;
        let mut iter = self.data.iter().skip(cursor as usize);
        for _ in 0..count.unwrap_or(10) {
            if let Some(entry) = iter.next() {
                let key = entry.key();
                if let Ok(pat) = glob::Pattern::new(pattern.unwrap_or("*")) {
                    if pat.matches(key) {
                        if let Some(type_filter) = type_filter {
                            if &entry.value().type_name == type_filter {
                                res.push(key.clone());
                            }
                        } else {
                            res.push(key.clone());
                        }
                    }
                }
                next_cursor += 1;
            } else {
                break;
            }
        }

        (next_cursor, res)
    }

    // KEYS returns all keys matching a pattern. It's simple but comes with an important caveat.
    fn keys(&self, pattern: &str) -> Vec<String> {
        let mut res = Vec::new();
        for entry in self.data.iter() {
            let key = entry.key();
            if let Ok(pat) = glob::Pattern::new(pattern) {
                if pat.matches(key) {
                    res.push(key.clone());
                }
            }
        }

        res
    }

    fn exists(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    fn del(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    fn get(&self, key: &str) -> Option<StringValue> {
        if !self.exists(key) {
            return None;
        }

        let data = self.data.get(key).unwrap();
        if let RedisValue::String(s) = &data.value {
            Some(s.clone())
        } else {
            None
        }
    }

    fn set(&mut self, key: &str, value: StringValue, opts: Option<SetOptions>) -> bool {
        let expire_time = opts.and_then(|o| o.ttl).and_then(|ttl| match ttl {
            SetTTL::EX(seconds) => Some(Instant::now() + Duration::from_secs(seconds)),
            SetTTL::PX(milliseconds) => Some(Instant::now() + Duration::from_millis(milliseconds)),
            SetTTL::EXAT(timestamp_seconds) => {
                let now = Instant::now();
                let expire_time = Instant::now() + Duration::from_secs(timestamp_seconds);
                if expire_time > now {
                    Some(expire_time)
                } else {
                    None
                }
            }
            SetTTL::PXAT(timestamp_millis) => {
                let now = Instant::now();
                let expire_time = Instant::now() + Duration::from_millis(timestamp_millis);
                if expire_time > now {
                    Some(expire_time)
                } else {
                    None
                }
            }
            SetTTL::KeepTTL => self.data.get(key).and_then(|v| v.expire_time),
        });

        let value = Value {
            value: RedisValue::String(value),
            type_name: "string".to_string(),
            expire_time,
            last_access: Instant::now(),
        };

        self.data.insert(key.to_string(), value);
        true
    }

    fn incr(&mut self, key: &str) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = v.value {
                *existing += 1;
                Some(*existing)
            } else {
                None
            }
        } else {
            let value = Value {
                value: RedisValue::String(StringValue::Int(1)),
                type_name: "string".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            };
            self.data.insert(key.to_string(), value);
            Some(1)
        }
    }

    fn incrby(&mut self, key: &str, increment: i64) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = v.value {
                *existing += increment;
                Some(*existing)
            } else {
                None
            }
        } else {
            let value = Value {
                value: RedisValue::String(StringValue::Int(increment)),
                type_name: "string".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            };
            self.data.insert(key.to_string(), value);
            Some(increment)
        }
    }

    fn decr(&mut self, key: &str) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = v.value {
                *existing -= 1;
                Some(*existing)
            } else {
                None
            }
        } else {
            let value = Value {
                value: RedisValue::String(StringValue::Int(-1)),
                type_name: "string".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            };
            self.data.insert(key.to_string(), value);
            Some(-1)
        }
    }

    fn decrby(&mut self, key: &str, decrement: i64) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = v.value {
                *existing -= decrement;
                Some(*existing)
            } else {
                None
            }
        } else {
            let value = Value {
                value: RedisValue::String(StringValue::Int(-decrement)),
                type_name: "string".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            };
            self.data.insert(key.to_string(), value);
            Some(-decrement)
        }
    }

    fn mget(&self, keys: Vec<&str>) -> Vec<Option<StringValue>> {
        let mut values = Vec::new();

        for key in keys {
            if let Some(v) = self.data.get(key) {
                if let RedisValue::String(s) = &v.value {
                    values.push(Some(s.clone()));
                } else {
                    values.push(None);
                }
            } else {
                values.push(None);
            }
        }

        values
    }

    fn mset(&mut self, pairs: Vec<(String, String)>) -> bool {
        for (key, value) in pairs {
            let value = Value {
                value: RedisValue::String(StringValue::Raw(value)),
                type_name: "string".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            };
            self.data.insert(key, value);
        }
        true
    }

    fn getrange(&self, key: &str, start: usize, stop: usize) -> Option<String> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::String(StringValue::Raw(s)) = &v.value {
                let len = s.len();
                let start = if start < len { start } else { len };
                let stop = if stop < len { stop } else { len };
                Some(s[start..=stop].to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn setrange(&mut self, key: &str, offset: usize, value: String) -> Option<usize> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Raw(ref mut existing)) = v.value {
                if offset > existing.len() {
                    existing.push_str(&" ".repeat(offset - existing.len()));
                }
                existing.replace_range(offset..offset + value.len(), &value);
                Some(existing.len())
            } else {
                None
            }
        } else {
            let mut new_value = "\x00".repeat(offset);
            new_value.push_str(&value);
            let length = new_value.len();

            let value = Value {
                value: RedisValue::String(StringValue::Raw(new_value)),
                type_name: "string".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            };
            self.data.insert(key.to_string(), value);
            Some(length)
        }
    }

    fn strlen(&self, key: &str) -> Option<usize> {
        let value = self.get(key)?;
        if let StringValue::Raw(s) = value {
            Some(s.len())
        } else {
            None
        }
    }

    fn hset(&mut self, key: &str, values: Vec<crate::value::HashEntry>) -> bool {
        todo!()
    }

    fn hget(&self, key: &str, field: &str) -> Option<crate::value::HashEntry> {
        todo!()
    }

    fn hmget(&self, key: &str, fields: Vec<&str>) -> Vec<crate::value::HashEntry> {
        todo!()
    }

    fn hgetall(&self, key: &str) -> Option<Vec<crate::value::HashEntry>> {
        todo!()
    }

    fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> Option<i64> {
        todo!()
    }

    fn hdel(&mut self, key: &str, field: &str) -> bool {
        todo!()
    }

    fn lpush(&mut self, key: &str, values: Vec<String>) -> Result<usize, RedisError> {
        self.data
            .entry(key.to_string())
            .or_insert_with(|| Value {
                value: RedisValue::List(ListValue {
                    items: VecDeque::new(),
                }),
                type_name: "list".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            })
            .value
            .left_extend_list(values)?;

        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value {
                return Ok(l.len());
            }
        }
        Ok(0)
    }

    fn rpush(&mut self, key: &str, values: Vec<String>) -> Result<usize, RedisError> {
        self.data
            .entry(key.to_string())
            .or_insert_with(|| Value {
                value: RedisValue::List(ListValue {
                    items: VecDeque::new(),
                }),
                type_name: "list".to_string(),
                expire_time: None,
                last_access: Instant::now(),
            })
            .value
            .extend_list(values)?;

        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value {
                return Ok(l.len());
            }
        }
        Ok(0)
    }

    fn lpop(&mut self, key: &str, count: usize) -> Result<Option<Vec<String>>, RedisError> {
        let poped_values = if let Some(mut v) = self.data.get_mut(key) {
            v.value.pop_list(count, true)?
        } else {
            vec![]
        };
        Ok(Some(poped_values))
    }

    fn rpop(&mut self, key: &str, count: usize) -> Result<Option<Vec<String>>, RedisError> {
        let poped_values = if let Some(mut v) = self.data.get_mut(key) {
            v.value.pop_list(count, false)?
        } else {
            vec![]
        };

        Ok(Some(poped_values))
    }

    /*
    LRANGE mylist 0 -1        # get all elements (0 = first, -1 = last)
    LRANGE mylist 0 4         # get first 5 elements
    LRANGE mylist -3 -1       # get last 3 elements
    */
    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Option<Vec<String>>, RedisError> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value {
                let len = l.items.len() as i64;
                let start = if start >= 0 { start } else { len + start }.max(0);
                let stop = if stop >= 0 { stop } else { len + stop }.max(0);

                let items: Vec<String> = l.items.iter().cloned().collect();
                return Ok(Some(items[start as usize..=stop as usize].to_vec()));
            } else {
                return Ok(Some(vec![]));
            }
        }

        Ok(Some(vec![]))
    }

    fn llen(&self, key: &str) -> Result<usize, RedisError> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value {
                return Ok(l.len());
            }
        }
        Ok(0)
    }

    /*
    LREM mylist 2  "a"        # remove 2 occurrences of "a" from head→tail
    LREM mylist -2 "a"        # remove 2 occurrences from tail→head
    LREM mylist 0  "a"        # remove ALL occurrences of "a"
    */
    fn lrem(&mut self, key: &str, count: i64, value: &str) -> Result<usize, RedisError> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::List(l) = &mut v.value {
                return Ok(l.lrem(count, value));
            }
        }
        Ok(0)
    }

    fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, RedisError> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value {
                let len = l.items.len() as i64;
                let index = if index >= 0 { index } else { len + index };
                if index >= 0 && index < len {
                    return Ok(Some(l.items[index as usize].clone()));
                }
            }
        }
        Ok(None)
    }

    fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<bool, RedisError> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::List(l) = &mut v.value {
                return l.ltrim(start, stop);
            }
        }
        Ok(true)
    }


    /// Inserts element in the list stored at key either before or after the reference value pivot.
    /// When key does not exist, it is considered an empty list and no operation is performed.
    /// An error is returned when key exists but does not hold a list value.
    fn linsert(
        &mut self,
        key: &str,
        position: ListInsertPivot,
        pivot: &str,
        value: &str,
    ) -> Result<bool, RedisError> {
        /*
        LINSERT mylist BEFORE "x" "new"   # insert "new" before "x"
        LINSERT mylist AFTER  "x" "new"   # insert "new" after "x"
        */
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::List(l) = &mut v.value {
                return l.linsert(position, pivot, value);
            }
        }
        Ok(false)
    }

    fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<(), RedisError> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::List(l) = &mut v.value {
                return l.lset(index, value);
            }
        }
        Ok(())
    }

    fn lmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<Option<String>, RedisError> {
        todo!()
    }

    fn blpop(
        &mut self,
        keys: Vec<&str>,
        timeout: u64,
    ) -> Result<Option<(String, String)>, RedisError> {
        todo!()
    }

    fn brpop(
        &mut self,
        keys: Vec<&str>,
        timeout: u64,
    ) -> Result<Option<(String, String)>, RedisError> {
        todo!()
    }

    fn blmove(
        &mut self,
        src: &str,
        dest: &str,
        source_side: ListMoveDirection,
        dest_side: ListMoveDirection,
        timeout: u64,
    ) -> Result<Option<String>, RedisError> {
        todo!()
    }
}
