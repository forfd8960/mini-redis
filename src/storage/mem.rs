use std::{
    collections::VecDeque,
    time::{Duration, Instant},
    vec,
};

use dashmap::DashMap;

use crate::{
    errors::RedisError,
    storage::{SetOptions, SetTTL, Storage},
    value::{
        HashEntry, ListInsertPivot, ListMoveDirection, RedisValue, StringValue, hash::HashValue,
        list::ListValue,
    },
};

pub struct MemStore {
    pub data: DashMap<String, RedisValue>,
    pub expire_table: DashMap<String, Instant>,
}

impl MemStore {
    pub fn new(cap: usize) -> Self {
        Self {
            data: DashMap::with_capacity(cap),
            expire_table: DashMap::with_capacity(cap),
        }
    }

    pub fn all_set_members(&self, keys: Vec<&str>) -> Vec<Vec<String>> {
        keys.iter()
            .filter_map(|key| {
                self.data.get(*key).and_then(|v| {
                    if let RedisValue::Set(set) = &*v {
                        Some(set.items.iter().cloned().collect::<Vec<String>>())
                    } else {
                        None
                    }
                })
            })
            .collect::<Vec<Vec<String>>>()
    }
}

impl Storage for MemStore {
    fn get_type(&self, key: &str) -> Option<String> {
        self.data.get(key).map(|v| match &v.value() {
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
    fn ttl(&self, key: &str) -> i64 {
        let value = self.expire_table.get(key);
        if value.is_none() {
            return if self.data.contains_key(key) { -1 } else { -2 };
        }

        let expire_time = *value.unwrap();
        if expire_time > Instant::now() {
            let ttl = expire_time.duration_since(Instant::now()).as_secs() as i64;
            ttl
        } else {
            -2 // expired
        }
    }

    fn expire(&mut self, key: &str, ttl: i64) -> bool {
        if self.data.contains_key(key) {
            if ttl > 0 {
                self.expire_table.insert(
                    key.to_string(),
                    Instant::now() + Duration::from_secs(ttl as u64),
                );
            } else {
                self.expire_table.remove(key);
            }
            true
        } else {
            false
        }
    }

    fn append(&mut self, key: &str, value: &str) -> Option<usize> {
        if let Some(mut stored) = self.data.get_mut(key) {
            match &mut *stored {
                RedisValue::String(StringValue::Raw(existing)) => {
                    existing.push_str(value);
                    Some(existing.len())
                }
                RedisValue::String(StringValue::Int(existing)) => {
                    let mut merged = existing.to_string();
                    merged.push_str(value);
                    let len = merged.len();
                    *stored = RedisValue::String(StringValue::Raw(merged));
                    Some(len)
                }
                _ => None,
            }
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::String(StringValue::Raw(value.to_string())),
            );
            Some(value.len())
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
                            if entry.value().type_name() == type_filter {
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

        // check if the key is expired
        if let Some(expire_time) = self.expire_table.get(key) {
            if *expire_time < Instant::now() {
                self.data.remove(key);
                self.expire_table.remove(key);
                return None;
            }
        }

        let data = self.data.get(key).unwrap();
        if let RedisValue::String(ref s) = *data {
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
            SetTTL::KeepTTL => self.expire_table.get(key).map(|t| *t),
        });

        self.data.insert(key.to_string(), RedisValue::String(value));

        if expire_time.is_some() {
            self.expire_table
                .insert(key.to_string(), expire_time.unwrap());
        } else {
            self.expire_table.remove(key);
        }

        true
    }

    fn incr(&mut self, key: &str) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = *v {
                *existing += 1;
                Some(*existing)
            } else {
                None
            }
        } else {
            self.data
                .insert(key.to_string(), RedisValue::String(StringValue::Int(1)));
            Some(1)
        }
    }

    fn incrby(&mut self, key: &str, increment: i64) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = *v {
                *existing += increment;
                Some(*existing)
            } else {
                None
            }
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::String(StringValue::Int(increment)),
            );
            Some(increment)
        }
    }

    fn decr(&mut self, key: &str) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = *v {
                *existing -= 1;
                Some(*existing)
            } else {
                None
            }
        } else {
            let value = RedisValue::String(StringValue::Int(-1));
            self.data.insert(key.to_string(), value);
            Some(-1)
        }
    }

    fn decrby(&mut self, key: &str, decrement: i64) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::String(StringValue::Int(ref mut existing)) = *v {
                *existing -= decrement;
                Some(*existing)
            } else {
                None
            }
        } else {
            let value = RedisValue::String(StringValue::Int(-decrement));
            self.data.insert(key.to_string(), value);
            Some(-decrement)
        }
    }

    fn mget(&self, keys: Vec<&str>) -> Vec<Option<StringValue>> {
        let mut values = Vec::new();

        for key in keys {
            if let Some(v) = self.data.get(key) {
                if let RedisValue::String(s) = v.value().clone() {
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
            let value = RedisValue::String(StringValue::Raw(value));
            self.data.insert(key, value);
        }
        true
    }

    fn getrange(&self, key: &str, start: usize, stop: usize) -> Option<String> {
        if let Some(v) = self.data.get(key) {
            let value = v.value();

            if let RedisValue::String(StringValue::Raw(s)) = value.clone() {
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
            if let RedisValue::String(StringValue::Raw(ref mut existing)) = *v {
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

            self.data.insert(
                key.to_string(),
                RedisValue::String(StringValue::Raw(new_value)),
            );
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

    fn lpush(&mut self, key: &str, values: Vec<String>) -> Result<usize, RedisError> {
        self.data
            .entry(key.to_string())
            .or_insert_with(|| {
                RedisValue::List(ListValue {
                    items: VecDeque::new(),
                })
            })
            .left_extend_list(values)?;

        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value() {
                return Ok(l.len());
            }
        }
        Ok(0)
    }

    fn rpush(&mut self, key: &str, values: Vec<String>) -> Result<usize, RedisError> {
        self.data
            .entry(key.to_string())
            .or_insert_with(|| {
                RedisValue::List(ListValue {
                    items: VecDeque::new(),
                })
            })
            .extend_list(values)?;

        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value() {
                return Ok(l.len());
            }
        }
        Ok(0)
    }

    fn lpop(&mut self, key: &str, count: usize) -> Result<Option<Vec<String>>, RedisError> {
        if let Some(mut list) = self.data.get_mut(key) {
            let ll = &mut *list;
            let values = ll.pop_list(count, true)?;
            return Ok(Some(values));
        }

        Ok(None)
    }

    fn rpop(&mut self, key: &str, count: usize) -> Result<Option<Vec<String>>, RedisError> {
        if let Some(mut list) = self.data.get_mut(key) {
            let ll = &mut *list;
            let values = ll.pop_list(count, false)?;
            return Ok(Some(values));
        }

        Ok(None)
    }

    /*
    LRANGE mylist 0 -1        # get all elements (0 = first, -1 = last)
    LRANGE mylist 0 4         # get first 5 elements
    LRANGE mylist -3 -1       # get last 3 elements
    */
    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Option<Vec<String>>, RedisError> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::List(l) = &v.value() {
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
            if let RedisValue::List(l) = &v.value() {
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
            if let RedisValue::List(l) = &mut *v {
                return Ok(l.lrem(count, value));
            }
        }
        Ok(0)
    }

    fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, RedisError> {
        if let Some(list) = self.data.get(key) {
            if let RedisValue::List(l) = &list.value() {
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
        if let Some(mut list) = self.data.get_mut(key) {
            if let RedisValue::List(l) = &mut *list {
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
            if let RedisValue::List(l) = &mut *v {
                return l.linsert(position, pivot, value);
            }
        }
        Ok(false)
    }

    fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<(), RedisError> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::List(l) = &mut *v {
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
        let src_list = self.data.get_mut(src);
        if src_list.is_none() {
            return Ok(None);
        }

        let dest_list = self.data.get_mut(dest);
        if dest_list.is_none() {
            return Ok(None);
        }

        if let RedisValue::List(l1) = &mut *src_list.unwrap() {
            if let RedisValue::List(l2) = &mut *dest_list.unwrap() {
                return l1.lmove(l2, source_side, dest_side);
            }
        }

        Ok(None)
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

    fn hset(&mut self, key: &str, values: Vec<HashEntry>) -> bool {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Hash(h) = &mut *v {
                return h.hset(values);
            }
        } else {
            self.data
                .insert(key.to_string(), RedisValue::Hash(HashValue::from(values)));
            return true;
        }

        false
    }

    fn hsetnx(&mut self, key: &str, field: &str, value: &str) -> bool {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Hash(h) = &mut *v {
                return h.hsetnx(field, value);
            }
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::Hash(HashValue::from(vec![(
                    field.to_string(),
                    value.to_string(),
                )])),
            );
            return true;
        }
        false
    }

    fn hget(&self, key: &str, field: &str) -> Option<HashEntry> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return h.hget(field);
            }
        }
        None
    }

    fn hmget(&self, key: &str, fields: Vec<&str>) -> Vec<HashEntry> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return h.hmget(fields);
            }
        }
        vec![]
    }

    fn hmset(&mut self, key: &str, field_values: Vec<HashEntry>) -> bool {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Hash(h) = &mut *v {
                return h.hmset(field_values);
            }
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::Hash(HashValue::from(field_values)),
            );
            return true;
        }
        false
    }

    fn hgetall(&self, key: &str) -> Option<Vec<HashEntry>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return Some(h.hgetall());
            }
        }
        None
    }

    fn hkeys(&self, key: &str) -> Option<Vec<String>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return Some(h.hkeys());
            }
        }
        None
    }

    fn hvals(&self, key: &str) -> Option<Vec<String>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return Some(h.hvals());
            }
        }
        None
    }

    fn hlen(&self, key: &str) -> Option<usize> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return Some(h.len());
            }
        }
        None
    }

    fn hexists(&self, key: &str, field: &str) -> bool {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Hash(h) = v.value() {
                return h.hexists(field);
            }
        }
        false
    }

    fn hscan(
        &self,
        key: &str,
        cursor: i64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Option<(i64, Vec<HashEntry>)> {
        todo!()
    }

    fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> Option<i64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Hash(h) = &mut *v {
                return h.hincrby(field, increment);
            }
        }
        None
    }

    fn hincrbyfloat(
        &mut self,
        key: &str,
        field: &str,
        increment: ordered_float::OrderedFloat<f64>,
    ) -> Option<f64> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Hash(h) = &mut *v {
                return h.hincrbyfloat(field, increment);
            }
        }
        None
    }

    fn hdel(&mut self, key: &str, fields: &[String]) -> usize {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Hash(h) = &mut *v {
                return h.hdel(fields);
            }
        }
        0
    }
}
