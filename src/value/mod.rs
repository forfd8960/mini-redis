use std::{collections::HashMap, time::Instant};

use ordered_float::OrderedFloat;
use skiplist::SkipList;

use crate::{errors::RedisError, value::list::ListValue};

pub mod list;


pub type Entry = Vec<u8>;
pub type HashEntry = (String, Entry);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListInsertPivot {
    Before,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListMoveDirection {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    pub value: RedisValue,
    pub type_name: String, // "string", "list", "hash", "set", "zset"
    pub expire_time: Option<Instant>,
    pub last_access: Instant, // for LRU eviction
}

// redis storage values
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisValue {
    String(StringValue),
    List(ListValue),
    Hash(HashValue),
    Set(SetValue),
    SortedSet(SortedSetValue),
    Nil, // for non-existing keys
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StringValue {
    Int(i64),
    Raw(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashValue {
    pub items: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetValue {
    pub items: HashMap<String, ()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedSetValue {
    pub members: HashMap<String, OrderedFloat<f64>>, // member -> score
    pub sorted_members: SkipList<(OrderedFloat<f64>, String)>, // sorted by score, then by member
}

impl RedisValue {
    pub fn type_name(&self) -> &str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Hash(_) => "hash",
            RedisValue::Set(_) => "set",
            RedisValue::SortedSet(_) => "zset",
            RedisValue::Nil => "nil",
        }
    }

    pub fn is_expired(&self, expire_time: &Option<Instant>) -> bool {
        match expire_time {
            Some(t) => Instant::now() > *t,
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RedisValue::String(s) => match s {
                StringValue::Int(i) => i.to_string().len(),
                StringValue::Raw(s) => s.len(),
            },
            RedisValue::List(l) => l.items.len(),
            RedisValue::Hash(h) => h.items.len(),
            RedisValue::Set(s) => s.items.len(),
            RedisValue::SortedSet(z) => z.members.len(),
            RedisValue::Nil => 0,
        }
    }

    pub fn left_extend_list(&mut self, other: Vec<String>) -> Result<(), RedisError> {
        match self {
            RedisValue::List(l) => {
                other.into_iter().rev().for_each(|v| l.items.push_front(v));
                Ok(())
            }
            _ => Err(RedisError::StorageError(
                "Cannot extend non-list value".to_string(),
            )),
        }
    }

    pub fn extend_list(&mut self, other: Vec<String>) -> Result<(), RedisError> {
        match self {
            RedisValue::List(l) => {
                other.into_iter().for_each(|v| l.items.push_back(v));
                Ok(())
            }
            _ => Err(RedisError::StorageError(
                "Cannot extend non-list value".to_string(),
            )),
        }
    }

    pub fn pop_list(&mut self, count: usize, from_left: bool) -> Result<Vec<String>, RedisError> {
        match self {
            RedisValue::List(l) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    let item = if from_left {
                        l.items.pop_front()
                    } else {
                        l.items.pop_back()
                    };
                    match item {
                        Some(v) => result.push(v),
                        None => break,
                    }
                }
                Ok(result)
            }
            _ => Err(RedisError::StorageError(
                "Cannot pop from non-list value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[test]
    fn test_list_lrem() {
        let mut list = ListValue::new(10);
        list.items.extend(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);

        assert_eq!(list.lrem(0, "a"), 2);
        assert_eq!(
            list.items,
            VecDeque::from(vec![
                "b".to_string(),
                "c".to_string(),
                "b".to_string(),
                "c".to_string(),
            ])
        );

        list.items
            .extend(vec!["a".to_string(), "a".to_string(), "a".to_string()]);

        assert_eq!(list.lrem(2, "a"), 2);
        assert_eq!(
            list.items,
            VecDeque::from(vec![
                "b".to_string(),
                "c".to_string(),
                "b".to_string(),
                "c".to_string(),
                "a".to_string(),
            ])
        );

        assert_eq!(list.lrem(-1, "a"), 1);
        assert_eq!(
            list.items,
            VecDeque::from(vec![
                "b".to_string(),
                "c".to_string(),
                "b".to_string(),
                "c".to_string(),
            ])
        );
    }
}
