use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

use ordered_float::OrderedFloat;
use skiplist::SkipList;

pub type Entry = Vec<u8>;

pub type HashEntry = (String, Entry);

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
pub struct ListValue {
    pub items: VecDeque<Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashValue {
    pub items: HashMap<String, Entry>,
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
