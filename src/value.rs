use std::collections::{HashMap, VecDeque};

use ordered_float::OrderedFloat;
use skiplist::{SkipList};

pub type Entry = Vec<u8>;

pub type HashEntry = (String, Entry);

// redis storage values
pub enum RedisValue {
    String(StringValue),
    List(ListValue),
    Hash(HashValue),
    Set(SetValue),
    SortedSet(SortedSetValue),
}

pub enum StringValue {
    Int(i64),
    Raw(Entry),
}

pub struct ListValue {
    pub items: VecDeque<Entry>,
}

pub struct HashValue {
    pub items: HashMap<String, Entry>,
}

pub struct SetValue {
    pub items: HashMap<String, ()>,
}

pub struct SortedSetValue {
    pub members: HashMap<String, OrderedFloat<f64>>, // member -> score
    pub sorted_members: SkipList<(OrderedFloat<f64>, String)>, // sorted by score, then by member
}