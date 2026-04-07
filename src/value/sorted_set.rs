use std::collections::HashMap;

use ordered_float::OrderedFloat;
use skiplist::SkipList;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedSetValue {
    pub members: HashMap<String, OrderedFloat<f64>>, // member -> score
    pub sorted_members: SkipList<(OrderedFloat<f64>, String)>, // sorted by score, then by member
}
