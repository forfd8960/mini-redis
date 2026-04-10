use std::collections::HashMap;

use ordered_float::OrderedFloat;
use skiplist::OrderedSkipList;

use crate::command::sorted_set::ZAddOptions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedSetValue {
    pub members: HashMap<String, OrderedFloat<f64>>, // member -> score
    pub sorted_members: OrderedSkipList<(OrderedFloat<f64>, String)>, // sorted by score, then by member
}

impl SortedSetValue {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            sorted_members: OrderedSkipList::new(),
        }
    }

    pub fn from_vec(members: Vec<(OrderedFloat<f64>, String)>) -> Self {
        let mut val = Self::new();
        for (score, member) in members {
            val.members.insert(member.clone(), score);
            val.sorted_members.insert((score, member));
        }

        val
    }

    pub fn zcard(&self) -> usize {
        self.members.len()
    }

    pub fn zrank(&self, member: &str) -> (Option<usize>, Option<OrderedFloat<f64>>) {
        let rank = self.sorted_members.iter().position(|(_, m)| m == member);

        let score = self.zscore(member);
        (rank, score)
    }

    pub fn zrevrank(&self, member: &str) -> (Option<usize>, Option<OrderedFloat<f64>>) {
        let rank = self
            .sorted_members
            .iter()
            .rev()
            .position(|(_, m)| m == member);

        let score = self.zscore(member);
        (rank, score)
    }

    pub fn zadd(
        &mut self,
        entries: Vec<(OrderedFloat<f64>, String)>,
        _options: &ZAddOptions,
    ) -> usize {
        let mut added = 0;
        for (score, member) in entries {
            if let Some(old_score) = self.members.insert(member.clone(), score) {
                if old_score != score {
                    // remove old position from skiplist
                    let idx = self
                        .sorted_members
                        .iter()
                        .position(|(s, m)| *s == old_score && *m == member)
                        .unwrap();

                    self.sorted_members.remove(idx);

                    // re-insert with new score
                    self.sorted_members.insert((score, member));
                }
            } else {
                self.sorted_members.insert((score, member));
                added += 1;
            }
        }
        added
    }

    pub fn zscore(&self, member: &str) -> Option<OrderedFloat<f64>> {
        self.members.get(member).cloned()
    }
}
