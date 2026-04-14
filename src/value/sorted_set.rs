use std::collections::HashMap;

use ordered_float::OrderedFloat;
use skiplist::OrderedSkipList;

use crate::command::sorted_set::{LexBound, Limit, RangeBy, ScoreBound, ScoredMember, ZAddOptions};

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

    pub fn zrem(&mut self, members: Vec<String>) -> usize {
        let mut removed = 0;
        for member in members {
            if let Some(score) = self.members.remove(&member) {
                // remove from skiplist
                let idx = self
                    .sorted_members
                    .iter()
                    .position(|(s, m)| *s == score && *m == member)
                    .unwrap();

                self.sorted_members.remove(idx);
                removed += 1;
            }
        }
        removed
    }

    pub fn zrange(
        &self,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    ) -> Vec<ScoredMember> {
        let len = self.zcard();
        let mut members = Vec::new();

        let iter: Box<dyn Iterator<Item = &(OrderedFloat<f64>, String)>> = if rev {
            Box::new(self.sorted_members.iter().rev())
        } else {
            Box::new(self.sorted_members.iter())
        };

        for (idx, (score, member)) in iter.enumerate() {
            if let Some(ref lmt) = limit {
                if idx < lmt.offset as usize {
                    continue;
                }
            }

            let score_f64 = score.into_inner();
            let member_str = member.as_str();

            let in_range = match &range {
                RangeBy::Rank { start, stop } => {
                    let (rank_start, rank_stop) = compute_range_start_stop(*start, *stop, len);
                    idx >= rank_start && idx <= rank_stop
                }
                RangeBy::Score { min, max } => {
                    compare_score_f64_with_scorebound(score_f64, min, max)
                }
                RangeBy::Lex { min, max } => compare_member_with_lexbound(member_str, min, max),
            };

            if in_range {
                if with_scores {
                    members.push(ScoredMember {
                        member: member.clone(),
                        score: Some(score.into_inner()),
                    });
                } else {
                    members.push(ScoredMember {
                        member: member.clone(),
                        score: None, // dummy score when not requested
                    });
                }

                if let Some(ref lmt) = limit {
                    if members.len() >= lmt.count as usize {
                        break;
                    }
                }
            }
        }

        members
    }
}

fn compute_range_start_stop(start: i64, stop: i64, total_len: usize) -> (usize, usize) {
    let total_len = total_len as i64;

    let start = if start < 0 {
        (total_len + start).max(0)
    } else {
        start
    };

    let stop = if stop < 0 {
        (total_len + stop).max(0)
    } else {
        stop
    };

    let start = start as usize;
    let stop = stop as usize;

    (start, stop)
}

fn compare_score_f64_with_scorebound(score: f64, min: &ScoreBound, max: &ScoreBound) -> bool {
    let min_ok = match min {
        ScoreBound::Inclusive(min) => score >= min.into_inner(),
        ScoreBound::Exclusive(min) => score > min.into_inner(),
        ScoreBound::NegInf => true,
        ScoreBound::PosInf => false,
    };

    let max_ok = match max {
        ScoreBound::Inclusive(max) => score <= max.into_inner(),
        ScoreBound::Exclusive(max) => score < max.into_inner(),
        ScoreBound::NegInf => false,
        ScoreBound::PosInf => true,
    };

    min_ok && max_ok
}

fn compare_member_with_lexbound(member_str: &str, min: &LexBound, max: &LexBound) -> bool {
    let min_ok = match min {
        LexBound::Inclusive(min) => member_str >= min.as_str(),
        LexBound::Exclusive(min) => member_str > min.as_str(),
        LexBound::NegInf => true,
        LexBound::PosInf => false,
    };

    let max_ok = match max {
        LexBound::Inclusive(max) => member_str <= max.as_str(),
        LexBound::Exclusive(max) => member_str < max.as_str(),
        LexBound::NegInf => false,
        LexBound::PosInf => true,
    };

    min_ok && max_ok
}
