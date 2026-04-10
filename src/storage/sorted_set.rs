use ordered_float::OrderedFloat;

use crate::{
    command::sorted_set::{
        Aggregate, LexBound, Limit, PopResult, RangeBy, RankResult, ScanPage, ScoreBound,
        ScoredMember, ZAddOptions,
    },
    storage::mem::MemStore,
    value::{RedisValue, sorted_set::SortedSetValue},
};

/// All Redis sorted set commands.
/*#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortedSetCommand {
    // ── Write commands ────────────────────────────────────────────────────────
    /// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
    ///
    /// Add or update members with their scores. Flags control
    /// insert/update behaviour and score comparison guards.
    ZAdd {
        key: String,
        members: Vec<(OrderedFloat<f64>, String)>,
        options: ZAddOptions,
    },

    /// ZINCRBY key increment member
    ///
    /// Increment the score of `member` by `increment`.
    /// Returns the new score.
    ZIncrBy {
        key: String,
        increment: OrderedFloat<f64>,
        member: String,
    },

    /// ZREM key member [member ...]
    ///
    /// Remove one or more members. Returns the count of removed members.
    ZRem { key: String, members: Vec<String> },

    // ── Pop commands ──────────────────────────────────────────────────────────
    /// ZPOPMIN key [count]
    ///
    /// Remove and return the members with the lowest scores.
    ZPopMin { key: String, count: Option<u64> },

    /// ZPOPMAX key [count]
    ///
    /// Remove and return the members with the highest scores.
    ZPopMax { key: String, count: Option<u64> },

    /// BZPOPMIN key [key ...] timeout
    ///
    /// Blocking ZPOPMIN across one or more keys.
    /// Blocks until an element is available or timeout expires (0 = forever).
    BZPopMin {
        keys: Vec<String>,
        timeout: OrderedFloat<f64>,
    },

    /// BZPOPMAX key [key ...] timeout
    ///
    /// Blocking ZPOPMAX across one or more keys.
    BZPopMax {
        keys: Vec<String>,
        timeout: OrderedFloat<f64>,
    },

    /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    ///
    /// Pop from the first non-empty sorted set in the key list.
    ZMPop {
        keys: Vec<String>,
        from_max: bool,
        count: Option<u64>,
    },

    /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    ///
    /// Blocking variant of ZMPOP.
    BZMPop {
        timeout: OrderedFloat<f64>,
        keys: Vec<String>,
        from_max: bool,
        count: Option<u64>,
    },

    // ── Range read commands ───────────────────────────────────────────────────
    /// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    ///
    /// Return a range of members. The range axis (rank/score/lex),
    /// direction, pagination, and score inclusion are all configurable.
    ZRange {
        key: String,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    },

    /// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    ///
    /// Like ZRANGE but stores the result in `dst` instead of returning it.
    ZRangeStore {
        dst: String,
        src: String,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
    },

    // ── Rank & score lookups ──────────────────────────────────────────────────
    /// ZRANK key member [WITHSCORE]
    ///
    /// Return the 0-indexed rank of `member` ordered from lowest score.
    ZRank {
        key: String,
        member: String,
        with_score: bool,
    },

    /// ZREVRANK key member [WITHSCORE]
    ///
    /// Return the 0-indexed rank of `member` ordered from highest score.
    ZRevRank {
        key: String,
        member: String,
        with_score: bool,
    },

    /// ZSCORE key member
    ///
    /// Return the score of `member`, or None if it does not exist.
    ZScore { key: String, member: String },

    /// ZMSCORE key member [member ...]
    ///
    /// Return scores for multiple members in one round-trip.
    /// Missing members return None in the result list.
    ZMScore { key: String, members: Vec<String> },

    // ── Count commands ────────────────────────────────────────────────────────
    /// ZCARD key
    ///
    /// Return the total number of members in the sorted set.
    ZCard { key: String },

    /// ZCOUNT key min max
    ///
    /// Count members with scores between min and max (inclusive).
    ZCount {
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    },

    /// ZLEXCOUNT key min max
    ///
    /// Count members between lexicographic bounds.
    /// Only meaningful when all members share the same score.
    ZLexCount {
        key: String,
        min: LexBound,
        max: LexBound,
    },

    // ── Remove-by-range commands ──────────────────────────────────────────────
    /// ZREMRANGEBYRANK key start stop
    ///
    /// Remove all members with rank between start and stop (0-indexed).
    ZRemRangeByRank { key: String, start: i64, stop: i64 },

    /// ZREMRANGEBYSCORE key min max
    ///
    /// Remove all members with scores between min and max.
    ZRemRangeByScore {
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    },

    /// ZREMRANGEBYLEX key min max
    ///
    /// Remove all members between lexicographic bounds.
    ZRemRangeByLex {
        key: String,
        min: LexBound,
        max: LexBound,
    },

    // ── Set algebra commands ──────────────────────────────────────────────────
    /// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
    ///
    /// Compute the union of sorted sets and store the result in `destination`.
    ZUnionStore {
        dst: String,
        keys: Vec<String>,
        weights: Option<Vec<OrderedFloat<f64>>>,
        aggregate: Aggregate,
    },

    /// ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
    ///
    /// Compute the intersection of sorted sets and store the result in `dst`.
    ZInterStore {
        dst: String,
        keys: Vec<String>,
        weights: Option<Vec<OrderedFloat<f64>>>,
        aggregate: Aggregate,
    },

    /// ZDIFFSTORE dst numkeys key [key ...]
    ///
    /// Compute the difference (keys[0] minus all others) and store in `dst`.
    ZDiffStore { dst: String, keys: Vec<String> },

    /// ZUNION numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    ///
    /// Compute union without storing. Returns members (and optionally scores).
    ZUnion {
        keys: Vec<String>,
        weights: Option<Vec<OrderedFloat<f64>>>,
        aggregate: Aggregate,
        with_scores: bool,
    },

    /// ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    ///
    /// Compute intersection without storing.
    ZInter {
        keys: Vec<String>,
        weights: Option<Vec<OrderedFloat<f64>>>,
        aggregate: Aggregate,
        with_scores: bool,
    },

    /// ZDIFF numkeys key [key ...] [WITHSCORES]
    ///
    /// Compute difference without storing.
    ZDiff {
        keys: Vec<String>,
        with_scores: bool,
    },

    /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
    ///
    /// Return the cardinality of the intersection.
    /// If limit is set, stop counting once it is reached (Redis 7.0+).
    ZInterCard {
        keys: Vec<String>,
        limit: Option<u64>,
    },

    // ── Random sampling ───────────────────────────────────────────────────────
    /// ZRANDMEMBER key [count [WITHSCORES]]
    ///
    /// Return random members.
    /// Positive count → distinct members (up to ZCARD).
    /// Negative count → may repeat members.
    ZRandMember {
        key: String,
        count: Option<i64>,
        with_scores: bool,
    },

    // ── Cursor iteration ──────────────────────────────────────────────────────
    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    ///
    /// Incrementally iterate over members and scores without blocking.
    /// Returns the next cursor and a batch of (member, score) pairs.
    ZScan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<u64>,
    },
}*/

pub trait SortedSetStore {
    fn zadd(
        &mut self,
        key: &str,
        members: Vec<(OrderedFloat<f64>, String)>,
        options: &ZAddOptions,
    ) -> usize;

    fn zincrby(
        &mut self,
        key: &str,
        increment: OrderedFloat<f64>,
        member: &str,
    ) -> Option<OrderedFloat<f64>>;

    fn zrem(&mut self, key: &str, members: &[String]) -> usize;

    fn zpopmin(&mut self, key: &str, count: Option<u64>) -> Vec<ScoredMember>;
    fn zpopmax(&mut self, key: &str, count: Option<u64>) -> Vec<ScoredMember>;

    fn bzpopmin(&mut self, keys: &[&str], timeout: f64) -> Option<PopResult>;
    fn bzpopmax(&mut self, keys: &[&str], timeout: f64) -> Option<PopResult>;

    fn zmpop(&mut self, keys: &[&str], from_max: bool, count: Option<u64>) -> Option<PopResult>;
    fn bzmpop(
        &mut self,
        timeout: f64,
        keys: &[&str],
        from_max: bool,
        count: Option<u64>,
    ) -> Option<PopResult>;

    fn zrange(
        &self,
        key: &str,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    ) -> Vec<ScoredMember>;

    fn zrangestore(
        &mut self,
        dst: &str,
        src: &str,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
    ) -> usize;

    fn zrank(&self, key: &str, member: &str, with_score: bool) -> Option<RankResult>;
    fn zrevrank(&self, key: &str, member: &str, with_score: bool) -> Option<RankResult>;
    fn zscore(&self, key: &str, member: &str) -> Option<OrderedFloat<f64>>;
    fn zmscore(&self, key: &str, members: &[String]) -> Vec<Option<OrderedFloat<f64>>>;

    fn zcard(&self, key: &str) -> usize;
    fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> usize;
    fn zlexcount(&self, key: &str, min: LexBound, max: LexBound) -> usize;

    fn zremrangebyrank(&mut self, key: &str, start: i64, stop: i64) -> usize;
    fn zremrangebyscore(&mut self, key: &str, min: ScoreBound, max: ScoreBound) -> usize;
    fn zremrangebylex(&mut self, key: &str, min: LexBound, max: LexBound) -> usize;

    fn zunionstore(
        &mut self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
    ) -> usize;

    fn zinterstore(
        &mut self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
    ) -> usize;

    fn zdiffstore(&mut self, dst: &str, keys: &[&str]) -> usize;

    fn zunion(
        &self,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> Vec<ScoredMember>;

    fn zinter(
        &self,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> Vec<ScoredMember>;

    fn zdiff(&self, keys: &[&str], with_scores: bool) -> Vec<ScoredMember>;
    fn zintercard(&self, keys: &[&str], limit: Option<u64>) -> usize;

    fn zrandmember(&self, key: &str, count: Option<i64>, with_scores: bool) -> Vec<ScoredMember>;

    fn zscan(&self, key: &str, cursor: u64, pattern: Option<&str>, count: Option<u64>) -> ScanPage;
}

impl SortedSetStore for MemStore {
    fn zadd(
        &mut self,
        key: &str,
        members: Vec<(OrderedFloat<f64>, String)>,
        options: &ZAddOptions,
    ) -> usize {
        let len = members.len();

        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::SortedSet(set) = &mut *v {
                return set.zadd(members, options);
            }

            return 0;
        }

        self.data.insert(
            key.to_string(),
            RedisValue::SortedSet(SortedSetValue::from_vec(members)),
        );

        len
    }

    fn zincrby(
        &mut self,
        key: &str,
        increment: OrderedFloat<f64>,
        member: &str,
    ) -> Option<OrderedFloat<f64>> {
        unimplemented!()
    }

    fn zrem(&mut self, key: &str, members: &[String]) -> usize {
        unimplemented!()
    }

    fn zpopmin(&mut self, key: &str, count: Option<u64>) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn zpopmax(&mut self, key: &str, count: Option<u64>) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn bzpopmin(&mut self, keys: &[&str], timeout: f64) -> Option<PopResult> {
        unimplemented!()
    }

    fn bzpopmax(&mut self, keys: &[&str], timeout: f64) -> Option<PopResult> {
        unimplemented!()
    }

    fn zmpop(&mut self, keys: &[&str], from_max: bool, count: Option<u64>) -> Option<PopResult> {
        unimplemented!()
    }

    fn bzmpop(
        &mut self,
        timeout: f64,
        keys: &[&str],
        from_max: bool,
        count: Option<u64>,
    ) -> Option<PopResult> {
        unimplemented!()
    }

    fn zrange(
        &self,
        key: &str,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    ) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn zrangestore(
        &mut self,
        dst: &str,
        src: &str,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
    ) -> usize {
        unimplemented!()
    }

    fn zrank(&self, key: &str, member: &str, with_score: bool) -> Option<RankResult> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::SortedSet(set) = v.value() {
                let (rank, score) = set.zrank(member);

                if rank.is_none() {
                    return None;
                }

                let r_score = if with_score {
                    Some(score.unwrap().into_inner())
                } else {
                    None
                };
                return Some(RankResult {
                    rank: rank.unwrap() as u64,
                    score: r_score,
                });
            }

            return None;
        }

        None
    }

    fn zrevrank(&self, key: &str, member: &str, with_score: bool) -> Option<RankResult> {
        unimplemented!()
    }

    fn zscore(&self, key: &str, member: &str) -> Option<OrderedFloat<f64>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::SortedSet(set) = v.value() {
                return set.zscore(member);
            }

            return None;
        }

        None
    }

    fn zmscore(&self, key: &str, members: &[String]) -> Vec<Option<OrderedFloat<f64>>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::SortedSet(set) = v.value() {
                return members.iter().map(|member| set.zscore(member)).collect();
            }

            return vec![None; members.len()];
        }

        vec![None; members.len()]
    }

    fn zcard(&self, key: &str) -> usize {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::SortedSet(zset) = &mut *v {
                return zset.zcard();
            }
        }

        0
    }

    fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> usize {
        unimplemented!()
    }

    fn zlexcount(&self, key: &str, min: LexBound, max: LexBound) -> usize {
        unimplemented!()
    }

    fn zremrangebyrank(&mut self, key: &str, start: i64, stop: i64) -> usize {
        unimplemented!()
    }

    fn zremrangebyscore(&mut self, key: &str, min: ScoreBound, max: ScoreBound) -> usize {
        unimplemented!()
    }

    fn zremrangebylex(&mut self, key: &str, min: LexBound, max: LexBound) -> usize {
        unimplemented!()
    }

    fn zunionstore(
        &mut self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
    ) -> usize {
        unimplemented!()
    }

    fn zinterstore(
        &mut self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
    ) -> usize {
        unimplemented!()
    }

    fn zdiffstore(&mut self, dst: &str, keys: &[&str]) -> usize {
        unimplemented!()
    }

    fn zunion(
        &self,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn zinter(
        &self,
        keys: &[&str],
        weights: Option<&[OrderedFloat<f64>]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn zdiff(&self, keys: &[&str], with_scores: bool) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn zintercard(&self, keys: &[&str], limit: Option<u64>) -> usize {
        unimplemented!()
    }

    fn zrandmember(&self, key: &str, count: Option<i64>, with_scores: bool) -> Vec<ScoredMember> {
        unimplemented!()
    }

    fn zscan(&self, key: &str, cursor: u64, pattern: Option<&str>, count: Option<u64>) -> ScanPage {
        unimplemented!()
    }
}
