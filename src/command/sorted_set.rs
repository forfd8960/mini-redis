use std::collections::HashMap;

use ordered_float::OrderedFloat;
use redis_protocol::resp2::types::BytesFrame;
use tokio::runtime::Handle;

use crate::{
    command::{CommandHandler, HandlerResult},
    errors::RedisError,
};

// ─── Score bound ────────────────────────────────────────────────────────────

/// Represents a score boundary for range queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScoreBound {
    /// Negative infinity: `-inf`
    NegInf,
    /// Positive infinity: `+inf`
    PosInf,
    /// Inclusive bound: `1500.0`
    Inclusive(OrderedFloat<f64>),
    /// Exclusive bound: `(1500.0`
    Exclusive(OrderedFloat<f64>),
}

// ─── Lex bound ───────────────────────────────────────────────────────────────

/// Represents a lexicographic boundary for BYLEX range queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Negative infinity: `-`
    NegInf,
    /// Positive infinity: `+`
    PosInf,
    /// Inclusive bound: `[alice`
    Inclusive(String),
    /// Exclusive bound: `(alice`
    Exclusive(String),
}

// ─── Range target ────────────────────────────────────────────────────────────

/// What axis a ZRANGE query operates on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeBy {
    /// Integer rank positions (0-indexed from lowest score).
    Rank { start: i64, stop: i64 },
    /// Floating-point score range.
    Score { min: ScoreBound, max: ScoreBound },
    /// Lexicographic range (only valid when all scores are equal).
    Lex { min: LexBound, max: LexBound },
}

// ─── ZADD flags ──────────────────────────────────────────────────────────────

/// Mutually exclusive condition flags for ZADD.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ZAddCondition {
    /// Default: always add or update.
    #[default]
    Always,
    /// NX: only add new members, never update existing ones.
    OnlyNew,
    /// XX: only update existing members, never add new ones.
    OnlyExisting,
}

/// Score comparison flags for ZADD GT / LT.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ZAddComparison {
    /// Default: no comparison guard.
    #[default]
    None,
    /// GT: only update if new score is greater than current.
    GreaterThan,
    /// LT: only update if new score is less than current.
    LessThan,
}

/// Options for ZADD.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZAddOptions {
    pub condition: ZAddCondition,
    pub comparison: ZAddComparison,
    /// CH: return the number of elements changed, not just added.
    pub changed: bool,
    /// INCR: treat the score as an increment (only one member allowed).
    pub incr: bool,
}

// ─── Aggregate function ───────────────────────────────────────────────────────

/// Score aggregation strategy for ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Aggregate {
    /// SUM: add scores together (default).
    #[default]
    Sum,
    /// MIN: keep the lower score.
    Min,
    /// MAX: keep the higher score.
    Max,
}

// ─── Pagination ───────────────────────────────────────────────────────────────

/// LIMIT clause for ZRANGE BYSCORE / BYLEX.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    /// Number of elements to skip.
    pub offset: u64,
    /// Maximum number of elements to return.
    pub count: u64,
}

// ─── Main enum ────────────────────────────────────────────────────────────────

/// All Redis sorted set commands.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
    ///
    /// Compute the union of sorted sets and store the result in `dst`.
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
}

// ─── Display ─────────────────────────────────────────────────────────────────

impl std::fmt::Display for SortedSetCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZAdd { key, members, .. } => write!(f, "ZADD {key} ({} members)", members.len()),
            Self::ZIncrBy {
                key,
                increment,
                member,
            } => write!(f, "ZINCRBY {key} {increment} {member}"),
            Self::ZRem { key, members } => write!(f, "ZREM {key} ({} members)", members.len()),
            Self::ZPopMin { key, count } => write!(f, "ZPOPMIN {key} {}", count.unwrap_or(1)),
            Self::ZPopMax { key, count } => write!(f, "ZPOPMAX {key} {}", count.unwrap_or(1)),
            Self::BZPopMin { keys, timeout } => {
                write!(f, "BZPOPMIN {} timeout={timeout}", keys.join(" "))
            }
            Self::BZPopMax { keys, timeout } => {
                write!(f, "BZPOPMAX {} timeout={timeout}", keys.join(" "))
            }
            Self::ZMPop { keys, from_max, .. } => write!(
                f,
                "ZMPOP {} {}",
                keys.join(" "),
                if *from_max { "MAX" } else { "MIN" }
            ),
            Self::BZMPop {
                timeout,
                keys,
                from_max,
                ..
            } => write!(
                f,
                "BZMPOP {timeout} {} {}",
                keys.join(" "),
                if *from_max { "MAX" } else { "MIN" }
            ),
            Self::ZRange { key, rev, .. } => {
                write!(f, "ZRANGE {key}{}", if *rev { " REV" } else { "" })
            }
            Self::ZRangeStore { dst, src, .. } => write!(f, "ZRANGESTORE {dst} {src}"),
            Self::ZRank { key, member, .. } => write!(f, "ZRANK {key} {member}"),
            Self::ZRevRank { key, member, .. } => write!(f, "ZREVRANK {key} {member}"),
            Self::ZScore { key, member } => write!(f, "ZSCORE {key} {member}"),
            Self::ZMScore { key, members } => {
                write!(f, "ZMSCORE {key} ({} members)", members.len())
            }
            Self::ZCard { key } => write!(f, "ZCARD {key}"),
            Self::ZCount { key, .. } => write!(f, "ZCOUNT {key}"),
            Self::ZLexCount { key, .. } => write!(f, "ZLEXCOUNT {key}"),
            Self::ZRemRangeByRank { key, start, stop } => {
                write!(f, "ZREMRANGEBYRANK {key} {start} {stop}")
            }
            Self::ZRemRangeByScore { key, .. } => write!(f, "ZREMRANGEBYSCORE {key}"),
            Self::ZRemRangeByLex { key, .. } => write!(f, "ZREMRANGEBYLEX {key}"),
            Self::ZUnionStore { dst, keys, .. } => {
                write!(f, "ZUNIONSTORE {dst} {} keys", keys.len())
            }
            Self::ZInterStore { dst, keys, .. } => {
                write!(f, "ZINTERSTORE {dst} {} keys", keys.len())
            }
            Self::ZDiffStore { dst, keys } => write!(f, "ZDIFFSTORE {dst} {} keys", keys.len()),
            Self::ZUnion { keys, .. } => write!(f, "ZUNION {} keys", keys.len()),
            Self::ZInter { keys, .. } => write!(f, "ZINTER {} keys", keys.len()),
            Self::ZDiff { keys, .. } => write!(f, "ZDIFF {} keys", keys.len()),
            Self::ZInterCard { keys, limit } => write!(
                f,
                "ZINTERCARD {} keys{}",
                keys.len(),
                limit.map_or(String::new(), |l| format!(" LIMIT {l}"))
            ),
            Self::ZRandMember { key, count, .. } => write!(
                f,
                "ZRANDMEMBER {key}{}",
                count.map_or(String::new(), |c| format!(" {c}"))
            ),
            Self::ZScan { key, cursor, .. } => write!(f, "ZSCAN {key} {cursor}"),
        }
    }
}

/// A member returned with its score.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredMember {
    pub member: String,
    pub score: f64,
}

/// Return value of ZRANK / ZREVRANK [WITHSCORE].
#[derive(Debug, Clone, PartialEq)]
pub struct RankResult {
    /// 0-indexed rank.
    pub rank: u64,
    /// Present when WITHSCORE was requested.
    pub score: Option<f64>,
}

/// Return value of ZSCAN — a cursor and a batch of member+score pairs.
#[derive(Debug, Clone)]
pub struct ScanPage {
    /// Pass this cursor to the next ZSCAN call.
    /// 0 means the full iteration is complete.
    pub cursor: u64,
    pub items: Vec<ScoredMember>,
}

/// Return value of ZMPOP / BZMPOP — which key was popped from, and the items.
#[derive(Debug, Clone)]
pub struct PopResult {
    pub key: String,
    pub items: Vec<ScoredMember>,
}

pub trait SortedSetHandler {
    // ── Write commands ────────────────────────────────────────────────────────

    /// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
    ///
    /// Without CH: returns the number of new members added.
    /// With CH:    returns the number of members added OR updated.
    /// With INCR:  returns the new score of the single member (or None if
    ///             the NX/XX/GT/LT condition was not met).
    fn zadd(
        &self,
        key: &str,
        members: Vec<(OrderedFloat<f64>, String)>,
        options: &ZAddOptions,
    ) -> HandlerResult;

    /// ZINCRBY key increment member
    ///
    /// Atomically increment the score of `member` by `increment`.
    /// Returns the new score.
    fn zincrby(&self, key: &str, increment: f64, member: &str) -> HandlerResult;

    /// ZREM key member [member ...]
    ///
    /// Remove one or more members. Returns the count actually removed
    /// (members that did not exist are not counted).
    fn zrem(&self, key: &str, members: Vec<String>) -> HandlerResult;

    // ── Pop commands ──────────────────────────────────────────────────────────

    /// ZPOPMIN key [count]
    ///
    /// Remove and return up to `count` members with the lowest scores.
    /// Returns an empty vec when the key does not exist.
    fn zpopmin(&self, key: &str, count: Option<u64>) -> HandlerResult;

    /// ZPOPMAX key [count]
    ///
    /// Remove and return up to `count` members with the highest scores.
    fn zpopmax(&self, key: &str, count: Option<u64>) -> HandlerResult;

    /// BZPOPMIN key [key ...] timeout
    ///
    /// Blocking ZPOPMIN across one or more keys.
    /// Returns None when the timeout expires before any element is available.
    /// `timeout` is in seconds; 0.0 means block forever.
    fn bzpopmin(&self, keys: &[&str], timeout: f64) -> HandlerResult;

    /// BZPOPMAX key [key ...] timeout
    ///
    /// Blocking ZPOPMAX across one or more keys.
    fn bzpopmax(&self, keys: &[&str], timeout: f64) -> HandlerResult;

    /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    ///
    /// Pop from the first non-empty sorted set in `keys`.
    /// Returns None when all keys are empty or do not exist.
    fn zmpop(&self, keys: &[&str], from_max: bool, count: Option<u64>) -> HandlerResult;

    /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    ///
    /// Blocking variant of ZMPOP.
    fn bzmpop(
        &self,
        timeout: f64,
        keys: &[&str],
        from_max: bool,
        count: Option<u64>,
    ) -> HandlerResult;

    // ── Range read commands ───────────────────────────────────────────────────

    /// ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    ///
    /// Return members in the given rank range (low → high unless REV).
    /// The score field of each `ScoredMember` is `f64::NAN` when
    /// WITHSCORES was not requested — callers should check `with_scores`.
    fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        rev: bool,
        with_scores: bool,
    ) -> HandlerResult;

    /// ZRANGE key min max BYSCORE [REV] [LIMIT offset count] [WITHSCORES]
    fn zrange_by_score(
        &self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    ) -> HandlerResult;

    /// ZRANGE key min max BYLEX [REV] [LIMIT offset count]
    ///
    /// Only valid when all members share the same score.
    fn zrange_by_lex(
        &self,
        key: &str,
        min: LexBound,
        max: LexBound,
        rev: bool,
        limit: Option<Limit>,
    ) -> HandlerResult;

    /// ZRANGESTORE dst src start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    ///
    /// Like `zrange` / `zrange_by_score` / `zrange_by_lex` but stores the
    /// result in `dst` and returns the number of elements stored.
    fn zrange_store(
        &self,
        dst: &str,
        src: &str,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
    ) -> HandlerResult;

    /// ZRANGESTORE … BYSCORE variant.
    fn zrange_store_by_score(
        &self,
        dst: &str,
        src: &str,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<Limit>,
    ) -> HandlerResult;

    // ── Rank & score lookups ──────────────────────────────────────────────────

    /// ZRANK key member [WITHSCORE]
    ///
    /// Returns None when `member` does not exist in the sorted set.
    fn zrank(&self, key: &str, member: &str, with_score: bool) -> HandlerResult;

    /// ZREVRANK key member [WITHSCORE]
    ///
    /// Like `zrank` but ordered from highest score (rank 0 = highest).
    fn zrevrank(&self, key: &str, member: &str, with_score: bool) -> HandlerResult;

    /// ZSCORE key member
    ///
    /// Returns None when `member` does not exist.
    fn zscore(&self, key: &str, member: &str) -> HandlerResult;

    /// ZMSCORE key member [member ...]
    ///
    /// Returns one `Option<f64>` per member — None for members not in the set.
    fn zmscore(&self, key: &str, members: &[&str]) -> HandlerResult;

    // ── Count commands ────────────────────────────────────────────────────────

    /// ZCARD key
    ///
    /// Returns 0 when the key does not exist.
    fn zcard(&self, key: &str) -> HandlerResult;

    /// ZCOUNT key min max
    ///
    /// Count members with scores between `min` and `max`.
    fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> HandlerResult;

    /// ZLEXCOUNT key min max
    ///
    /// Count members between lexicographic bounds.
    fn zlexcount(&self, key: &str, min: LexBound, max: LexBound) -> HandlerResult;

    // ── Remove-by-range commands ──────────────────────────────────────────────

    /// ZREMRANGEBYRANK key start stop
    ///
    /// Remove all members with rank in [start, stop]. Returns count removed.
    fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> HandlerResult;

    /// ZREMRANGEBYSCORE key min max
    ///
    /// Remove all members with scores in [min, max]. Returns count removed.
    fn zremrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> HandlerResult;

    /// ZREMRANGEBYLEX key min max
    ///
    /// Remove all members between lexicographic bounds. Returns count removed.
    fn zremrangebylex(&self, key: &str, min: LexBound, max: LexBound) -> HandlerResult;

    // ── Set algebra — storing variants ────────────────────────────────────────

    /// ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
    ///
    /// Store the union of `keys` into `dst`. Returns the number of elements
    /// in the resulting sorted set.
    fn zunionstore(
        &self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
    ) -> HandlerResult;

    /// ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
    ///
    /// Store the intersection of `keys` into `dst`. Returns the element count.
    fn zinterstore(
        &self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
    ) -> HandlerResult;

    /// ZDIFFSTORE dst numkeys key [key ...]
    ///
    /// Store keys[0] minus all subsequent keys into `dst`. Returns element count.
    fn zdiffstore(&self, dst: &str, keys: &[&str]) -> HandlerResult;

    // ── Set algebra — non-storing variants ───────────────────────────────────

    /// ZUNION numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    fn zunion(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> HandlerResult;

    /// ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    fn zinter(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> HandlerResult;

    /// ZDIFF numkeys key [key ...] [WITHSCORES]
    fn zdiff(&self, keys: &[&str], with_scores: bool) -> HandlerResult;

    /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
    ///
    /// Returns the cardinality of the intersection, capped at `limit` if set.
    fn zintercard(&self, keys: &[&str], limit: Option<u64>) -> HandlerResult;

    // ── Random sampling ───────────────────────────────────────────────────────

    /// ZRANDMEMBER key [count [WITHSCORES]]
    ///
    /// `count` behaviour:
    ///   None     → return exactly 1 random member (or None if key missing).
    ///   positive → up to `count` distinct members.
    ///   negative → exactly `|count|` members, duplicates allowed.
    fn zrandmember(&self, key: &str, count: Option<i64>, with_scores: bool) -> HandlerResult;

    // ── Cursor iteration ──────────────────────────────────────────────────────

    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    ///
    /// Non-blocking incremental iteration. Pass the returned `cursor` back
    /// on the next call; stop when it returns 0 (full cycle complete).
    fn zscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> HandlerResult;

    // ── Convenience helpers ───────────────────────────────────────────────────

    /// Iterate all members matching `pattern` across as many ZSCAN calls as
    /// needed, collecting everything into a single `Vec`.
    ///
    /// Provided as a default method — implementors get it for free.
    fn zscan_all(&self, key: &str, pattern: Option<&str>) -> HandlerResult;
}

impl SortedSetHandler for CommandHandler {
    fn zadd(
        &self,
        key: &str,
        members: Vec<(OrderedFloat<f64>, String)>,
        options: &ZAddOptions,
    ) -> HandlerResult {
        unimplemented!()
    }

    fn zincrby(&self, key: &str, increment: f64, member: &str) -> HandlerResult {
        unimplemented!()
    }

    fn zrem(&self, key: &str, members: Vec<String>) -> HandlerResult {
        unimplemented!()
    }

    fn zpopmin(&self, key: &str, count: Option<u64>) -> HandlerResult {
        unimplemented!()
    }

    fn zpopmax(&self, key: &str, count: Option<u64>) -> HandlerResult {
        unimplemented!()
    }

    fn bzpopmin(&self, keys: &[&str], timeout: f64) -> HandlerResult {
        unimplemented!()
    }

    fn bzpopmax(&self, keys: &[&str], timeout: f64) -> HandlerResult {
        unimplemented!()
    }

    fn zmpop(&self, keys: &[&str], from_max: bool, count: Option<u64>) -> HandlerResult {
        unimplemented!()
    }

    fn bzmpop(
        &self,
        timeout: f64,
        keys: &[&str],
        from_max: bool,
        count: Option<u64>,
    ) -> HandlerResult {
        unimplemented!()
    }

    fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        rev: bool,
        with_scores: bool,
    ) -> HandlerResult {
        todo!()
    }

    fn zrange_by_score(
        &self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<Limit>,
        with_scores: bool,
    ) -> HandlerResult {
        todo!()
    }

    fn zrange_by_lex(
        &self,
        key: &str,
        min: LexBound,
        max: LexBound,
        rev: bool,
        limit: Option<Limit>,
    ) -> HandlerResult {
        todo!()
    }

    fn zrange_store(
        &self,
        dst: &str,
        src: &str,
        range: RangeBy,
        rev: bool,
        limit: Option<Limit>,
    ) -> HandlerResult {
        todo!()
    }

    fn zrange_store_by_score(
        &self,
        dst: &str,
        src: &str,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<Limit>,
    ) -> HandlerResult {
        todo!()
    }

    fn zrank(&self, key: &str, member: &str, with_score: bool) -> HandlerResult {
        todo!()
    }

    fn zrevrank(&self, key: &str, member: &str, with_score: bool) -> HandlerResult {
        todo!()
    }

    fn zscore(&self, key: &str, member: &str) -> HandlerResult {
        todo!()
    }

    fn zmscore(&self, key: &str, members: &[&str]) -> HandlerResult {
        todo!()
    }

    fn zcard(&self, key: &str) -> HandlerResult {
        todo!()
    }

    fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> HandlerResult {
        todo!()
    }

    fn zlexcount(&self, key: &str, min: LexBound, max: LexBound) -> HandlerResult {
        todo!()
    }

    fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> HandlerResult {
        todo!()
    }

    fn zremrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> HandlerResult {
        todo!()
    }

    fn zremrangebylex(&self, key: &str, min: LexBound, max: LexBound) -> HandlerResult {
        todo!()
    }

    fn zunionstore(
        &self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
    ) -> HandlerResult {
        todo!()
    }

    fn zinterstore(
        &self,
        dst: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
    ) -> HandlerResult {
        todo!()
    }

    fn zdiffstore(&self, dst: &str, keys: &[&str]) -> HandlerResult {
        todo!()
    }

    fn zunion(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> HandlerResult {
        todo!()
    }

    fn zinter(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Aggregate,
        with_scores: bool,
    ) -> HandlerResult {
        todo!()
    }

    fn zdiff(&self, keys: &[&str], with_scores: bool) -> HandlerResult {
        todo!()
    }

    fn zintercard(&self, keys: &[&str], limit: Option<u64>) -> HandlerResult {
        todo!()
    }

    fn zrandmember(&self, key: &str, count: Option<i64>, with_scores: bool) -> HandlerResult {
        todo!()
    }

    fn zscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> HandlerResult {
        todo!()
    }

    fn zscan_all(&self, key: &str, pattern: Option<&str>) -> HandlerResult {
        todo!()
    }
}
