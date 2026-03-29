use ordered_float::OrderedFloat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Basic(BasicCommand),
    String(StringCommand),
    List(ListCommand),
    Hash(HashCommand),
    Set(SetCommand),
    SortedSet(SortedSetCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BasicCommand {
    Ping,
    Echo(String),
    Exists(String),
    TTL(String),
    Expire(String, u64),
    // scan cursor [MATCH pattern] [COUNT count] [TYPE type]
    Scan(
        Option<String>,
        Option<String>,
        Option<usize>,
        Option<String>,
    ),
    Keys(String), // keys pattern
    Del(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StringCommand {
    Get(String),
    /// https://redis.io/docs/latest/commands/set/
    /// set key value [EX seconds] [PX milliseconds] [EXAT timestamp-seconds]
    ///     [PXAT timestamp-milliseconds] [KEEPTTL] [NX|XX] [GET]
    Set(String, String, SetOptions),
    Incr(String),
    IncrBy(String, i64),
    Decr(String),
    DecrBy(String, i64),
    Mget(Vec<String>),               // mget key1 key2 ...
    Mset(Vec<(String, String)>),     // mset key1 value1 key2 value2 ...
    GetRange(String, usize, usize),  // getrange key start end
    SetRange(String, usize, String), // setrange key offset value
    Append(String, String),          // append key value
    StrLen(String),                  // strlen key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOptions {
    pub ttl: Option<SetTTL>,
    pub condition: Option<SetCondition>,
    pub get: bool, // whether to return the old value
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetTTL {
    EX(u64),   // expire time in seconds
    PX(u64),   // expire time in milliseconds
    EXAT(u64), // expire time as Unix timestamp in seconds
    PXAT(u64), // expire time as Unix timestamp in milliseconds
    KeepTTL,   // keep the existing TTL,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCondition {
    NX, // Only set the key if it does not already exist.
    XX, // Only set the key if it already exists.
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListCommand {
    Lpush(String, Vec<String>),   // lpush key value1 value2 ...
    Rpush(String, Vec<String>),   // rpush key value1 value2 ...
    Lpop(String, usize),          // lpop key count
    Rpop(String, usize),          // rpop key count
    Lrange(String, usize, usize), // lrange key start stop
    Llen(String),                 // llen key
    Lrem(String, String, usize),  // lrem key value count
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashCommand {
    Hset(String, Vec<(String, String)>), // hset key field1 value1 field2 value2 ...
    Hget(String, String),                // hget key field
    Hmget(String, Vec<String>),          // hmget key field1 field2 ...
    Hgetall(String),                     // hgetall key
    Hincrby { key: String, field: String, increment: i64 }, // hincrby key field increment
    Hdel { key: String, fields: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCommand {
    Sadd(String, Vec<String>), // sadd key member1 member2 ...
    Srem(String, Vec<String>), // srem key member1 member2 ...
    Smembers(String),          // smembers key
    Sismember(String, String), // sismember key member
    Scard(String),             // scard key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortedSetCommand {
    Zadd(String, Vec<(String, OrderedFloat<f64>)>), // zadd key score1 member1 score2 member2 ...
    Zrem(String, Vec<String>),                      // zrem key member1 member2 ...
    Zrange(String, usize, usize),                   // zrange key start stop
    ZrangeWithScores(String, usize, usize),         // zrange key start stop withscores
    Zrank(String, String),                          // zrank key member
    Zscore(String, String),                         // zscore key member
}
