/*
SADD myset "a" "b"          # add members
SREM myset "a"              # remove member
SMEMBERS myset              # get all members
SCARD myset                 # count members
SISMEMBER myset "a"         # membership check → 0 or 1
SMISMEMBER myset "a" "b"    # check multiple → [1, 1]
SPOP myset                  # remove & return random
SRANDMEMBER myset 3         # random sample, no removal
SMOVE src dst "a"           # atomic move between sets
SUNION s1 s2                # all members from both
SINTER s1 s2                # only shared members
SDIFF  s1 s2                # in s1 but not s2
SUNIONSTORE dst s1 s2       # union → stored in dst
SINTERSTORE dst s1 s2       # intersection → stored in dst
SDIFFSTORE  dst s1 s2       # difference → stored in dst
SINTERCARD 2 s1 s2          # count of intersection (7.0+)
SSCAN myset 0 MATCH "u:*"   # cursor-based scan
*/
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCommand {
    SAdd(String, Vec<String>),          // sadd key member1 member2 ...
    SRem(String, Vec<String>),          // srem key member1 member2 ...
    SMembers(String),                   // smembers key
    SCard(String),                      // count members
    SIsMember(String, String),          // sismember key member
    SMIsMember(String, Vec<String>),    // smismember key member1 member2 ...
    SPop(String),                       // spop key
    SRandMember(String, Option<usize>), // srandmember key [count]
    SMove(String, String, String),      // smove src dst member
    SUnion(Vec<String>),                // sunion s1 s2 ...
    SInter(Vec<String>),                // sinter s1 s2 ...
    SDiff(Vec<String>),                 // sdiff s1 s2 ...
    SUnionStore(String, Vec<String>),   // sunionstore dst s1 s2 ...
    SInterStore(String, Vec<String>),   // sinterstore dst s1 s2 ...
    SDiffStore(String, Vec<String>),    // sdiffstore dst s1 s2 ...
    SInterCard(usize, Vec<String>, Option<usize>), // sintercard numkeys s1 s2 ... [LIMIT count]
    SScan(String, usize, Option<String>), // sscan key cursor [match pattern]
}
