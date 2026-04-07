use crate::{
    storage::mem::MemStore,
    value::{RedisValue, set::SetValue},
};

/*
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCommand {
    SAdd(String, Vec<String>),            // sadd key member1 member2 ...
    SRem(String, Vec<String>),            // srem key member1 member2 ...
    SMembers(String),                     // smembers key
    SCard(String),                        // count members
    SIsMember(String, String),            // sismember key member
    SMIsMember(String, Vec<String>),      // smismember key member1 member2 ...
    SPop(String),                         // spop key
    SRandMember(String, Option<usize>),   // srandmember key [count]
    SMove(String, String, String),        // smove src dst member
    SUnion(Vec<String>),                  // sunion s1 s2 ...
    SInter(Vec<String>),                  // sinter s1 s2 ...
    SDiff(Vec<String>),                   // sdiff s1 s2 ...
    SUnionStore(String, Vec<String>),     // sunionstore dst s1 s2 ...
    SInterStore(String, Vec<String>),     // sinterstore dst s1 s2 ...
    SDiffStore(String, Vec<String>),      // sdiffstore dst s1 s2 ...
    SInterCard(Vec<String>, usize),       // sintercard numkeys s1 s2 ...
    SScan(String, usize, Option<String>), // sscan key cursor [match pattern]
}
*/
pub trait SetStore {
    fn sadd(&mut self, key: &str, members: Vec<&str>) -> usize;
    fn srem(&mut self, key: &str, members: Vec<&str>) -> usize;
    fn smembers(&self, key: &str) -> Option<Vec<String>>;
    fn scard(&self, key: &str) -> usize;
    fn sismember(&self, key: &str, member: &str) -> bool;
    fn spop(&mut self, key: &str, count: Option<usize>) -> Option<Vec<String>>;
    fn srandmember(&self, key: &str, count: Option<usize>) -> Option<Vec<String>>;
    fn smove(&mut self, src: &str, dst: &str, member: &str) -> bool;
    fn sunion(&self, keys: Vec<&str>) -> Option<Vec<String>>;
    fn sinter(&self, keys: Vec<&str>) -> Option<Vec<String>>;
    fn sdiff(&self, keys: Vec<&str>) -> Option<Vec<String>>;
    fn sunionstore(&mut self, dst: &str, keys: Vec<&str>) -> usize;
    fn sinterstore(&mut self, dst: &str, keys: Vec<&str>) -> usize;
    fn sdiffstore(&mut self, dst: &str, keys: Vec<&str>) -> usize;
    fn sintercard(&self, keys: Vec<&str>, limit: Option<usize>) -> usize;
    fn sscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<&str>,
    ) -> Option<(usize, Vec<String>)>;
}

impl SetStore for MemStore {
    fn sadd(&mut self, key: &str, members: Vec<&str>) -> usize {
        let len = members.len();
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Set(h) = &mut *v {
                return h.sadd(members);
            }
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::Set(SetValue::from_vec(members)),
            );
            return len;
        }

        0
    }

    fn srem(&mut self, key: &str, members: Vec<&str>) -> usize {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Set(set) = &mut *v {
                return set.srem(members);
            }
        }
        0
    }

    fn smembers(&self, key: &str) -> Option<Vec<String>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Set(set) = &*v {
                return Some(set.members());
            }
        }
        None
    }

    fn scard(&self, key: &str) -> usize {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Set(set) = &*v {
                return set.len();
            }
        }
        0
    }

    fn sismember(&self, key: &str, member: &str) -> bool {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Set(set) = &*v {
                return set.is_member(member);
            }
        }
        false
    }

    fn spop(&mut self, key: &str, count: Option<usize>) -> Option<Vec<String>> {
        if let Some(mut v) = self.data.get_mut(key) {
            if let RedisValue::Set(set) = &mut *v {
                return set.spop(count);
            }
        }
        None
    }

    fn srandmember(&self, key: &str, count: Option<usize>) -> Option<Vec<String>> {
        if let Some(v) = self.data.get(key) {
            if let RedisValue::Set(set) = &*v {
                return Some(set.rand_member(count));
            }
        }
        None
    }

    fn smove(&mut self, src: &str, dst: &str, member: &str) -> bool {
        if let Some(mut src_set) = self.data.get_mut(src) {
            if let RedisValue::Set(src_set) = &mut *src_set {
                if let Some(mut dst_set) = self.data.get_mut(dst) {
                    if let RedisValue::Set(dst_set) = &mut *dst_set {
                        return src_set.smove(dst_set, member);
                    }
                } else {
                    let mut dst_set = SetValue::new();
                    if src_set.smove(&mut dst_set, member) {
                        self.data.insert(dst.to_string(), RedisValue::Set(dst_set));
                        return true;
                    }
                }
            }
        }
        false
    }

    fn sunion(&self, keys: Vec<&str>) -> Option<Vec<String>> {
        let all_members = self.all_set_members(keys);
        Some(SetValue::sunion(all_members))
    }

    fn sinter(&self, keys: Vec<&str>) -> Option<Vec<String>> {
        let all_members = self.all_set_members(keys);
        Some(SetValue::sinter(all_members))
    }

    fn sdiff(&self, keys: Vec<&str>) -> Option<Vec<String>> {
        let all_members = self.all_set_members(keys);
        Some(SetValue::sdiff(all_members))
    }

    fn sunionstore(&mut self, dst: &str, keys: Vec<&str>) -> usize {
        let all_members = self.all_set_members(keys);

        let union_members = SetValue::sunion(all_members);
        self.sadd(dst, union_members.iter().map(|s| s.as_str()).collect())
    }

    fn sinterstore(&mut self, dst: &str, keys: Vec<&str>) -> usize {
        let all_members = self.all_set_members(keys);

        let inter_members = SetValue::sinter(all_members);
        self.sadd(dst, inter_members.iter().map(|s| s.as_str()).collect())
    }

    fn sdiffstore(&mut self, dst: &str, keys: Vec<&str>) -> usize {
        let all_members = self.all_set_members(keys);

        let diff_members = SetValue::sdiff(all_members);
        self.sadd(dst, diff_members.iter().map(|s| s.as_str()).collect())
    }

    fn sintercard(&self, keys: Vec<&str>, _limit: Option<usize>) -> usize {
        // count the size of the intersection without actually computing the intersection set
        let all_members = self.all_set_members(keys);
        let inter_members = SetValue::sinter(all_members);
        inter_members.len()
    }

    fn sscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<&str>,
    ) -> Option<(usize, Vec<String>)> {
        unimplemented!()
    }
}
