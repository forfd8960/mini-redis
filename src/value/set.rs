use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetValue {
    pub items: HashSet<String>,
}

impl SetValue {
    pub fn new() -> Self {
        SetValue {
            items: HashSet::new(),
        }
    }

    pub fn from_vec(members: Vec<&str>) -> Self {
        SetValue {
            items: HashSet::from_iter(members.iter().map(|&s| s.to_string())),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn members(&self) -> Vec<String> {
        self.items.iter().cloned().collect()
    }

    pub fn rand_member(&self, count: Option<usize>) -> Vec<String> {
        let count = count.unwrap_or(1);
        self.items.iter().take(count).cloned().collect()
    }

    pub fn is_member(&self, member: &str) -> bool {
        self.items.contains(member)
    }

    pub fn sm_ismember(&self, members: Vec<&str>) -> Vec<i64> {
        members
            .iter()
            .map(|&m| if self.items.contains(m) { 1 } else { 0 })
            .collect()
    }

    pub fn sadd(&mut self, members: Vec<&str>) -> usize {
        let mut added = 0;
        for member in members {
            if self.items.insert(member.to_string()) {
                added += 1;
            }
        }
        added
    }

    pub fn srem(&mut self, members: Vec<&str>) -> usize {
        let mut removed = 0;
        for member in members {
            if self.items.remove(member) {
                removed += 1;
            }
        }
        removed
    }

    pub fn spop(&mut self, count: Option<usize>) -> Option<Vec<String>> {
        let count = count.unwrap_or(1);
        if self.items.is_empty() {
            return None;
        }

        let mut popped = Vec::new();
        for _ in 0..count {
            if let Some(member) = self.items.iter().next().cloned() {
                self.items.remove(&member);
                popped.push(member);
            } else {
                break;
            }
        }
        Some(popped)
    }

    pub fn smove(&mut self, dst: &mut SetValue, member: &str) -> bool {
        if self.items.remove(member) {
            dst.items.insert(member.to_string());
            true
        } else {
            false
        }
    }

    pub fn sunion(sets: Vec<Vec<String>>) -> Vec<String> {
        let mut union_set = HashSet::new();
        for set in sets {
            union_set.extend(set.iter().cloned());
        }
        union_set.into_iter().collect()
    }

    pub fn sinter(sets: Vec<Vec<String>>) -> Vec<String> {
        if sets.is_empty() {
            return Vec::new();
        }

        let mut intersection_set: HashSet<String> = sets[0].iter().cloned().collect();
        for set in sets.iter().skip(1) {
            let current_set: HashSet<String> = set.iter().cloned().collect();
            intersection_set = intersection_set
                .intersection(&current_set)
                .cloned()
                .collect();
        }
        intersection_set.into_iter().collect()
    }

    pub fn sdiff(sets: Vec<Vec<String>>) -> Vec<String> {
        if sets.is_empty() {
            return Vec::new();
        }

        let mut difference_set: HashSet<String> = sets[0].iter().cloned().collect();
        for set in sets.iter().skip(1) {
            let current_set: HashSet<String> = set.iter().cloned().collect();
            difference_set = difference_set.difference(&current_set).cloned().collect();
        }
        difference_set.into_iter().collect()
    }
}
