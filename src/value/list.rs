use std::collections::VecDeque;

use crate::{
    errors::RedisError,
    value::{ListInsertPivot, ListMoveDirection},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListValue {
    pub items: VecDeque<String>,
}

impl ListValue {
    pub fn new(cap: usize) -> Self {
        ListValue {
            items: VecDeque::with_capacity(cap),
        }
    }

    pub fn from(values: Vec<String>) -> Self {
        ListValue {
            items: VecDeque::from(values),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn lrem(&mut self, count: i64, value: &str) -> usize {
        let mut removed = 0;
        if count == 0 {
            self.items.retain(|v| {
                if v == value {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        } else if count > 0 {
            let mut to_remove = count as usize;
            self.items.retain(|v| {
                if v == value && to_remove > 0 {
                    removed += 1;
                    to_remove -= 1;
                    false
                } else {
                    true
                }
            });
        } else {
            let mut to_remove = (-count) as usize;
            let mut rev_items = self.items.iter().rev().cloned().collect::<Vec<_>>();
            rev_items.retain(|v| {
                if v == value && to_remove > 0 {
                    removed += 1;
                    to_remove -= 1;
                    false
                } else {
                    true
                }
            });

            let mut new_queue = VecDeque::with_capacity(rev_items.len());
            for item in rev_items.into_iter().rev() {
                new_queue.push_back(item);
            }
            self.items = new_queue;
        }
        removed
    }

    pub fn lset(&mut self, index: i64, value: &str) -> Result<(), RedisError> {
        let idx = if index >= 0 {
            index as usize
        } else {
            let abs_index = (-index) as usize;
            if abs_index > self.items.len() {
                return Err(RedisError::StorageError(format!(
                    "Index out of range: {}",
                    index
                )));
            }
            self.items.len() - abs_index
        };

        if idx >= self.items.len() {
            return Err(RedisError::StorageError(format!(
                "Index out of range: {}",
                index
            )));
        }

        self.items[idx] = value.to_string();
        Ok(())
    }

    pub fn linsert(
        &mut self,
        position: ListInsertPivot,
        pivot: &str,
        value: &str,
    ) -> Result<bool, RedisError> {
        match position {
            ListInsertPivot::Before => {
                let original_len = self.items.len();
                insert_before(&mut self.items, &pivot.to_string(), value.to_string());
                Ok(self.items.len() > original_len) // Return true if an insertion occurred
            }
            ListInsertPivot::After => {
                let original_len = self.items.len();
                insert_after(&mut self.items, &pivot.to_string(), value.to_string());
                Ok(self.items.len() > original_len) // Return true if an insertion occurred
            }
        }
    }

    pub fn ltrim(&mut self, start: i64, stop: i64) -> Result<bool, RedisError> {
        let len = self.items.len() as i64;
        let start = if start >= 0 { start } else { len + start };
        let stop = if stop >= 0 { stop } else { len + stop };

        if start < 0 || stop < 0 || start >= len || stop >= len {
            return Err(RedisError::StorageError(format!(
                "Index out of range: start={}, stop={}, list length={}",
                start, stop, len
            )));
        }

        keep_range(&mut self.items, start as usize, (stop + 1) as usize);
        Ok(true)
    }

    pub fn lmove(
        &mut self,
        dest: &mut ListValue,
        src_side: ListMoveDirection,
        dest_side: ListMoveDirection,
    ) -> Result<Option<String>, RedisError> {
        let move_item = match src_side {
            ListMoveDirection::Left => self.items.pop_front(),
            ListMoveDirection::Right => self.items.pop_back(),
        };

        if move_item.is_none() {
            return Ok(None);
        }

        let target_item = move_item.unwrap();

        match dest_side {
            ListMoveDirection::Left => dest.items.push_front(target_item.clone()),
            ListMoveDirection::Right => dest.items.push_back(target_item.clone()),
        }

        Ok(Some(target_item))
    }
}

pub fn insert_before<T: PartialEq>(deque: &mut VecDeque<T>, target: &T, value: T) {
    if let Some(pos) = deque.iter().position(|x| x == target) {
        deque.insert(pos, value); // insert at the target's index (shifts target right)
    }
}

pub fn insert_after<T: PartialEq>(deque: &mut VecDeque<T>, target: &T, value: T) {
    if let Some(pos) = deque.iter().position(|x| x == target) {
        deque.insert(pos + 1, value); // insert after the target's index
    }
}

fn keep_range<T: PartialEq>(deque: &mut VecDeque<T>, start: usize, stop: usize) {
    // Remove elements after `stop` first (to preserve indices)
    deque.drain(stop..);
    // Then remove elements before `start`
    deque.drain(..start);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn test_insert_before() {
        let mut deque = VecDeque::from(vec!["a", "b", "c"]);
        insert_before(&mut deque, &"b", "x");
        assert_eq!(deque, VecDeque::from(vec!["a", "x", "b", "c"]));
    }

    #[test]
    fn test_insert_after() {
        let mut deque = VecDeque::from(vec!["a", "b", "c"]);
        insert_after(&mut deque, &"b", "y");
        println!("after insert y {:?}", deque);
        assert_eq!(deque, VecDeque::from(vec!["a", "b", "y", "c"]));
    }

    fn keep_range<T: PartialEq>(deque: &mut VecDeque<T>, start: usize, stop: usize) {
        // Remove elements after `stop` first (to preserve indices)
        deque.drain(stop..);
        // Then remove elements before `start`
        deque.drain(..start);
    }

    fn trim_items<T: PartialEq>(items: &mut VecDeque<T>, start: i64, stop: i64) {
        let len = items.len() as i64;
        let start = if start >= 0 { start } else { len + start };
        let stop = if stop >= 0 { stop } else { len + stop };
        keep_range(items, start as usize, (stop + 1) as usize); // stop is inclusive, so add 1
    }

    #[test]
    fn test_trim_items() {
        let mut items = VecDeque::from(
            "a b c d e"
                .split_whitespace()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
        trim_items(&mut items, 1, 2);
        println!("after ltrim 1 .. 2 {:?}", items);
        assert_eq!(
            items,
            VecDeque::from(vec!["b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn test_ltrim() {
        let mut list = ListValue::new(10);
        list.items
            .extend(vec!["a".to_string(), "b".to_string(), "c".to_string()]);

        assert!(list.ltrim(0, 1).is_ok());
        assert_eq!(
            list.items,
            VecDeque::from(vec!["a".to_string(), "b".to_string()])
        );

        list.items
            .extend(vec!["c".to_string(), "d".to_string(), "e".to_string()]);

        assert!(list.ltrim(-3, -1).is_ok());
        assert_eq!(
            list.items,
            VecDeque::from(
                "c d e"
                    .split_whitespace()
                    .map(String::from)
                    .collect::<Vec<_>>()
            )
        );
    }

    #[test]
    fn test_lmove() {
        let mut src_list = ListValue::from(
            "a b c"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let mut dest_list = ListValue::from(
            "D E F"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let res = src_list.lmove(
            &mut dest_list,
            ListMoveDirection::Left,
            ListMoveDirection::Right,
        );

        println!("src_list: {:?}", src_list);
        println!("dest_list: {:?}", dest_list);

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Some("a".to_string()));
        assert_eq!(src_list.items, VecDeque::from("b c".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
        assert_eq!(dest_list.items, VecDeque::from("D E F a".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
    }

    #[test]
    fn test_lmove1() {
        let mut src_list = ListValue::from(
            "a b c"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let mut dest_list = ListValue::from(
            "D E F"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let res = src_list.lmove(
            &mut dest_list,
            ListMoveDirection::Right,
            ListMoveDirection::Left,
        );

        println!("src_list: {:?}", src_list);
        println!("dest_list: {:?}", dest_list);

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Some("c".to_string()));
        assert_eq!(src_list.items, VecDeque::from("a b".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
        assert_eq!(dest_list.items, VecDeque::from("c D E F".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
    }

    #[test]
    fn test_lmove2() {
        let mut src_list = ListValue::from(
            "a b c"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let mut dest_list = ListValue::from(
            "D E F"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let res = src_list.lmove(
            &mut dest_list,
            ListMoveDirection::Left,
            ListMoveDirection::Left,
        );

        println!("src_list: {:?}", src_list);
        println!("dest_list: {:?}", dest_list);

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Some("a".to_string()));
        assert_eq!(src_list.items, VecDeque::from("b c".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
        assert_eq!(dest_list.items, VecDeque::from("a D E F".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
    }

    #[test]
    fn test_list_lmove_no_item() {
        let mut src_list = ListValue::from(vec![]);
        let mut dest_list = ListValue::from(
            "D E F"
                .split_whitespace()
                .map(|v| String::from(v))
                .collect::<Vec<String>>(),
        );

        let res = src_list.lmove(
            &mut dest_list,
            ListMoveDirection::Left,
            ListMoveDirection::Left,
        );

        println!("src_list: {:?}", src_list);
        println!("dest_list: {:?}", dest_list);

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), None);
        assert_eq!(src_list.items, VecDeque::from(vec![]));
        assert_eq!(dest_list.items, VecDeque::from("D E F".split_whitespace().map(|v| v.to_string()).collect::<Vec<String>>()));
    }
}
