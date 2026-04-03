use crate::command::{CommandHandler, StringCommand};
use crate::storage::{SetOptions, Storage};
use crate::value::StringValue;

pub trait StringHandler {
    fn get(&self, key: &str) -> Option<StringValue>;
    fn set(&mut self, key: &str, value: String, opts: Option<SetOptions>) -> bool;
    fn incr(&mut self, key: &str) -> Option<i64>;
    fn incrby(&mut self, key: &str, increment: i64) -> Option<i64>;
    fn decr(&mut self, key: &str) -> Option<i64>;
    fn decrby(&mut self, key: &str, decrement: i64) -> Option<i64>;
    fn mget(&self, keys: Vec<&str>) -> Vec<Option<StringValue>>;
    fn mset(&mut self, pairs: Vec<(String, String)>) -> bool;
    fn getrange(&self, key: &str, start: usize, stop: usize) -> Option<String>;
    fn setrange(&mut self, key: &str, offset: usize, value: String) -> Option<usize>;
    fn append(&mut self, key: &str, value: &str) -> Option<usize>;
    fn strlen(&self, key: &str) -> Option<usize>;
}

impl StringHandler for CommandHandler {
    fn get(&self, key: &str) -> Option<StringValue> {
        self.mem_storage.get(key)
    }

    fn set(&mut self, key: &str, value: String, opts: Option<SetOptions>) -> bool {
        self.mem_storage.set(key, StringValue::Raw(value), opts)
    }

    fn incr(&mut self, key: &str) -> Option<i64> {
        self.mem_storage.incr(key)
    }

    fn incrby(&mut self, key: &str, increment: i64) -> Option<i64> {
        self.mem_storage.incrby(key, increment)
    }

    fn decr(&mut self, key: &str) -> Option<i64> {
        self.mem_storage.decr(key)
    }

    fn decrby(&mut self, key: &str, decrement: i64) -> Option<i64> {
        self.mem_storage.decrby(key, decrement)
    }

    fn mget(&self, keys: Vec<&str>) -> Vec<Option<StringValue>> {
        self.mem_storage.mget(keys)
    }

    fn mset(&mut self, pairs: Vec<(String, String)>) -> bool {
        self.mem_storage.mset(pairs)
    }

    fn getrange(&self, key: &str, start: usize, stop: usize) -> Option<String> {
        self.mem_storage.getrange(key, start, stop)
    }

    fn setrange(&mut self, key: &str, offset: usize, value: String) -> Option<usize> {
        self.mem_storage.setrange(key, offset, value)
    }

    fn append(&mut self, key: &str, value: &str) -> Option<usize> {
        self.mem_storage.append(key, value)
    }

    fn strlen(&self, key: &str) -> Option<usize> {
        self.mem_storage.strlen(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::mem::MemStore;

    use super::*;

    #[test]
    fn test_string_commands() {
        let mut handler = CommandHandler::new(MemStore::new(100));
        assert!(handler.set("key1", "value1".to_string(), None));
        assert_eq!(
            handler.get("key1"),
            Some(StringValue::Raw("value1".to_string()))
        );
        assert_eq!(handler.incr("counter"), Some(1));
        assert_eq!(handler.incrby("counter", 5), Some(6));
        assert_eq!(handler.decr("counter"), Some(5));
        assert_eq!(handler.decrby("counter", 2), Some(3));
        assert_eq!(
            handler.mget(vec!["key1", "counter"]),
            vec![
                Some(StringValue::Raw("value1".to_string())),
                Some(StringValue::Raw("3".to_string()))
            ]
        );
        assert!(handler.mset(vec![
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string())
        ]));
        assert_eq!(handler.getrange("key1", 0, 4), Some("value".to_string()));
        assert_eq!(handler.setrange("key1", 6, "X".to_string()), Some(7));
        assert_eq!(
            handler.get("key1"),
            Some(StringValue::Raw("valueX".to_string()))
        );
        assert_eq!(handler.append("key1", "Y"), Some(7));
        assert_eq!(
            handler.get("key1"),
            Some(StringValue::Raw("valueXY".to_string()))
        );
        assert_eq!(handler.append("new_key", "hello"), Some(5));
        assert_eq!(
            handler.get("new_key"),
            Some(StringValue::Raw("hello".to_string()))
        );
        assert_eq!(handler.strlen("key1"), Some(7));
    }
}
