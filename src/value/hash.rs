use std::collections::HashMap;

use ordered_float::OrderedFloat;

use crate::value::HashEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashValue {
    pub items: HashMap<String, String>,
}

impl HashValue {
    pub fn new() -> Self {
        HashValue {
            items: HashMap::new(),
        }
    }

    pub fn from(entries: Vec<(String, String)>) -> Self {
        let mut items = HashMap::new();
        for (key, value) in entries {
            items.insert(key, value);
        }
        HashValue { items }
    }

    pub fn hget(&self, field: &str) -> Option<HashEntry> {
        self.items
            .get(field)
            .map(|value| (field.to_string(), value.clone()))
    }

    pub fn hmget(&self, fields: Vec<&str>) -> Vec<HashEntry> {
        fields
            .into_iter()
            .filter_map(|field| self.hget(field))
            .collect()
    }

    pub fn hmset(&mut self, field_values: Vec<HashEntry>) -> bool {
        let mut updated = false;
        for (field, value) in field_values {
            if self.items.insert(field, value).is_none() {
                updated = true;
            }
        }
        updated
    }

    pub fn hgetall(&self) -> Vec<HashEntry> {
        self.items
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn hkeys(&self) -> Vec<String> {
        self.items.keys().cloned().collect()
    }

    pub fn hvals(&self) -> Vec<String> {
        self.items.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn hexists(&self, field: &str) -> bool {
        self.items.contains_key(field)
    }

    pub fn hset(&mut self, entries: Vec<(String, String)>) -> bool {
        let mut updated = false;
        for (field, value) in entries {
            if self.items.insert(field, value).is_none() {
                updated = true;
            }
        }
        updated
    }

    pub fn hsetnx(&mut self, key: &str, value: &str) -> bool {
        if self.items.contains_key(key) {
            false
        } else {
            self.items.insert(key.to_string(), value.to_string());
            true
        }
    }

    pub fn hincrby(&mut self, field: &str, increment: i64) -> Option<i64> {
        let current_value = self.items.get(field)?.parse::<i64>().ok()?;
        let new_value = current_value + increment;
        self.items.insert(field.to_string(), new_value.to_string());
        Some(new_value)
    }

    pub fn hincrbyfloat(&mut self, field: &str, increment: OrderedFloat<f64>) -> Option<f64> {
        let current_value = self.items.get(field)?.parse::<f64>().ok()?;
        let new_value = current_value + increment.into_inner();
        self.items.insert(field.to_string(), new_value.to_string());
        Some(new_value)
    }

    pub fn hdel(&mut self, fields: &[String]) -> usize {
        let mut deleted_count = 0;
        for field in fields {
            if self.items.remove(field).is_some() {
                deleted_count += 1;
            }
        }
        deleted_count
    }
}
