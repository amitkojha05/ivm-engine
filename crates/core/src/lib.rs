use std::collections::HashMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// A Z-set: map from T → weight (integer).
/// Positive weight = inserted rows, negative = deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZSet<T: Eq + std::hash::Hash> {
    pub inner: IndexMap<T, i64>,
}

impl<T: Eq + std::hash::Hash> Default for ZSet<T> {
    fn default() -> Self {
        Self {
            inner: IndexMap::new(),
        }
    }
}

impl<T: Eq + std::hash::Hash> ZSet<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, row: T, weight: i64) {
        use indexmap::map::Entry;
        match self.inner.entry(row) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += weight;
                if *e.get() == 0 {
                    e.shift_remove();
                }
            }
            Entry::Vacant(e) => {
                if weight != 0 {
                    e.insert(weight);
                }
            }
        }
    }

    /// Merge another Z-set into self (additive).
    pub fn merge(&mut self, other: ZSet<T>) {
        for (row, weight) in other.inner {
            self.insert(row, weight);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Optional event-time watermark for a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub event_time_ms: u64,
    pub source_id: String,
}

/// A timestamped batch of Z-set changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<T: Eq + std::hash::Hash> {
    pub epoch: u64,
    pub delta: ZSet<T>,
    pub watermark: Option<Watermark>,
}

impl<T: Eq + std::hash::Hash> Batch<T> {
    pub fn new(epoch: u64, delta: ZSet<T>) -> Self {
        Self {
            epoch,
            delta,
            watermark: None,
        }
    }

    pub fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            delta: ZSet::default(),
            watermark: None,
        }
    }

    pub fn with_watermark(mut self, wm: Watermark) -> Self {
        self.watermark = Some(wm);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row(pub HashMap<String, Value>);

impl std::hash::Hash for Row {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut pairs: Vec<_> = self.0.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in pairs {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl Row {
    pub fn new(fields: HashMap<String, Value>) -> Self {
        Self(fields)
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn get_int(&self, key: &str) -> i64 {
        match self.0.get(key) {
            Some(Value::Int(v)) => *v,
            _ => 0,
        }
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.0.get(key) {
            Some(Value::Str(s)) => Some(s.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zset_insert_and_prune() {
        let mut z = ZSet::new();
        z.insert("a".to_string(), 1);
        assert_eq!(z.len(), 1);
        z.insert("a".to_string(), -1);
        assert!(z.is_empty());
    }

    #[test]
    fn zset_merge() {
        let mut a = ZSet::new();
        a.insert("x".to_string(), 2);
        let mut b = ZSet::new();
        b.insert("x".to_string(), -1);
        b.insert("y".to_string(), 1);
        a.merge(b);
        assert_eq!(a.inner.get("x"), Some(&1));
        assert_eq!(a.inner.get("y"), Some(&1));
    }

    #[test]
    fn batch_roundtrip() {
        let mut delta = ZSet::new();
        let mut row = HashMap::new();
        row.insert("id".into(), Value::Int(1));
        delta.insert(Row(row), 1);
        let batch = Batch::new(42, delta);
        assert_eq!(batch.epoch, 42);
        assert_eq!(batch.delta.len(), 1);
        assert!(batch.watermark.is_none());
    }
}
