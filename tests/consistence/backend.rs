use std::collections::HashMap;

use minipaxos::learner::Backend;

pub struct KVDataBase {
    data: HashMap<String, String>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct KVSet {
    pub key: String,
    pub value: String,
}

impl KVSet {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone)]
pub struct KVGet {
    pub key: String,
}

impl KVGet {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
        }
    }
}

impl Backend<KVSet, KVGet> for KVDataBase {
    type Output = Option<String>;

    fn process(&mut self, proposal: KVSet) {
        let KVSet { key, value } = proposal;
        self.data.insert(key, value);
    }

    fn read(&self, request: KVGet) -> Self::Output {
        let KVGet { key } = request;
        self.data.get(&key).cloned()
    }
}

impl KVDataBase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}
