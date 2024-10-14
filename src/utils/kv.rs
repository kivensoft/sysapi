//! 键值结构定义
//!
//! Author: kiven lee
//! Date: 2024-05-08

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

pub type IntStr = KeyValue<i64, String>;
// pub type StrStr = KeyValue<String, String>;

impl<K, V> KeyValue<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}
