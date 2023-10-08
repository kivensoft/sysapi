//! FastStr implement
// author: kiven
// slince 2023-09-29

use std::{fmt::{Display, Formatter}, ops::{Deref, DerefMut}, str::FromStr, convert::Infallible};
use compact_str::CompactString;
use serde::{Serialize, Deserialize, Deserializer, de::Visitor};
use super::{ConvIr, FromValueError, Value, FromValue};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord)]
pub struct FastStr(CompactString);

impl FastStr {
    #[inline]
    pub fn new(value: &str) -> Self {
        Self(CompactString::new(value))
    }

    pub fn as_compact_string(&self) -> &CompactString {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for FastStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for FastStr {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for FastStr {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsMut<CompactString> for FastStr {
    fn as_mut(&mut self) -> &mut CompactString {
        &mut self.0
    }
}

impl Deref for FastStr {
    type Target = CompactString;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FastStr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<&str> for FastStr {
    fn from(value: &str) -> Self {
        Self(CompactString::new(value))
    }
}

impl FromStr for FastStr {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(CompactString::new(s)))
    }
}

impl Serialize for FastStr {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        serializer.serialize_str(&self.0)
    }
}

impl <'de> Deserialize<'de> for FastStr {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> {
        // deserializer.deserialize_str(LocalTimeVisitor) // 为 Deserializer 提供 Visitor
        deserializer.deserialize_str(BetterStrVisitor)
    }
}

struct BetterStrVisitor; // LocalDateTime 的 Visitor，用来反序列化

impl <'de> Visitor<'de> for BetterStrVisitor {
    type Value = FastStr; // Visitor 的类型参数，这里我们需要反序列化的最终目标是 LocalDateTime

    // 必须重写的函数，用于为预期之外的类型提供错误信息
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("str must be utf8")
    }

    // 从字符串中反序列化
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(FastStr(CompactString::new(v)))
    }
}

impl ConvIr<FastStr> for Vec<u8> {
    fn new(v: Value) -> Result<Vec<u8>, FromValueError> {
        match v {
            Value::Bytes(bytes) => match CompactString::from_utf8(&*bytes) {
                Ok(_) => Ok(bytes),
                Err(_) => Err(FromValueError(Value::Bytes(bytes))),
            },
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> FastStr {
        unsafe { FastStr(CompactString::from_utf8_unchecked(self)) }
    }
    fn rollback(self) -> Value {
        Value::Bytes(self)
    }
}

impl FromValue for FastStr {
    type Intermediate = Vec<u8>;
}

impl From<FastStr> for Value {
    fn from(value: FastStr) -> Self {
        Value::Bytes(Vec::from(value.0.as_bytes()))
    }
}
