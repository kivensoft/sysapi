//! 记录变化事件推送单元

use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};

use crate::services::rmq::{ChannelName, make_channel, publish};


#[derive(Serialize_repr, Deserialize_repr, PartialEq, Clone, Debug)]
#[repr(u8)]
pub enum RecordChangedType { All, Insert, Update, Delete }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RecChanged<T: Serialize> {
    #[serde(rename = "type")]
    change_type: RecordChangedType,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

pub fn emit<T: Serialize>(chan: ChannelName, value: &RecChanged<T>) {
    let msg = serde_json::to_string(value).expect("json序列化失败");

    tokio::spawn(async move {
        let chan = make_channel(chan);
        if let Err(e) = publish(&chan, &msg).await {
            log::error!("redis发布消息失败: {e:?}");
        }
    });
}

pub fn type_from_id<T>(id: &Option<T>) -> RecordChangedType {
    match id {
        Some(_) => RecordChangedType::Update,
        None => RecordChangedType::Insert,
    }
}

impl <T: Serialize> RecChanged<T> {
    #[allow(dead_code)]
    pub fn new(change_type: RecordChangedType, data: T) -> Self {
        Self { change_type, data: Some(data) }
    }

    #[allow(dead_code)]
    pub fn with_all() -> Self {
        Self { change_type: RecordChangedType::All, data: None }
    }

    #[allow(dead_code)]
    pub fn with_insert(data: T) -> Self {
        Self { change_type: RecordChangedType::Insert, data: Some(data) }
    }

    #[allow(dead_code)]
    pub fn with_update(data: T) -> Self {
        Self { change_type: RecordChangedType::Update, data: Some(data) }
    }

    #[allow(dead_code)]
    pub fn with_delete(data: T) -> Self {
        Self { change_type: RecordChangedType::Delete, data: Some(data) }
    }

}
