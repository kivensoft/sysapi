//! 记录变化事件推送单元
use httpserver::{log_error, log_trace};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::services::rmq::{make_channel, publish, ChannelName};

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Clone, Debug)]
#[repr(u8)]
pub enum RecordChangedType {
    All,
    Insert,
    Update,
    Delete,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RecChanged<T: Serialize> {
    #[serde(rename = "type")]
    change_type: RecordChangedType,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

pub fn emit<T: Serialize>(req_id: u32, chan: ChannelName, value: &RecChanged<T>) {
    let msg = serde_json::to_string(value).expect("json序列化失败");

    tokio::spawn(async move {
        let chan = make_channel(chan);
        match publish(&chan, &msg).await {
            Ok(_) => log_trace!(req_id, "redis发布消息成功: chan = {}, msg = {}", chan, msg),
            Err(e) => log_error!(
                req_id,
                "redis发布消息失败: chan = {}, msg = {}, err = {:?}",
                chan,
                msg,
                e
            ),
        }
    });
}

pub fn type_from_id<T>(id: &Option<T>) -> RecordChangedType {
    match id {
        Some(_) => RecordChangedType::Update,
        None => RecordChangedType::Insert,
    }
}

impl RecChanged<()> {
    #[allow(dead_code)]
    pub fn with_all() -> Self {
        Self {
            change_type: RecordChangedType::All,
            data: None,
        }
    }
}

impl<T: Serialize> RecChanged<T> {
    #[allow(dead_code)]
    pub fn new(change_type: RecordChangedType, data: T) -> Self {
        Self {
            change_type,
            data: Some(data),
        }
    }

    #[allow(dead_code)]
    pub fn with_insert(data: T) -> Self {
        Self {
            change_type: RecordChangedType::Insert,
            data: Some(data),
        }
    }

    #[allow(dead_code)]
    pub fn with_update(data: T) -> Self {
        Self {
            change_type: RecordChangedType::Update,
            data: Some(data),
        }
    }

    #[allow(dead_code)]
    pub fn with_delete(data: T) -> Self {
        Self {
            change_type: RecordChangedType::Delete,
            data: Some(data),
        }
    }
}
