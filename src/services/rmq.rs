//! redis 消息队列服务
use std::collections::HashMap;

use anyhow::{Context, Result};
use compact_str::{CompactString, format_compact};
use deadpool_redis::redis::{aio::PubSub, Msg, Client, Cmd};
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};
use tokio::sync::{Mutex, mpsc, OnceCell, MutexGuard};

use crate::AppConf;

use super::rcache;

macro_rules! get_global_value {
    ($e: expr) => {
        unsafe {
            debug_assert!($e.is_some());
            match $e.as_mut() {
                Some(val) => val,
                None => std::hint::unreachable_unchecked(),
            }
        }
    };
}

pub enum ChannelName {
    ModApi,
    ModConfig,
    ModDict,
    ModPermission,
    ModRole,
    ModUser,
    ModMenu,
    Login,
    Logout,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Clone, Debug)]
#[repr(u8)]
pub enum RecordChangedType {
    All,
    Insert,
    Update,
    Delete,
}

#[async_trait::async_trait]
pub trait RedisOnMessage: Send + Sync + 'static {
    async fn handle(&self, msg: Msg) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RecChanged<T: Serialize> {
    #[serde(rename = "type")]
    change_type: RecordChangedType,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

/// 订阅消息, 当末尾为'*'时, 表示通配符模式订阅. 相同的频道只能被订阅1次,
/// 当频道存在时, 订阅失败
///
/// Arguments:
///
/// * `channel`: 订阅的频道, "abc" 可接收来自 "abc" 的消息, 但不能接收 "abcd" 的消息,
///     "abc*" 表示可接收 "abc", "abcd", "abcde" 的消息
/// * `on_msg`: 异步回调处理函数
///
/// Returns:
///
/// * `Ok(true)`: 订阅成功
/// * `Ok(false)`: 订阅失败, 该频道已被订阅
/// * `Err(e)`: 其它错误
#[allow(dead_code)]
pub async fn subscribe(channel: &str, msg_func: impl RedisOnMessage) -> Result<bool> {
    debug_assert!(!channel.is_empty());

    SUB_DATA_INIT.get_or_init(|| async {
        init_sub_data().await.expect("初始化redis消息订阅全局对象失败");
        true
    }).await;

    let msg_map = get_global_value!(MSG_MAP);
    let mut msg_map = msg_map.lock().await;

    // 区分具体频道订阅还是匹配模式订阅进行分别处理
    let cs = channel.as_bytes();
    let mut modified = false;
    if cs[cs.len() - 1] == b'*' {
        msg_map.pfuncs.entry(CompactString::new(channel))
            .or_insert_with(|| {
                modified = true;
                Box::new(msg_func)
            });
    } else {
        msg_map.funcs.entry(CompactString::new(channel))
            .or_insert_with(|| {
                modified = true;
                Box::new(msg_func)
            });
    }

    // 需要订阅的频道尚未有人订阅, 可以订阅
    if modified {
        msg_map.modified = true;
        get_global_value!(SUB_DATA_TX).send(false)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// 取消订阅
///
/// Arguments:
///
/// * `channel`: 取消订阅的频道
///
#[allow(dead_code)]
pub async fn unsubscribe(channel: &str) -> Result<()> {
    debug_assert!(!channel.is_empty());

    SUB_DATA_INIT.get_or_init(|| async {
        init_sub_data().await.expect("初始化redis消息订阅全局对象失败");
        true
    }).await;

    let msg_map = get_global_value!(MSG_MAP);
    let mut msg_map = msg_map.lock().await;

    // 区分具体频道订阅还是匹配模式订阅进行分别处理
    let cs = channel.as_bytes();
    if cs[cs.len() - 1] == b'*' {
        if msg_map.pfuncs.remove(channel).is_some() {
            msg_map.modified = true;
        }
    } else if msg_map.funcs.remove(channel).is_some() {
        msg_map.modified = true;
    }

    if msg_map.modified {
        get_global_value!(SUB_DATA_TX).send(false)?;
    }

    Ok(())
}

/// 查询指定的频道是否已经被订阅(相同地址的频道只能被订阅1次)
#[allow(dead_code)]
pub async fn already_subscribe(channel: &str) -> bool {
    let msg_map = get_global_value!(MSG_MAP);
    let msg_map = msg_map.lock().await;
    let cs = channel.as_bytes();
    if cs[cs.len() - 1] == b'*' {
        msg_map.pfuncs.contains_key(channel)
    } else {
        msg_map.funcs.contains_key(channel)
    }
}

/// 生成订阅频道(使用统一的应用前缀)
///
/// Arguments:
///
/// * `sub_channel`: 子频道
///
/// Returns:
///
/// 频道名称
#[allow(dead_code)]
pub fn make_channel(sub_channel: ChannelName) -> CompactString {
    let sub_channel = match sub_channel {
        ChannelName::ModApi        => "modified:api",
        ChannelName::ModConfig     => "modified:config",
        ChannelName::ModDict       => "modified:dict",
        ChannelName::ModPermission => "modified:permission",
        ChannelName::ModRole       => "modified:role",
        ChannelName::ModUser       => "modified:user",
        ChannelName::ModMenu       => "modified:menu",
        ChannelName::Login         => "event.login",
        ChannelName::Logout        => "event.logout",
    };

    format_compact!("{}:{}", AppConf::get().cache_pre, sub_channel)
}

#[allow(dead_code)]
pub async fn publish(channel: &str, message: &str) -> Result<()> {
    let mut conn = rcache::get_conn().await?;
    let cmd = Cmd::publish(channel, message);
    match cmd.query_async(&mut conn).await {
        Ok(v) => Ok(v),
        Err(e) => {
            log::error!("redis publish error: {e:?}");
            anyhow::bail!("系统内部错误, 发布消息操作失败")
        }
    }
}

#[allow(dead_code)]
pub fn publish_rec_change_spawm<T>(chan: ChannelName,
    change_type: RecordChangedType, value: T)
where
    T: Serialize + Send + 'static,
{
    tokio::spawn(async move {
        let chan = make_channel(chan);

        let msg = serde_json::to_string(&RecChanged {
            change_type,
            data: Some(value),
        }).expect("json序列化失败");

        if let Err(e) = publish(&chan, &msg).await {
            log::error!("redis发布消息失败: {e:?}");
        }
    });
}

impl <T: Serialize> RecChanged<T> {
    #[allow(dead_code)]
    pub async fn publish_all(channel: &str) -> Result<()> {
        publish(channel, &serde_json::to_string(&Self {
            change_type: RecordChangedType::All,
            data: None,
        })?).await
    }

    #[allow(dead_code)]
    pub async fn publish_insert(channel: &str, data: T) -> Result<()> {
        publish(channel, &serde_json::to_string(&Self {
            change_type: RecordChangedType::Insert,
            data: Some(data),
        })?).await
    }

    #[allow(dead_code)]
    pub async fn publish_update(channel: &str, data: T) -> Result<()> {
        publish(channel, &serde_json::to_string(&Self {
            change_type: RecordChangedType::Update,
            data: Some(data),
        })?).await
    }

    #[allow(dead_code)]
    pub async fn publish_delete(channel: &str, data: T) -> Result<()> {
        publish(channel, &serde_json::to_string(&Self {
            change_type: RecordChangedType::Delete,
            data: Some(data),
        })?).await
    }

}

static mut SUB_DATA: Option<Mutex<PubSub>> = None;
static mut MSG_MAP: Option<Mutex<MessageMap>> = None;
static mut SUB_DATA_TX: Option<mpsc::UnboundedSender<bool>> = None;
static mut SUB_DATA_RX: Option<mpsc::UnboundedReceiver<bool>> = None;
static SUB_DATA_INIT: OnceCell<bool> = OnceCell::const_new();

#[async_trait::async_trait]
impl<FN: Send + Sync + 'static, Fut> RedisOnMessage for FN
where
    FN: Fn(Msg) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static, {

    async fn handle(&self, msg: Msg) -> Result<()> {
        self(msg).await
    }
}

type FuncMap = HashMap<CompactString, Box<dyn RedisOnMessage>>;

struct MessageMap {
    // MessageMap对象修改标志
    // 用于消除异步中多次调用subscribe订阅造成反复注册的操作
    modified: bool,
    funcs: FuncMap,
    pfuncs: FuncMap,
}

fn gen_url(ac: &AppConf) -> String {
    format!(
        "redis://{}:{}@{}:{}/{}",
        ac.cache_user, ac.cache_pass, ac.cache_host, ac.cache_port, ac.cache_name
    )
}

/// 初始化消息订阅相关全局变量
async fn init_sub_data() -> Result<()> {
    let url = gen_url(AppConf::get());
    let client = Client::open(url).context("创建redis连接失败")?;
    let conn = client.get_async_connection().await.context("获取redis连接失败")?;
    let sub_data = Mutex::new(conn.into_pubsub());
    let msg_map = Mutex::new(MessageMap {
        modified: false,
        funcs: HashMap::new(),
        pfuncs: HashMap::new(),
    });
    let (tx, rx) = mpsc::unbounded_channel();

    unsafe {
        debug_assert!(SUB_DATA.is_none());
        debug_assert!(MSG_MAP.is_none());
        debug_assert!(SUB_DATA_TX.is_none());
        debug_assert!(SUB_DATA_RX.is_none());

        SUB_DATA = Some(sub_data);
        MSG_MAP = Some(msg_map);
        SUB_DATA_TX = Some(tx);
        SUB_DATA_RX = Some(rx);
    }

    tokio::task::spawn(msg_loop());

    Ok(())
}

/// redis异步消息处理函数
async fn msg_loop() {
    // 获取全局接收通道,接收并处理停止订阅的消息
    let rx = get_global_value!(SUB_DATA_RX);

    loop {
        // 获取redis事件订阅对象
        let sub_data_lock = get_global_value!(SUB_DATA);
        let mut sub_data_lock = sub_data_lock.lock().await;

        // 注册map中所有订阅事件
        subscribe_by_map(&mut sub_data_lock).await.expect("redis注册频道失败");

        let mut pstream = sub_data_lock.on_message();

        loop {
            tokio::select! {
                // 收到订阅消息, 调用相应的消息处理函数
                Some(msg) = pstream.next() => on_message(msg).await.expect("redis订阅消息处理函数失败"),

                // 收到停止订阅消息, false表示订阅频道有变化, 需要重新订阅, true表示停止订阅
                Some(flag) = rx.recv() =>
                    if flag {
                        log::trace!("结束redis消息订阅处理");
                        return;
                    } else {
                        log::trace!("暂停redis消息处理");
                        break;
                    },
            };
        }
    }
}

/// 订阅MSG_MAP中的所有频道
async fn subscribe_by_map(sub_data_lock: &mut MutexGuard<'_, PubSub>) -> Result<()> {
    let msg_map = get_global_value!(MSG_MAP);
    let mut msg_map = msg_map.lock().await;

    // 异步订阅, 会出现多个协程订阅产生反复订阅/取消的操作
    // 加个modified标志, 有利于合并同时进行的订阅操作
    if msg_map.modified {
        for (key, _) in msg_map.funcs.iter() {
            sub_data_lock.subscribe(key.as_str()).await?;
            log::trace!("订阅redis消息: {key}")
        }

        for (key, _) in msg_map.pfuncs.iter() {
            sub_data_lock.psubscribe(key.as_str()).await?;
            log::trace!("订阅redis消息: {key}")
        }
        msg_map.modified = false;
    } else {
        log::trace!("所有频道均已订阅, 无需再次订阅")
    }

    Ok(())
}

/// 收到订阅频道的消息后的处理函数
async fn on_message(msg: Msg) -> Result<()> {
    let msg_map_lock = get_global_value!(MSG_MAP);
    let msg_map_lock = msg_map_lock.lock().await;

    let channel: String = if msg.from_pattern() {
        msg.get_pattern()?
    } else {
        msg.get_channel()?
    };

    if log::log_enabled!(log::Level::Trace) {
        let payload: String = msg.get_payload().unwrap();
        log::trace!("[on_message] 收到redis消息: [{}] => [{}]", channel, payload);
    }


    let func = if msg.from_pattern() {
        msg_map_lock.pfuncs.get(channel.as_str())
    } else {
        msg_map_lock.funcs.get(channel.as_str())
    };

    if let Some(func) = func {
        if let Err(e) = func.handle(msg).await {
            log::error!("redis消息处理失败: {:?}", e);
        }
    }

    Ok(())
}
