//! redis 消息队列服务

use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc}, vec};

use anyhow::{Context, Result};
use compact_str::{CompactString, format_compact};
use deadpool_redis::redis::{aio::PubSub, Msg, Client, Cmd};
use futures_util::StreamExt;
use tokio::sync::{Mutex, mpsc, OnceCell};
use parking_lot::Mutex as PlMutex;

use crate::AppConf;
use super::rcache;

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

#[async_trait::async_trait]
pub trait RedisOnMessage: Send + Sync + 'static {
    async fn handle(&self, msg: Arc<Msg>) -> Result<()>;
}

#[derive(Clone)]
struct FuncItem {
    id: u32,
    func: Arc<dyn RedisOnMessage>,
}

type FuncMap = HashMap<CompactString, Vec<FuncItem>>;

struct MessageMap {
    modified: AtomicBool, // 修改标志, 用于消除异步中多次调用subscribe订阅造成反复注册的操作
    id:       u32,        // 最后插入消息对象ID，用于funcs和pfuncs插入
    funcs:    FuncMap,    // 完全匹配的频道/消息处理函数
    pfuncs:   FuncMap,    // 前缀匹配的频道/消息处理函数
}

struct GlobalVal {
    pubsub:  Mutex<PubSub>,               // redis消息订阅对象
    msg_map: PlMutex<Arc<MessageMap>>,    // 频道/处理函数对象
    tx:      mpsc::UnboundedSender<bool>, // 接收重新订阅消息的通道
}

static GLOBAL_VAL: Option<GlobalVal> = None;
static GLOBAL_VAL_INIT: OnceCell<bool> = OnceCell::const_new();

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
pub async fn subscribe(chan: CompactString, func: impl RedisOnMessage) -> Result<u32> {
    debug_assert!(!chan.is_empty());
    global_init().await;

    let mut msg_map = global_val().msg_map.lock();
    let mut new_msg_map = msg_map.as_ref().clone();
    let id = msg_map.id + 1;
    let cbs = chan.as_bytes();
    let func = Arc::new(func);

    let funcs = if cbs[cbs.len() - 1] == b'*' {
        &mut new_msg_map.pfuncs
    } else {
        &mut new_msg_map.funcs
    };

    // 添加新的订阅频道
    funcs.entry(chan)
        .and_modify(|v| v.push(FuncItem { id, func: func.clone() }))
        .or_insert_with(|| vec!(FuncItem { id, func }));

    // 需要订阅的频道尚未有人订阅, 可以订阅
    new_msg_map.modified.store(true, Ordering::Release);
    new_msg_map.id = id;
    *msg_map = Arc::new(new_msg_map);
    global_val().tx.send(false)?;

    Ok(id)
}

/// 取消订阅
///
/// Arguments:
///
/// * `id`: 取消订阅的频道id
///
#[allow(dead_code)]
pub async fn unsubscribe(id: u32) -> Result<()> {
    unsubscribe_slice(std::slice::from_ref(&id)).await
}

/// 取消订阅
///
/// Arguments:
///
/// * `ids`: 取消订阅的频道的id列表
///
#[allow(dead_code)]
pub async fn unsubscribe_slice(ids: &[u32]) -> Result<()> {
    global_init().await;

    let mut msg_map = global_val().msg_map.lock();
    let mut new_msg_map = msg_map.as_ref().clone();
    let mut modified = false;
    let f = |v: &mut Vec<FuncItem>, id: u32, modi: &mut bool| {
        v.retain(|v2| {
            *modi = v2.id == id;
            !*modi
        });
        !v.is_empty()
    };

    for id in ids {
        new_msg_map.pfuncs.retain(|_, v| f(v, *id, &mut modified));
        new_msg_map.funcs.retain(|_, v| f(v, *id, &mut modified));
    }

    if modified {
        new_msg_map.modified.store(true, Ordering::Release);
        *msg_map = Arc::new(new_msg_map);
        global_val().tx.send(false)?;
    }

    Ok(())
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

#[async_trait::async_trait]
impl<FN: Send + Sync + 'static, Fut> RedisOnMessage for FN
where
    FN: Fn(Arc<Msg>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static, {

    async fn handle(&self, msg: Arc<Msg>) -> Result<()> {
        self(msg).await
    }
}

impl Clone for MessageMap {
    fn clone(&self) -> Self {
        Self {
            modified: AtomicBool::new(self.modified.load(Ordering::Relaxed)),
            id: self.id.clone(),
            funcs: self.funcs.clone(),
            pfuncs: self.pfuncs.clone()
        }
    }
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
    let pubsub = Mutex::new(conn.into_pubsub());

    let msg_map = PlMutex::new(Arc::new(MessageMap {
        modified: AtomicBool::new(false),
        id: 0,
        funcs: HashMap::new(),
        pfuncs: HashMap::new(),
    }));
    let (tx, rx) = mpsc::unbounded_channel();

    unsafe {
        let gval = &GLOBAL_VAL as *const Option<GlobalVal> as *mut Option<GlobalVal>;
        *gval = Some(GlobalVal {pubsub, msg_map, tx});
    }

    tokio::task::spawn(msg_loop(rx));

    Ok(())
}

/// redis异步消息处理函数
async fn msg_loop(mut rx: mpsc::UnboundedReceiver<bool>) {
    loop {
        // 注册map中所有订阅事件
        subscribe_to_redis().await.expect("redis注册频道失败");

        // 获取redis事件订阅对象
        let mut pubsub = global_val().pubsub.lock().await;
        let mut pstream = pubsub.on_message();

        loop {
            tokio::select! {
                // 收到订阅消息, 调用相应的消息处理函数
                Some(msg) = pstream.next() => on_message(msg).await,

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
async fn subscribe_to_redis() -> Result<()> {
    let msg_map = global_val().msg_map.lock().clone();
    let modified = msg_map.modified.compare_exchange(
        true, false, Ordering::Acquire, Ordering::Relaxed
    ).unwrap_or(false);

    // 异步订阅, 会出现多个协程订阅产生反复订阅/取消的操作
    // 加个modified标志, 有利于合并同时进行的订阅操作
    if modified {
        let mut pubsub = global_val().pubsub.lock().await;

        let keys: Vec<_> = msg_map.funcs.keys().map(|v| v.as_str()).collect();
        if !keys.is_empty() {
            pubsub.subscribe(&keys).await?;
            log::trace!("订阅redis消息：{:?}", keys);
        }

        let keys: Vec<_> = msg_map.pfuncs.keys().map(|v| v.as_str()).collect();
        if !keys.is_empty() {
            pubsub.subscribe(&keys).await?;
            log::trace!("订阅redis消息：{:?}", keys);
        }
    } else {
        log::trace!("所有频道均已订阅, 无需再次订阅")
    }

    Ok(())
}

/// 收到订阅频道的消息后的处理函数
async fn on_message(msg: Msg) {
    const ON_MSG: &str = "[on_message]";

    // 获取频道
    let chan = if msg.from_pattern() {
        msg.get_pattern()
    } else {
        msg.get_channel()
    };
    let chan: String = match chan {
        Ok(s) => s,
        Err(e) => {
            log::warn!("{} 解析channel出错: {e:?}", ON_MSG);
            return;
        },
    };

    // 记录日志
    if log::log_enabled!(log::Level::Trace) {
        let payload: String = match msg.get_payload() {
            Ok(s) => s,
            Err(e) => {
                log::warn!("{} 解析payload出错: {e:?}", ON_MSG);
                return;
            },
        };
        log::trace!("{} 收到redis消息: [{}] => [{}]", ON_MSG, chan, payload);
    }

    let msg_map = global_val().msg_map.lock().clone();

    let func_vec = if msg.from_pattern() {
        msg_map.pfuncs.get(chan.as_str())
    } else {
        msg_map.funcs.get(chan.as_str())
    };

    match func_vec {
        Some(func_vec) => {
            let msg = Arc::new(msg);
            // 调用该频道下所有的回调函数
            for fi in func_vec {
                if let Err(e) = fi.func.handle(msg.clone()).await {
                    log::error!("redis消息处理失败: {e:?}");
                }
            }
        },
        None => log::trace!("{} 消息{}没有定义响应的处理函数", ON_MSG, chan),
    };

}

async fn global_init() {
    GLOBAL_VAL_INIT.get_or_init(|| async {
        init_sub_data().await.expect("初始化redis消息订阅全局对象失败");
        true
    }).await;
}

fn global_val() -> &'static GlobalVal {
    unsafe {
        match &GLOBAL_VAL {
            Some(v) => v,
            None => std::hint::unreachable_unchecked(),
        }
    }
}
