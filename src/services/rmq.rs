//! redis 消息队列服务
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, OnceLock,
    },
    vec,
};

use anyhow_ext::{Context, Result};
use compact_str::{format_compact, CompactString};
use deadpool_redis::redis::{aio::PubSub, Client, Cmd, Msg};
use futures_util::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::rcache;
use crate::AppConf;

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

#[derive(Debug)]
pub struct MqMsg(Msg);

#[async_trait::async_trait]
pub trait MsgQueueEvent: Send + Sync + 'static {
    async fn handle(&self, msg: Arc<MqMsg>) -> Result<()>;
}

pub struct ChanBuilder (CompactString);

struct FuncItem {
    id: u32,    // 订阅的唯一id
    func: Func, // 消息处理函数
}

type Func = Arc<dyn MsgQueueEvent>;
type FuncMap = HashMap<CompactString, Vec<FuncItem>>;

enum ModData {
    Insert(u32, CompactString, Func),
    Delete(u32),
    Quit,
}

struct AllFuncMap {
    funcs: FuncMap,       // 完全匹配的频道/消息处理函数
    pfuncs: FuncMap,      // 前缀匹配的频道/消息处理函数
}

struct GlobalVal {
    tx: UnboundedSender<ModData>,       // 接收重新订阅消息的通道
    next_id: AtomicU32,                 // 下一个订阅消息的id
}

static GLOBAL_VAL: OnceLock<GlobalVal> = OnceLock::new();

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
pub async fn subscribe(chan: CompactString, func: impl MsgQueueEvent) -> Result<u32> {
    debug_assert!(!chan.is_empty());

    let gv = global_val();
    let id = gv.next_id.fetch_add(1, Ordering::Acquire);
    gv.tx.send(ModData::Insert(id, chan, Arc::new(func)))?;

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
    let gv = global_val();
    for id in ids {
        gv.tx.send(ModData::Delete(*id))?;
    }

    Ok(())
}

/// 停止监听服务
#[allow(dead_code)]
pub async fn stop() -> Result<()> {
    Ok(global_val().tx.send(ModData::Quit)?)
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
        ChannelName::ModApi => "modified:api",
        ChannelName::ModConfig => "modified:config",
        ChannelName::ModDict => "modified:dict",
        ChannelName::ModPermission => "modified:permission",
        ChannelName::ModRole => "modified:role",
        ChannelName::ModUser => "modified:user",
        ChannelName::ModMenu => "modified:menu",
        ChannelName::Login => "event:login",
        ChannelName::Logout => "event:logout",
    };

    format_compact!("{}:{}", AppConf::get().cache_pre, sub_channel)
}

/// 发送redis消息
#[allow(dead_code)]
pub async fn publish(channel: &str, message: &str) -> Result<()> {
    let mut conn = rcache::get_conn().await?;
    let cmd = Cmd::publish(channel, message);
    match cmd.query_async(&mut conn).await {
        Ok(v) => {
            log::trace!("发送redis消息成功: chan = {}, msg = {}", channel, message);
            Ok(v)
        }
        Err(e) => {
            log::error!(
                "redis publish error: chan = {}, msg = {}, err = {:?}",
                channel,
                message,
                e
            );
            anyhow_ext::bail!("系统内部错误, 发布消息操作失败")
        }
    }
}

/// 使用异步方式发送消息，无需等待消息发送完成立即返回
#[allow(dead_code)]
pub fn publish_async(channel: CompactString, message: String) {
    tokio::spawn(async move {
        let (chan, msg) = (channel, message);
        let _ = publish(&chan, &msg).await;
    });
}

impl MqMsg {
    #[allow(dead_code)]
    pub fn get_channel(&self) -> &str {
        self.0.get_channel_name()
    }

    #[allow(dead_code)]
    pub fn get_payload(&self) -> &[u8] {
        self.0.get_payload_bytes()
    }

    #[allow(dead_code)]
    pub fn get_pattern(&self) -> Result<String> {
        Ok(self.0.get_pattern()?)
    }
}

#[allow(dead_code)]
impl ChanBuilder {
    pub fn new() -> Self {
        Self(CompactString::with_capacity(0))
    }

    pub fn build(self) -> CompactString {
        self.0
    }

    pub fn path(mut self, path: &str) -> Self {
        self.0.push_str(path);
        self
    }
}

#[async_trait::async_trait]
impl<FN: Send + Sync + 'static, Fut> MsgQueueEvent for FN
where
    FN: Fn(Arc<MqMsg>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    async fn handle(&self, msg: Arc<MqMsg>) -> Result<()> {
        self(msg).await
    }
}

fn gen_url(ac: &AppConf) -> String {
    format!(
        "redis://{}:{}@{}:{}/{}",
        ac.cache_user, ac.cache_pass, ac.cache_host, ac.cache_port, ac.cache_name
    )
}

/// redis异步消息处理函数
async fn msg_loop(mut rx: UnboundedReceiver<ModData>) {
    let mut pubsub = {
        let url = gen_url(AppConf::get());
        let client = Client::open(url).expect("创建redis连接失败");
        client.get_async_pubsub().await.expect("获取redis消息订阅流失败")
    };
    let mut all_func_map = AllFuncMap { funcs: HashMap::new(), pfuncs: HashMap::new() };
    let mut curr_recv = None;

    loop {
        // 注册map中所有订阅事件
        if let Some(recv) = curr_recv.take() {
            match recv {
                ModData::Insert(id, chan, func) =>
                    redis_subscribe(&mut pubsub, &mut all_func_map, id, chan, func).await,
                ModData::Delete(id) =>
                    redis_unsubscribe(&mut pubsub, &mut all_func_map, id).await,
                ModData::Quit => return,
            }
        }

        // 获取redis事件订阅对象
        let mut pstream = pubsub.on_message();

        loop {
            tokio::select! {
                // 收到订阅消息, 调用相应的消息处理函数
                Some(msg) = pstream.next() => {
                    if let Err(e) = on_message(&all_func_map, msg).await {
                        log::error!("[redis:on_message] 发生错误: {e:?}");
                    }
                }

                // 收到更新订阅消息，需要退出内层循环才能获取pubsub变量进行操作
                Some(data) = rx.recv() => {
                    curr_recv = Some(data);
                    break;
                }
            };
        }
    }
}

async fn redis_subscribe(pubsub: &mut PubSub, all_func_map: &mut AllFuncMap,
    id: u32, chan: CompactString, func: Func)
{
    let cbs = chan.as_bytes();
    let funcs = if cbs[cbs.len() - 1] == b'*' {
        &mut all_func_map.pfuncs
    } else {
        &mut all_func_map.funcs
    };
    let mut is_new = false;
    let func_item = FuncItem { id, func };

    match funcs.entry(chan.clone()) {
        Entry::Occupied(mut v) => v.get_mut().push(func_item),
        Entry::Vacant(v) => {
            is_new = true;
            v.insert(vec![func_item]);
        },
    }

    if is_new {
        match pubsub.subscribe(chan.as_str()).await {
            Ok(_) => log::debug!("订阅频道[id:{}]: {}", id, chan),
            Err(e) => {
                remove_func_by_id(all_func_map, id);
                log::error!("redis订阅失败: chan = {}, error = {:?}", chan, e);
            }
        }
    }
}

async fn redis_unsubscribe(pubsub: &mut PubSub, all_func_map: &mut AllFuncMap, id: u32) {
    let chan = remove_func_by_id(all_func_map, id);

    if !chan.is_empty() {
        match pubsub.unsubscribe(chan.as_str()).await {
            Ok(_) => log::debug!("取消订阅[id:{}]: {}", id, chan),
            Err(e) => log::error!("redis取消订阅失败: id = {}, chan = {}, error = {:?}", id, chan, e),
        }
    }
}

fn remove_func_by_id(all_func_map: &mut AllFuncMap, id: u32) -> CompactString {
    let mut chan = CompactString::with_capacity(0);

    let find_fn = |k: &CompactString, v: &mut Vec<FuncItem>, chan: &mut CompactString| {
        let old_len = v.len();
        v.retain(|v2| v2.id != id);
        if old_len != v.len() {
            chan.push_str(k);
        }
        !v.is_empty()
    };

    all_func_map.funcs.retain(|k, v| find_fn(k, v, &mut chan));

    if chan.is_empty() {
        all_func_map.pfuncs.retain(|k, v| find_fn(k, v, &mut chan));
    }

    chan
}

/// 收到订阅频道的消息后的处理函数
async fn on_message(all_func_map: &AllFuncMap, msg: Msg) -> Result<()> {
    // 获取频道
    let chan = msg.get_channel_name();
    let pchan: Option<String> = msg.get_pattern().context("解析pattern出错")?;

    // 记录日志
    log::trace!(
        "收到redis消息: [{}] => [{}]",
        chan,
        std::str::from_utf8(msg.get_payload_bytes()).unwrap_or_default()
    );

    let func_vec = match &pchan {
        Some(pchan) => all_func_map.pfuncs.get(pchan.as_str()),
        None => all_func_map.funcs.get(chan),
    };

    match func_vec {
        Some(func_vec) => {
            let msg = Arc::new(MqMsg(msg));

            // 调用该频道下所有的回调函数
            for fi in func_vec.iter() {
                let id = fi.id;
                let func = fi.func.clone();
                let m = msg.clone();
                tokio::spawn(async move {
                    if let Err(e) = func.handle(m).await {
                        log::error!("mq callback error[id:{}]: {}", id, e);
                    }
                });
            }
        }
        None => log::warn!("redis消息{}没有定义响应的处理函数", chan),
    };

    Ok(())
}

fn global_val() -> &'static GlobalVal {
    GLOBAL_VAL.get_or_init(|| {
        let (tx, rx) = unbounded_channel();
        tokio::task::spawn(msg_loop(rx));

        GlobalVal {
            tx,
            next_id: AtomicU32::new(1)
        }
    })
}
