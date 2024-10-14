//! redis 消息队列服务
use std::{
    collections::{hash_map::Entry, HashMap}, mem::MaybeUninit, sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    }, vec
};

use anyhow_ext::{Context, Result};
use deadpool_redis::redis::{aio::PubSub, Client, Cmd};
use futures_util::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::uri;


#[derive(Debug)]
pub struct Msg(deadpool_redis::redis::Msg);

impl Msg {
    pub fn get_channel(&self) -> &str {
        self.0.get_channel_name()
    }

    pub fn get_payload(&self) -> &str {
        std::str::from_utf8(self.0.get_payload_bytes()).unwrap_or("")
    }

    pub fn get_pattern(&self) -> Option<String> {
        match self.0.get_pattern::<String>() {
            Ok(s) => if !s.is_empty() {
                Some(s)
            } else {
                None
            },
            Err(_) => None,
        }
    }
}


#[async_trait::async_trait]
pub trait OnMsg: Send + Sync + 'static {
    async fn handle(&self, msg: Arc<Msg>) -> Result<()>;
}

#[async_trait::async_trait]
impl<FN: Send + Sync + 'static, Fut> OnMsg for FN
where
    FN: Fn(Arc<Msg>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    async fn handle(&self, msg: Arc<Msg>) -> Result<()> {
        self(msg).await
    }
}


struct FuncItem {
    id: u32,    // 订阅的唯一id
    func: OnMsgFn, // 消息处理函数
}

type OnMsgFn = Arc<dyn OnMsg>;
type FuncMap = HashMap<String, Vec<FuncItem>>;

enum EvtData {
    Insert(u32, String, OnMsgFn),
    Delete(u32),
    Quit,
}

struct AllFuncMap {
    funcs: FuncMap,     // 完全匹配的频道/消息处理函数
    pfuncs: FuncMap,    // 前缀匹配的频道/消息处理函数
}

struct GlobalVal {
    tx: UnboundedSender<EvtData>,   // 接收重新订阅消息的通道
    seq: AtomicU32,                 // 下一个订阅消息的id
}

static mut GLOBAL_VAL: MaybeUninit<GlobalVal> = MaybeUninit::uninit();
#[cfg(debug_assertions)]
static mut INITED: bool = false;

fn global_val() -> &'static GlobalVal {
    unsafe {
        #[cfg(debug_assertions)]
        debug_assert!(INITED);
        GLOBAL_VAL.assume_init_ref()
    }
}

/// 初始化redis连接字符串
pub fn init(url: &str, use_test: bool) {
    if use_test {
        uri::try_connect(url).unwrap();
    }

    let (tx, rx) = unbounded_channel();
    let gv = GlobalVal { tx, seq: AtomicU32::new(1) };

    unsafe {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!INITED);
            INITED = true;
        }
        GLOBAL_VAL.write(gv);
    }

    tokio::task::spawn(msg_loop(rx, url.to_string()));
}

/// 订阅消息, 当末尾为'*'时, 表示通配符模式订阅.
/// 相同的频道且相同的回调函数只能被订阅1次,
/// 当频道存在时, 订阅失败
///
/// Arguments:
///
/// * `channel`: 订阅的频道, "abc" 可接收来自 "abc" 的消息,
///     但不能接收 "abcd" 的消息,
///     "abc*" 表示可接收 "abc", "abcd", "abcde" 的消息
/// * `on_msg`: 异步回调处理函数
///
/// Returns:
///
/// * `Ok(id)`: 订阅成功，返回订阅id
/// * `Err(e)`: 函数调用失败
pub async fn subscribe(chan: String, func: impl OnMsg) -> Result<u32> {
    debug_assert!(!chan.is_empty());

    let gv = global_val();
    let id = gv.seq.fetch_add(1, Ordering::Acquire);
    gv.tx.send(EvtData::Insert(id, chan, Arc::new(func)))?;

    Ok(id)
}

/// 取消订阅
///
/// Arguments:
///
/// * `id`: 取消订阅的频道id
///
pub async fn unsubscribe(id: u32) -> Result<()> {
    unsubscribe_slice(std::slice::from_ref(&id)).await
}

/// 取消订阅
///
/// Arguments:
///
/// * `ids`: 取消订阅的频道的id列表
///
pub async fn unsubscribe_slice(ids: &[u32]) -> Result<()> {
    let gv = global_val();
    for id in ids {
        gv.tx.send(EvtData::Delete(*id))?;
    }

    Ok(())
}

/// 停止监听服务
pub async fn stop() -> Result<()> {
    Ok(global_val().tx.send(EvtData::Quit)?)
}

/// 发送redis消息
pub async fn publish(channel: &str, message: &str) -> Result<()> {
    let mut conn = uri::get_conn().await?;
    let cmd = Cmd::publish(channel, message);
    match cmd.query_async(&mut conn).await {
        Ok(v) => {
            log::trace!("发送redis消息成功: chan = {channel}, msg = {message}");
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
pub fn publish_async(channel: String, message: String) {
    tokio::spawn(async move {
        let (chan, msg) = (channel, message);
        let _ = publish(&chan, &msg).await;
    });
}

/// redis异步消息处理函数
async fn msg_loop(mut rx: UnboundedReceiver<EvtData>, url: String) {
    let mut pubsub = {
        let client = Client::open(url).dot().expect("创建redis连接失败");
        client.get_async_pubsub().await.dot().expect("获取redis消息订阅流失败")
    };
    let mut all_func_map = AllFuncMap {
        funcs: HashMap::new(),
        pfuncs: HashMap::new(),
    };
    let mut curr_recv = None;

    loop {
        // 注册map中所有订阅事件
        if let Some(recv) = curr_recv.take() {
            match recv {
                EvtData::Insert(id, chan, func) => {
                    if subscribe_to_map(&mut all_func_map, id, chan.clone(), func) {
                        redis_subscribe(&mut pubsub, &mut all_func_map, id, &chan).await;
                    }
                }
                EvtData::Delete(id) => redis_unsubscribe(&mut pubsub, &mut all_func_map, id).await,
                EvtData::Quit => return,
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

fn subscribe_to_map(func_map: &mut AllFuncMap, id: u32, chan: String, func: OnMsgFn) -> bool {
    let funcs = if is_pattern(&chan) {
        &mut func_map.pfuncs
    } else {
        &mut func_map.funcs
    };
    let func_item = FuncItem { id, func };

    match funcs.entry(chan) {
        Entry::Occupied(mut v) => {
            v.get_mut().push(func_item);
            false
        }
        Entry::Vacant(v) => {
            v.insert(vec![func_item]);
            true
        }
    }
}

async fn redis_subscribe(pubsub: &mut PubSub, func_map: &mut AllFuncMap, id: u32, chan: &str) {
    match pubsub.subscribe(chan).await {
        Ok(_) => log::debug!("订阅频道[{id}]: {chan}"),
        Err(e) => {
            remove_func_by_id(func_map, id);
            log::error!("订阅频道[{id}]失败: chan = {chan}, error = {e:?}");
        }
    }
}

async fn redis_unsubscribe(pubsub: &mut PubSub, all_func_map: &mut AllFuncMap, id: u32) {
    let chan = remove_func_by_id(all_func_map, id);

    if !chan.is_empty() {
        match pubsub.unsubscribe(chan.as_str()).await {
            Ok(_) => log::debug!("取消订阅[id:{}]: {}", id, chan),
            Err(e) => log::error!("redis取消订阅{id}失败: chan = {chan}, error = {e:?}"),
        }
    }
}

/// 删除订阅, 返回成功删除的订阅对应的频道, 删除失败返回空字符串
fn remove_func_by_id(all_func_map: &mut AllFuncMap, id: u32) -> String {
    let mut chan = remove_from_map(&mut all_func_map.funcs, id);
    if chan.is_empty() {
        chan = remove_from_map(&mut all_func_map.pfuncs, id);
    }
    chan
}

fn remove_from_map(func_map: &mut FuncMap, id: u32) -> String {
    let mut index = None;
    let mut chan = String::new();

    for (k, vec) in func_map.iter_mut() {
        for (i, item) in vec.iter().enumerate() {
            if item.id == id {
                index = Some(i);
                break;
            }
        }

        if let Some(index) = index {
            vec.remove(index);
            if vec.is_empty() {
                chan.push_str(k);
            }
            break;
        }
    }

    if !chan.is_empty() {
        func_map.remove(&chan);
    }

    chan
}

/// 收到订阅频道的消息后的处理函数
async fn on_message(all_func_map: &AllFuncMap, msg: deadpool_redis::redis::Msg) -> Result<()> {
    // 获取频道
    let chan = msg.get_channel_name();
    let pchan: Option<String> = msg.get_pattern().context("解析pattern出错").dot()?;

    // 记录日志
    log::trace!(
        "收到redis消息: [{}] => [{}]",
        chan,
        std::str::from_utf8(msg.get_payload_bytes()).unwrap_or_default()
    );

    // 查找消息对应的处理函数数组
    let func_vec = match &pchan {
        Some(pchan) => all_func_map.pfuncs.get(pchan.as_str()),
        None => all_func_map.funcs.get(chan),
    };

    match func_vec {
        Some(func_vec) => {
            let msg = Arc::new(Msg(msg));

            // 调用该频道下所有的回调函数
            for item in func_vec.iter() {
                let (id, func, m) = (item.id, item.func.clone(), msg.clone());
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

fn is_pattern(chan: &str) -> bool {
    chan.as_bytes().last() == Some(&b'*')
}
