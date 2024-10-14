use std::{any::Any, mem::MaybeUninit, sync::Arc, time::Duration};

use anyhow_ext::Context;
use deadpool_redis::redis::{FromRedisValue, ToRedisArgs};
use mini_moka::sync::Cache;
use serde::{de::DeserializeOwned, Serialize};

use crate::utils::{consts, uni_redis::UniRedis};

use super::{mq, uri::UniRedisImpl};

pub type GeneralMultiCache = GeneralMultiCacheTmpl<UniRedisImpl>;

type MemCache = Cache<String, Arc<CacheValue>>;
type CacheValue = dyn Any + 'static + Send + Sync;

pub struct GeneralMultiCacheTmpl<R: UniRedis> {
    memory_cache: MemCache,
    redis_cache: R,
    redis_prefix: String,
    redis_expire: u32,
    chan_prefix: String,
}

static mut CACHE: MaybeUninit<GeneralMultiCache> = MaybeUninit::uninit();
#[cfg(debug_assertions)]
static mut INITED: bool = false;

impl<R: UniRedis> GeneralMultiCacheTmpl<R> {
    /// 创建对象
    ///
    /// Arguments:
    ///
    /// * `local_max_size`: 本地缓存最大数量
    /// * `local_expire`: 本地缓存存活时间
    /// * `redis_prefix`: redis缓存前缀
    /// * `redis_expire`: redis缓存过期时间
    ///
    pub fn new(
        local_max_size: u32,
        local_expire_secs: u32,
        redis_cache: R,
        redis_prefix: String,
        redis_expire_secs: u32,
        chan_prefix: String
    ) -> Self {
        let mut b = MemCache::builder().max_capacity(local_max_size as u64);
        if local_expire_secs > 0 {
            b = b.time_to_idle(Duration::from_secs(local_expire_secs as u64));
        }

        Self {
            memory_cache: b.build(),
            redis_cache,
            redis_prefix,
            redis_expire: redis_expire_secs,
            chan_prefix,
        }
    }

    /// 读取缓存项
    ///
    /// Arguments:
    ///
    /// * `category`: 类别
    /// * `key`: 键
    ///
    pub async fn get<T>(&self, category: &str, key: &str) -> Option<Arc<T>>
    where
        T: FromRedisValue + Send + Sync + 'static,
    {
        // 优先从本地缓存中读取，如果存在，则直接返回
        let mem_key = format!("{}:{}", category, key);
        let value = self.memory_cache.get(&mem_key);
        if let Some(value) = value {
            if let Ok(v) = value.downcast() {
                return Some(v);
            }
        }

        // 本地缓存找不到，从redis中读取，如果存在，则更新存活时间及本地缓存并返回
        let redis_key = format!("{}{}", self.redis_prefix, mem_key);
        if let Some(value) = self.redis_cache.get::<T>(&redis_key).await {
            let arc_value = Arc::new(value);
            self.redis_cache.expire(&redis_key, self.redis_expire as i64).await;
            self.memory_cache.insert(mem_key, arc_value.clone());
            return Some(arc_value);
        }

        None
    }

    /// 读取缓存项
    ///
    /// Arguments:
    ///
    /// * `category`: 类别
    /// * `key`: 键
    ///
    pub async fn get_json<T>(&self, category: &str, key: &str) -> Option<Arc<T>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        self.get_json_with(category, key, false).await
    }

    /// 读取缓存项
    ///
    /// Arguments:
    ///
    /// * `category`: 类别
    /// * `key`: 键
    /// * `use_decompress`: 是否进行解压缩
    ///
    pub async fn get_json_with<T>(&self, category: &str, key: &str, use_lz4: bool) -> Option<Arc<T>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        // 优先从本地缓存中读取，如果存在，则直接返回
        let mem_key = format!("{}:{}", category, key);
        let value = self.memory_cache.get(&mem_key);
        if let Some(value) = value {
            if let Ok(v) = value.downcast() {
                return Some(v);
            }
        }

        // 本地缓存找不到，从redis中读取，如果存在，则更新存活时间及本地缓存并返回
        let redis_key = format!("{}{}", self.redis_prefix, mem_key);
        if let Some(value) = self.redis_cache.get_json::<T>(&redis_key, use_lz4).await {
            let arc_value = Arc::new(value);
            self.redis_cache.expire(&redis_key, self.redis_expire as i64).await;
            self.memory_cache.insert(mem_key, arc_value.clone());
            return Some(arc_value);
        }

        None
    }

    /// 写入缓存项
    ///
    /// Arguments:
    ///
    /// * `category`: 类别
    /// * `key`: 键
    /// * `value`: 值
    ///
    pub async fn put<T>(&self, category: &str, key: &str, value: Arc<T>)
    where
        T: ToRedisArgs + Send + Sync + 'static,
    {
        let mem_key = format!("{}:{}", category, key);
        let redis_key = format!("{}{}", self.redis_prefix, mem_key);
        // 写入redis
        self.redis_cache.set_ex(&redis_key, &value, self.redis_expire as u64).await;
        // 写入本地缓存
        self.memory_cache.insert(mem_key, value);
    }

    /// 写入缓存项
    ///
    /// Arguments:
    ///
    /// * `category`: 类别
    /// * `key`: 键
    /// * `value`: 值
    ///
    pub async fn put_json<T>(&self, category: &str, key: &str, value: Arc<T>)
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.put_json_with(category, key, value, false).await
    }

    /// 写入缓存项
    ///
    /// Arguments:
    ///
    /// * `category`: 类别
    /// * `key`: 键
    /// * `value`: 值
    /// * `use_compress`: 是否进行压缩
    ///
    pub async fn put_json_with<T>(&self, category: &str, key: &str, value: Arc<T>, use_lz4: bool)
    where
        T: Serialize + Send + Sync + 'static,
    {
        let mem_key = format!("{}:{}", category, key);
        let redis_key = format!("{}{}", self.redis_prefix, mem_key);
        // 写入redis
        self.redis_cache.set_json_ex(&redis_key, &value, self.redis_expire as u64, use_lz4).await;
        // 写入本地缓存
        self.memory_cache.insert(mem_key, value);
    }

    /// 删除缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    ///
    pub async fn del(&self, category: &str, key: &str) {
        let mem_key = format!("{}:{}", category, key);
        self.memory_cache.invalidate(&mem_key);
        let redis_key = format!("{}{}", self.redis_prefix, mem_key);
        self.redis_cache.del(&redis_key).await;
    }

    /// 清除缓存
    pub async fn clear_category(&self, category: &str) {
        // 删除内存中的数据
        let mem_key = format!("{}:", category);
        let mem_keys: Vec<_> = self
            .memory_cache
            .iter()
            .filter(|entry| entry.key().starts_with(mem_key.as_str()))
            .map(|entry| entry.key().clone())
            .collect();
        mem_keys.iter().for_each(|k| self.memory_cache.invalidate(k));

        // 删除redis中的数据
        let redis_key = format!("{}{}:*", self.redis_prefix, category);
        self.redis_cache.pdel(&redis_key).await;
    }

    /// 清除缓存
    pub async fn clear(&self) {
        self.memory_cache.invalidate_all();
        let redis_key = format!("{}*", self.redis_prefix);
        self.redis_cache.pdel(&redis_key).await;
    }

    pub async fn notify(&self, category: &str, key: &str) {
        let chan = format!("{}{}", self.chan_prefix, category);
        self.redis_cache.publish(&chan, key).await
    }


    async fn on_notified(&self, msg: Arc<mq::Msg>) {
        let category = &msg.get_channel()[self.chan_prefix.len()..];
        let key = msg.get_payload();
        log::trace!("收到[{}]消息, category = {}, key = {}", msg.get_channel(), category, key);
        if !key.is_empty() {
            self.del(category, key).await
        } else {
            self.clear_category(category).await
        }
    }

}

pub async fn init(
    local_max_size: u32,
    local_expire_secs: u32,
    redis: UniRedisImpl,
    redis_prefix: &str,
    redis_expire_secs: u32,
) {
    let mut redis_prefix = String::from(redis_prefix);
    if !redis_prefix.is_empty() {
        redis_prefix.push(':');
    }
    let mut chan_prefix = redis_prefix.clone();
    redis_prefix.push_str(consts::gmc::TABLE_KEY);
    redis_prefix.push(':');
    chan_prefix.push_str(consts::gmc::MOD_KEY);
    chan_prefix.push(':');

    let mut chan = chan_prefix.clone();
    chan.push('*');

    let cache = GeneralMultiCache::new(
        local_max_size,
        local_expire_secs,
        redis,
        redis_prefix,
        redis_expire_secs,
        chan_prefix,
    );

    unsafe {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!INITED);
            INITED = true;
        }

        CACHE.write(cache);
    }

    crate::services::mq::subscribe(chan, |msg| async move {
        get_cache().on_notified(msg).await;
        Ok(())
    }).await.dot().unwrap();
}

pub fn get_cache() -> &'static GeneralMultiCache {
    unsafe {
        #[cfg(debug_assertions)]
        debug_assert!(INITED);

        CACHE.assume_init_ref()
    }
}
