//! 多级缓存类，分为2级缓存，1级缓存是内存，2级缓存是redis
//!
//! Author: kiven lee
//! Date: 2024-07-31

use anyhow_ext::{bail, Result};
use mini_moka::sync::Iter;
use serde::{de::DeserializeOwned, Serialize};

use std::{hash::{Hash, RandomState}, sync::Arc, time::Duration};

use super::uni_redis::UniRedis;

type Cache<K, V> = mini_moka::sync::Cache<K, V>;

pub struct MultiCache<K, V, R: UniRedis> {
    memory_cache: Cache<K, V>,
    redis_cache: R,
    redis_prefix: String,
    redis_expire: Duration,
}

pub struct Builder<R> {
    local_max_size: u64,
    local_expire: Duration,
    redis_cache: Option<R>,
    redis_prefix: String,
    redis_expire: Duration,
}

impl<K, V, R> MultiCache<K, V, R>
where
    K: Eq + Hash + Send + Sync + AsRef<str> + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    R: UniRedis,
{
    /// 创建对象
    ///
    /// Arguments:
    ///
    /// * `local_max_size`: 本地缓存最大数量
    /// * `local_expire`: 本地缓存存活时间
    /// * `redis_prefix`: redis缓存前缀
    /// * `redis_expire`: redis缓存过期时间
    /// * `use_compress`: 是否使用压缩
    ///
    pub fn new(
        local_max_size: u64,
        local_expire: Duration,
        redis_cache: R,
        redis_prefix: String,
        redis_expire: Duration,
    ) -> Self {
        let mut cache_builder = Cache::<K, V>::builder().max_capacity(local_max_size);
        if local_expire.as_secs() > 0 {
            cache_builder = cache_builder.time_to_idle(local_expire);
        }

        Self {
            memory_cache: cache_builder.build(),
            redis_cache,
            redis_prefix,
            redis_expire,
        }
    }

    /// 使用builder模式创建对象
    pub fn builder() -> Builder<R> {
        Builder::new()
    }

    /// 读取缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    ///
    pub async fn get(&self, key: &K) -> Option<V> {
        self.get_with(key, false).await
    }

    /// 读取缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    /// * `use_decompress`: 是否进行解压缩
    ///
    pub async fn get_with(&self, key: &K, use_lz4: bool) -> Option<V> {
        let mut value = self.memory_cache.get(key);
        if value.is_none() {
            let key = self.redis_key(key.as_ref());
            value = self.redis_cache.get_json(&key, use_lz4).await
        }
        value
    }

    /// 写入缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    /// * `value`: 缓存值
    ///
    pub async fn set(&self, key: K, value: V) {
        self.set_with(key, value, false).await
    }

    /// 写入缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    /// * `value`: 缓存值
    /// * `use_compress`: 是否进行压缩
    ///
    pub async fn set_with(&self, key: K, value: V, use_lz4: bool) {
        let redis_key = self.redis_key(key.as_ref());
        self.redis_cache.set_json_ex(&redis_key, &value, self.redis_expire.as_secs(), use_lz4).await;
        self.memory_cache.insert(key, value);
    }

    /// 删除缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    ///
    pub async fn del(&self, key: &K) {
        self.memory_cache.invalidate(key);
        let key = self.redis_key(key.as_ref());
        self.redis_cache.del(key.as_str()).await;
    }

    /// 清除缓存, 包括内存缓存及redis缓存
    pub async fn clear(&self) {
        self.memory_cache.invalidate_all();
        let redis_key = self.redis_key("*");
        self.redis_cache.pdel(&redis_key).await;
    }

    pub fn mem_iter(&self) -> Iter<K, V, RandomState> {
        self.memory_cache.iter()
    }

    fn redis_key(&self, key: &str) -> String {
        let mut result = self.redis_prefix.clone();
        if !self.redis_prefix.is_empty() {
            result.push(':');
        }
        result.push_str(key);
        result
    }

}

impl<K, R> MultiCache<K, Arc<String>, R>
where
    K: Eq + Hash + Send + Sync + AsRef<str> + 'static,
    R: UniRedis,
{
    /// 读取缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    ///
    pub async fn get_str(&self, key: &K) -> Option<Arc<String>> {
        let mut value = self.memory_cache.get(key);
        if value.is_none() {
            let key = self.redis_key(key.as_ref());
            value = self.redis_cache.get(&key).await;
        }
        value
    }

    /// 写入缓存项
    ///
    /// Arguments:
    ///
    /// * `key`: 缓存键
    /// * `value`: 缓存值
    /// * `use_compress`: 是否进行压缩
    ///
    pub async fn set_str(&self, key: K, value: Arc<String>) {
        let redis_key = self.redis_key(key.as_ref());
        self.redis_cache.set_ex(&redis_key, &value, self.redis_expire.as_secs()).await;
        self.memory_cache.insert(key, value);
    }

}


impl<R> Builder<R> {
    pub fn new() -> Self {
        Self {
            local_max_size: 0,
            local_expire: Duration::ZERO,
            redis_cache: None,
            redis_prefix: String::new(),
            redis_expire: Duration::ZERO,
        }
    }

    pub fn build<K, V>(self) -> Result<MultiCache<K, V, R>>
    where
        K: Eq + Hash + Send + Sync + AsRef<str> + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        R: UniRedis + Default,
    {
        if self.local_max_size == 0 {
            bail!("local_max_size must be greater than 0");
        }
        if self.redis_expire.as_secs() == 0 {
            bail!("redis_expire must be greater than 0");
        }

        Ok(MultiCache::new(
            self.local_max_size,
            self.local_expire,
            self.redis_cache.unwrap(),
            self.redis_prefix,
            self.redis_expire,
        ))
    }

    /// 设置本地缓存的最大数量
    pub fn local_max_size(mut self, local_max_size: u64) -> Self {
        self.local_max_size = local_max_size;
        self
    }

    /// 设置本地缓存的存活时间
    pub fn local_expire(mut self, local_expire: Duration) -> Self {
        self.local_expire = local_expire;
        self
    }

    /// 设置redis缓存的前缀
    pub fn redis_cache(mut self, redis_cache: R) -> Self {
        self.redis_cache = Some(redis_cache);
        self
    }

    /// 设置redis缓存的前缀
    pub fn redis_prefix(mut self, redis_prefix: String) -> Self {
        self.redis_prefix = redis_prefix;
        self
    }

    /// 设置redis缓存的过期时间
    pub fn redis_expire(mut self, redis_expire: Duration) -> Self {
        self.redis_expire = redis_expire;
        self
    }
}
