//! redis 缓存服务
use std::mem::MaybeUninit;

use anyhow_ext::{Context, Result};
use deadpool_redis::{
    redis::{self, Cmd, FromRedisValue, ToRedisArgs},
    Config, Connection, Pool, Runtime,
};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use serde::{de::DeserializeOwned, Serialize};

use crate::utils::uni_redis::UniRedis;

pub const TTL_NOT_EXISTS: i64 = -2;
pub const TTL_NOT_EXPIRE: i64 = -1;
/// 默认缓存过期时间(单位: 秒)
pub const DEFAULT_TTL: u32 = 600;
// pub const CK_LOGIN_FAIL: &str = "login:fail";
pub const CK_INVALID_TOKEN: &str = "invalidToken";
pub const CK_MOBILE_AUTH_CODE: &str = "mobileAuthCode";
pub const CK_EMAIL_AUTH_CODE: &str = "emailAuthCode";
pub const CK_MENUS: &str = "sys:menus";

static mut POOL: MaybeUninit<Pool> = MaybeUninit::uninit();
#[cfg(debug_assertions)]
static mut POOL_INITED: bool = false;

pub struct RedisConfig<'a> {
    pub host: &'a str,
    pub port: &'a str,
    pub user: &'a str,
    pub pass: &'a str,
    pub db: &'a str,
}

#[derive(Clone)]
pub struct UniRedisImpl {
    root_key: String,
    def_ttl: u64,
}

impl UniRedisImpl {
    pub fn new(root_key: &str, def_ttl: u64) -> Self {
        UniRedisImpl {
            root_key: String::from(root_key),
            def_ttl,
        }
    }
}

#[async_trait::async_trait]
impl UniRedis for UniRedisImpl {
    fn make_key(&self, key: &str) -> String {
        let mut redis_key = self.root_key.clone();
        if !self.root_key.is_empty() {
            redis_key.push(':');
        }
        redis_key.push_str(key);
        redis_key
    }

    async fn cmd<T: FromRedisValue>(&self, c: &Cmd) -> Option<T> {
        cmd(c).await
    }

    async fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        get(key).await
    }

    async fn set<T: ToRedisArgs + Send>(&self, key: &str, value: T) {
        set(key, value, self.def_ttl).await
    }

    async fn set_ex<T: ToRedisArgs + Send>(&self, key: &str, value: T, ttl_secs: u64) {
        set(key, value, ttl_secs).await
    }

    async fn del<T: ToRedisArgs + Send>(&self, keys: T) -> Option<u64> {
        del(keys).await
    }

    async fn pdel(&self, key: &str) -> Option<u64> {
        pdel(key).await
    }

    async fn expire(&self, key: &str, ttl_secs: i64) -> bool {
        expire(key, ttl_secs).await
    }

    async fn ttl(&self, key: &str) -> Option<i64> {
        ttl(key).await
    }

    async fn keys(&self, key: &str) -> Option<Vec<String>> {
        keys(key).await
    }

    async fn incr(&self, key: &str, delta: u64) -> Option<u64> {
        incr(key, delta).await
    }

    async fn get_lz4(&self, key: &str) -> Option<Vec<u8>> {
        get_lz4(key).await
    }

    async fn set_lz4(&self, key: &str, value: &[u8]) {
        set_lz4(key, value, self.def_ttl).await
    }

    async fn set_lz4_ex(&self, key: &str, value: &[u8], ttl_secs: u64) {
        set_lz4(key, value, ttl_secs).await
    }

    async fn get_json<T: DeserializeOwned>(&self, key: &str, use_lz4: bool) -> Option<T> {
        get_json(key, use_lz4).await
    }

    async fn set_json<T: Serialize + Sync>(&self, key: &str, value: &T, use_lz4: bool) {
        set_json(key, value, self.def_ttl, use_lz4).await
    }

    async fn set_json_ex<T: Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
        ttl_secs: u64,
        use_lz4: bool,
    ) {
        set_json(key, value, ttl_secs, use_lz4).await
    }

    async fn publish(&self, chan: &str, msg: &str) {
        publish(chan, msg).await
    }

    async fn pub_json<T: Serialize + Sync>(&self, chan: &str, msg: &T) {
        pub_json(chan, msg).await
    }
}

/// 初始化redis连接池
pub fn init(url: String) -> Result<()> {
    // 测试redis连接配置的正确性
    try_connect(&url)?;

    let cfg = Config::from_url(url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;

    unsafe {
        #[cfg(debug_assertions)]
        {
            if POOL_INITED {
                panic!("redis pool already inited");
            }
            POOL_INITED = true;
        }
        POOL.write(pool);
    }

    Ok(())
}

/// 从缓冲池中获取一个redis客户端连接
pub async fn get_conn() -> Result<Connection> {
    unsafe {
        #[cfg(debug_assertions)]
        if !POOL_INITED {
            panic!("redis pool not inited");
        }
        POOL.assume_init_ref()
            .get()
            .await
            .context("redis获取连接异常")
    }
}

fn get_pool() -> &'static Pool {
    unsafe {
        #[cfg(debug_assertions)]
        if !POOL_INITED {
            panic!("redis pool not inited");
        }
        POOL.assume_init_ref()
    }
}

// redis连接测试
pub fn try_connect(url: &str) -> Result<()> {
    let c = redis::Client::open(url)?;
    let mut conn = c.get_connection()?;
    let val: String = redis::cmd("PING").query(&mut conn)?;
    if val != "PONG" {
        anyhow_ext::bail!(format!("can't connect redis server: {url}"));
    }

    Ok(())
}

async fn query_async<T: FromRedisValue>(cmd: &Cmd) -> Result<T> {
    let mut conn = get_conn().await.context("获取redis连接失败").dot()?;
    cmd.query_async(&mut conn).await.context("执行redis命令失败").dot()
}

impl<'a> RedisConfig<'a> {
    pub fn build_url(self) -> String {
        let port = self.port.parse().unwrap_or(6379);
        let db = self.db.parse().unwrap_or(0);

        format!(
            "redis://{}:{}@{}:{}/{}",
            self.user, self.pass, self.host, port, db
        )
    }
}

/// 执行自定义命令
pub async fn cmd<T: FromRedisValue>(cmd: &Cmd) -> Option<T> {
    match query_async(cmd).await.dot() {
        Ok(v) => v,
        Err(e) => {
            log::error!("{e:?}");
            None
        }
    }
}

pub async fn get<T: FromRedisValue>(key: &str) -> Option<T> {
    match query_async(&Cmd::get(key)).await.dot() {
        Ok(v) => v,
        Err(e) => {
            log::error!("redis查询缓存{key}失败: {e:?}");
            None
        }
    }
}

pub async fn set<T: ToRedisArgs + Send>(key: &str, value: T, ttl_secs: u64) {
    if let Err(e) = query_async::<()>(&Cmd::set_ex(key, value, ttl_secs)).await.dot() {
        log::error!("redis设置缓存{key}失败: {e:?}");
    }
}

pub async fn del<T: ToRedisArgs + Send>(keys: T) -> Option<u64> {
    match query_async(&Cmd::del(keys)).await.dot() {
        Ok(n) => n,
        Err(e) => {
            log::error!("redis删除缓存项异常: {e:?}");
            None
        }
    }
}

pub async fn pdel(key: &str) -> Option<u64> {
    let mut cursor: u64 = 0;
    let mut total = 0;
    loop {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor).arg("MATCH").arg(key).arg("COUNT").arg(1024);

        let (c, ret_keys): (u64, Vec<String>) = match query_async(&cmd).await.dot() {
            Ok(v) => v,
            Err(e) => {
                log::error!("执行redis scan命令错误: {e:?}");
                return None;
            }
        };

        if !ret_keys.is_empty() {
            match del(&ret_keys).await {
                Some(n) => total += n,
                None => return None,
            }
        }

        if c == 0 {
            break Some(total);
        }
        cursor = c;
    }
}

pub async fn expire(key: &str, ttl_secs: i64) -> bool {
    match query_async::<u64>(&Cmd::expire(key, ttl_secs)).await.dot() {
        Ok(n) => n == 1,
        Err(e) => {
            log::error!("redis设置缓存存活时间异常: {e:?}");
            false
        }
    }
}

pub async fn ttl(key: &str) -> Option<i64> {
    match query_async::<i64>(&Cmd::ttl(key)).await.dot() {
        Ok(v) => {
            if v < 0 {
                None
            } else {
                Some(v)
            }
        }
        Err(e) => {
            log::error!("redis查询键异常: {e:?}");
            None
        }
    }
}

pub async fn keys(key: &str) -> Option<Vec<String>> {
    match query_async(&Cmd::keys(key)).await.dot() {
        Ok(v) => Some(v),
        Err(e) => {
            log::error!("redis查询键异常: {e:?}");
            None
        }
    }
}

pub async fn incr(key: &str, delta: u64) -> Option<u64> {
    match query_async(&Cmd::incr(key, delta)).await.dot() {
        Ok(v) => v,
        Err(e) => {
            log::error!("redis查询键异常: {e:?}");
            None
        }
    }
}

pub async fn get_lz4(key: &str) -> Option<Vec<u8>> {
    match query_async::<Option<Vec<u8>>>(&Cmd::get(key)).await.dot() {
        Ok(opt_vec) => {
            if let Some(lz4_vec) = &opt_vec {
                match decompress_size_prepended(lz4_vec) {
                    Ok(vec_data) => return Some(vec_data),
                    Err(e) => log::error!("lz4解压缩异常: {e:?}"),
                }
            }
        }
        Err(e) => log::error!("redis查询缓存{key}失败: {e:?}"),
    }
    None
}

pub async fn set_lz4(key: &str, value: &[u8], ttl_secs: u64) {
    let value = compress_prepend_size(value);
    if let Err(e) = query_async::<()>(&Cmd::set_ex(key, value, ttl_secs)).await.dot() {
        log::error!("redis设置缓存{key}失败: {e:?}");
    }
}

pub async fn get_json<T: DeserializeOwned>(key: &str, use_lz4: bool) -> Option<T> {
    match query_async::<Vec<u8>>(&Cmd::get(key)).await.dot() {
        Ok(mut value_vec) => {
            if use_lz4 {
                match decompress_size_prepended(&value_vec) {
                    Ok(v) => value_vec = v,
                    Err(e) => {
                        log::error!("redis解压缩错误: {e:?}");
                        return None;
                    }
                }
            }
            match serde_json::from_slice(&value_vec) {
                Ok(v) => return Some(v),
                Err(e) => log::error!("json解码异常: {e:?}"),
            }
        }
        Err(e) => {
            log::error!("redis查询缓存{key}失败: {e:?}");
        }
    }
    None
}

pub async fn set_json<T: Serialize + Sync>(key: &str, value: &T, ttl_secs: u64, use_lz4: bool) {
    match serde_json::to_vec(value).dot() {
        Ok(mut value_vec) => {
            if use_lz4 {
                value_vec = compress_prepend_size(&value_vec);
            }
            let cmd = Cmd::set_ex(key, &value_vec, ttl_secs);
            if let Err(e) = query_async::<()>(&cmd).await.dot() {
                log::error!("redis设置缓存{key}失败: {e:?}");
            }
        }
        Err(e) => log::error!("json编码失败: {e:?}"),
    };
}

/// 发布消息
pub async fn publish(chan: &str, msg: &str) {
    if let Err(e) = query_async::<()>(&Cmd::publish(chan, msg)).await {
        log::error!("redis发布消息异常: {e:?}");
    }
}

pub async fn pub_json<T: Serialize + Sync>(chan: &str, msg: &T) {
    match serde_json::to_string(msg).dot() {
        Ok(value) => publish(chan, &value).await,
        Err(e) => log::error!("json编码失败: {e:?}"),
    }
}
