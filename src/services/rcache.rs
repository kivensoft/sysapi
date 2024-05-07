//! redis 缓存服务
use crate::AppConf;
use anyhow_ext::{Context, Result};
use deadpool_redis::{
    redis::{self, Cmd, FromRedisValue, ToRedisArgs},
    Config, Connection, Pool, Runtime,
};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use serde::{de::DeserializeOwned, Serialize};

#[allow(dead_code)]
pub const TTL_NOT_EXISTS: i32 = -2;
#[allow(dead_code)]
pub const TTL_NOT_EXPIRE: i32 = -1;
/// 默认缓存过期时间(单位: 秒)
pub const DEFAULT_TTL: u32 = 300;
pub const CK_LOGIN_FAIL: &str = "loginFail";
pub const CK_INVALID_TOKEN: &str = "invalidToken";
pub const CK_MOBILE_AUTH_CODE: &str = "mobileAuthCode";
pub const CK_EMAIL_AUTH_CODE: &str = "emailAuthCode";
pub const CK_MENUS: &str = "menus";

static mut CACHE_POOL: Option<Pool> = None;

/// 从缓冲池中获取一个redis客户端连接
pub async fn get_conn() -> Result<Connection> {
    unsafe {
        debug_assert!(CACHE_POOL.is_some());
        match &CACHE_POOL {
            Some(pool) => pool.get().await.context("redis获取连接异常"),
            _ => std::hint::unreachable_unchecked(),
        }
    }
}

/// 初始化redis连接池
pub fn init_pool(ac: &AppConf) -> Result<()> {
    debug_assert!(unsafe { CACHE_POOL.is_none() });

    let url = gen_url(ac);

    // 测试redis连接配置的正确性
    try_connect(&url)?;

    let cfg = Config::from_url(url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    unsafe {
        CACHE_POOL = Some(pool);
    }

    Ok(())
}

/// 获取key对应的value, 返回原始的字符串值(不做转换)
pub async fn get<T: FromRedisValue>(key: &str) -> Option<T> {
    match query_async(&Cmd::get(key)).await {
        Ok(v) => v,
        Err(e) => {
            log::error!("查询redis缓存异常: {e:?}");
            None
        }
    }
}

/// 设置key, value, secs指定存活时间
pub async fn set<T: ToRedisArgs>(key: &str, value: T, ttl: u64) {
    if let Err(e) = query_async::<()>(&Cmd::set_ex(key, value, ttl)).await {
        log::error!("保存redis缓存异常: {e:?}");
    }
}

/// 获取key对应的value并进行解压缩, 返回原始的字符串值(不做转换)
#[allow(dead_code)]
pub async fn lz4_get(key: &str) -> Option<Vec<u8>> {
    if let Some(v) = get::<Vec<u8>>(key).await {
        match decompress_size_prepended(&v) {
            Ok(v) => return Some(v),
            Err(e) => log::error!("lz4解压缩异常: {e:?}"),
        }
    }
    None
}

/// 压缩value并进行key, value保存, secs指定存活时间
#[allow(dead_code)]
pub async fn lz4_set(key: &str, value: &[u8], ttl: u64) {
    set(key, compress_prepend_size(value), ttl).await
}

/// 获取key对应的value并进行json解码
#[allow(dead_code)]
pub async fn json_get<T: DeserializeOwned>(key: &str) -> Option<T> {
    if let Some(v) = get::<Vec<u8>>(key).await {
        match serde_json::from_slice(&v) {
            Ok(v) => return Some(v),
            Err(e) => log::error!("json解码异常: {e:?}"),
        }
    }
    None
}

/// 设置key, value, secs指定存活时间
#[allow(dead_code)]
pub async fn json_set<T: Serialize>(key: &str, value: &T, ttl: u64) {
    match serde_json::to_vec(&value) {
        Ok(v) => set(key, v, ttl).await,
        Err(e) => log::error!("json编码异常: {e:?}"),
    }
}

/// 获取key对应的value并进行解压缩, 返回原始的字符串值(不做转换)
#[allow(dead_code)]
pub async fn json_lz4_get<T: DeserializeOwned>(key: &str) -> Option<T> {
    if let Some(v) = get::<Vec<u8>>(key).await {
        match decompress_size_prepended(&v) {
            Ok(v) => match serde_json::from_slice(&v) {
                Ok(v) => return Some(v),
                Err(e) => log::error!("json解码失败: {e:?}"),
            },
            Err(e) => log::error!("lz4解压缩异常: {e:?}"),
        }
    }
    None
}

/// 压缩value并进行key, value保存, secs指定存活时间
#[allow(dead_code)]
pub async fn json_lz4_set<T: Serialize>(key: &str, value: &T, ttl: u64) {
    match serde_json::to_vec(&value) {
        Ok(v) => set(key, compress_prepend_size(&v), ttl).await,
        Err(e) => log::error!("json编码异常: {e:?}"),
    }
}

/// 删除key，返回删除数量
#[allow(dead_code)]
pub async fn del<T: ToRedisArgs>(keys: T) -> Option<u64> {
    match query_async(&Cmd::del(keys)).await {
        Ok(n) => n,
        Err(e) => {
            log::error!("redis删除缓存项异常: {e:?}");
            None
        }
    }
}

/// 自增，返回自增后的值
///
/// Arguments
/// * `key`: 键
/// * `secs`: 缓存超时时间, 为0时，不设置超时时间
///
pub async fn incr(key: &str, secs: i64) -> u64 {
    match get_conn().await {
        Ok(mut conn) => match query_async_with(&mut conn, &Cmd::incr(key, 1)).await {
            Ok(n) => {
                if secs > 0 {
                    match query_async_with::<()>(&mut conn, &Cmd::expire(key, secs)).await {
                        Ok(_) => return n,
                        Err(e) => log::error!("redis设置缓存项超时时间异常: {e:?}"),
                    }
                } else {
                    return n;
                }
            }
            Err(e) => log::error!("redis获取自增缓存项异常: {e:?}"),
        },
        Err(e) => log::error!("redis获取缓存连接异常: {e:?}"),
    }

    0
}

/// 获取key的当前存活时间，秒为单位
pub async fn ttl(key: &str) -> i32 {
    match query_async(&Cmd::ttl(key)).await {
        Ok(n) => n,
        Err(e) => {
            log::error!("redis设置缓存项存活时间异常: {e:?}");
            -2
        }
    }
}

/// 设置key的存活时间，秒为单位
#[allow(dead_code)]
pub async fn expire(key: &str, secs: i64) -> bool {
    match query_async::<u32>(&Cmd::expire(key, secs)).await {
        Ok(n) => n == 1,
        Err(e) => {
            log::error!("redis设置缓存存活时间异常: {e:?}");
            false
        }
    }
}

/// 查询键
pub async fn keys(key: &str) -> Option<Vec<String>> {
    match query_async(&Cmd::keys(key)).await {
        Ok(v) => Some(v),
        Err(e) => {
            log::error!("redis查询键异常: {e:?}");
            None
        }
    }
}

/// 发布消息
#[allow(dead_code)]
pub async fn publish(channel: &str, message: &str) {
    if let Err(e) = query_async::<()>(&Cmd::publish(channel, message)).await {
        log::error!("redis发布消息异常: {e:?}");
    }
}

fn gen_url(ac: &AppConf) -> String {
    format!(
        "redis://{}:{}@{}:{}/{}",
        ac.cache_user, ac.cache_pass, ac.cache_host, ac.cache_port, ac.cache_name
    )
}

// redis连接测试
fn try_connect(url: &str) -> Result<()> {
    let c = redis::Client::open(url)?;
    let mut conn = c.get_connection()?;
    let val: String = redis::cmd("PING").arg(crate::APP_NAME).query(&mut conn)?;
    if val != crate::APP_NAME {
        anyhow_ext::bail!(format!("can't connect redis server: {url}"));
    }

    Ok(())
}

async fn query_async<T: FromRedisValue>(cmd: &Cmd) -> Result<T> {
    let mut conn = get_conn().await?;
    query_async_with(&mut conn, cmd).await
}

async fn query_async_with<T: FromRedisValue>(conn: &mut Connection, cmd: &Cmd) -> Result<T> {
    cmd.query_async(conn).await.context("执行redis命令失败")
}
