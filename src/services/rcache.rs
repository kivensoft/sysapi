//! redis 缓存服务
use crate::AppConf;
use anyhow::Result;
use deadpool_redis::{redis::{self, FromRedisValue, Cmd}, Config, Connection, Pool, Runtime};

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
            Some(pool) => {
                match pool.get().await {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        log::error!("redis get connection error: {e:?}");
                        Err(anyhow::anyhow!("系统内部错误, 连接缓存服务失败"))
                    }
                }
            },
            _ => std::hint::unreachable_unchecked(),
        }
    }
}

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
pub async fn get(key: &str) -> Result<Option<String>> {
    query_async(&Cmd::get(key)).await
}

/// 设置key, value, secs指定存活时间
pub async fn set(key: &str, value: &str, ttl: usize) -> Result<()> {
    query_async(&Cmd::set_ex(key, value, ttl)).await
}

/// 删除key，返回删除数量
#[allow(dead_code)]
pub async fn del(keys: &[String]) -> Result<u64> {
    query_async(&Cmd::del(keys)).await
}

/// 自增，返回自增后的值
pub async fn incr(key: &str, secs: usize) -> Result<u64> {
    let mut conn = get_conn().await?;
    let ret: u64 = query_async2(&mut conn, &Cmd::incr(key, 1)).await?;
    if secs > 0 {
        query_async2(&mut conn, &Cmd::expire(key, secs)).await?;
    }
    Ok(ret)
}

/// 获取key的当前存活时间，秒为单位
pub async fn ttl(key: &str) -> Result<i32> {
    query_async(&Cmd::ttl(key)).await
}

/// 设置key的存活时间，秒为单位
#[allow(dead_code)]
pub async fn expire(key: &str, secs: usize) -> Result<bool> {
    let ret: u32 = query_async(&Cmd::expire(key, secs)).await?;
    Ok(ret == 1)
}

/// 查询键
pub async fn keys(key: &str) -> Result<Vec<String>> {
    query_async(&Cmd::keys(key)).await
}

/// 发布消息
#[allow(dead_code)]
pub async fn publish(channel: &str, message: &str) -> Result<()> {
    query_async(&Cmd::publish(channel, message)).await?;
    Ok(())
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
        anyhow::bail!(format!("can't connect redis server: {url}"));
    }

    Ok(())
}

async fn query_async<T: FromRedisValue>(cmd: &Cmd) -> Result<T> {
    let mut conn = get_conn().await?;
    query_async2(&mut conn, cmd).await
}

async fn query_async2<T: FromRedisValue>(conn: &mut Connection, cmd: &Cmd) -> Result<T> {
    match cmd.query_async(conn).await {
        Ok(v) => Ok(v),
        Err(e) => {
            log::error!("redis query async error: {e:?}");
            anyhow::bail!("系统内部错误, 缓存操作失败")
        }
    }
}
