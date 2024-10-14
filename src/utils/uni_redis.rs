use deadpool_redis::redis::{FromRedisValue, ToRedisArgs};

pub use deadpool_redis::redis::Cmd;
use serde::{de::DeserializeOwned, Serialize};

#[allow(dead_code)]
#[async_trait::async_trait]
pub trait UniRedis {
    /// 生成redis的key
    fn make_key(&self, key: &str) -> String;

    /// 执行命令
    async fn cmd<T: FromRedisValue>(&self, cmd: &Cmd) -> Option<T>;

    /// 获取key对应的value, 返回原始的字符串值(不做转换)
    async fn get<T: FromRedisValue>(&self, key: &str) -> Option<T>;

    /// 设置key, value
    async fn set<T: ToRedisArgs + Send>(&self, key: &str, value: T);

    /// 设置key, value, secs指定存活时间
    async fn set_ex<T: ToRedisArgs + Send>(&self, key: &str, value: T, ttl_secs: u64);

    /// 删除key，返回删除数量
    async fn del<T: ToRedisArgs + Send>(&self, keys: T) -> Option<u64>;

    /// 批量删除匹配的key，使用scan
    async fn pdel(&self, pattern: &str) -> Option<u64>;

    /// 设置key的存活时间，秒为单位
    async fn expire(&self, key: &str, secs: i64) -> bool;

    /// 查询key的剩余存活时间
    async fn ttl(&self, key: &str) -> Option<i64>;

    /// 查询键
    async fn keys(&self, key: &str) -> Option<Vec<String>>;

    /// 递增指定键，当键不存在时，先用0创建，然后再递增返回
    async fn incr(&self, key: &str, delta: u64) -> Option<u64>;

    async fn get_lz4(&self, key: &str) -> Option<Vec<u8>>;

    async fn set_lz4(&self, key: &str, value: &[u8]);

    async fn set_lz4_ex(&self, key: &str, value: &[u8], ttl_secs: u64);

    async fn get_json<T: DeserializeOwned>(&self, key: &str, use_lz4: bool) -> Option<T>;

    async fn set_json<T: Serialize + Sync>(&self, key: &str, value: &T, use_lz4: bool);

    async fn set_json_ex<T: Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
        ttl_secs: u64,
        use_lz4: bool,
    );

    async fn publish(&self, chan: &str, msg: &str);

    async fn pub_json<T: Serialize + Sync>(&self, chan: &str, msg: &T);
}
