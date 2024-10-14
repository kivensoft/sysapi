//! 权限校验中间件
use anyhow_ext::{anyhow, Context, Result};
use arc_swap::ArcSwap;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use fnv::FnvBuildHasher;
use httpserver::{
    log_debug, log_error, log_trace, log_warn, HttpContext, HttpResponse, Next, Resp,
};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::{Duration, SystemTime}};

type Cache<K, V> = mini_moka::sync::Cache<K, V, FnvBuildHasher>;
type MCache = MultiCache<String, TokenCacheItem, UniRedisImpl>;
type PermitValue = Arc<Vec<i16>>;
type ArcMap<K, V> = ArcSwap<HashMap<K, V>>;

pub struct Authentication {
    content_path: String,                            // 上下文路径
    jwt_data: Option<JwtData>,                              // 令牌校验参数
    token_cache: MCache,                                    // jwt验证解码缓存
    role_permits_map: ArcMap<u32, String>,           // 角色权限缓存
    path_permits_map: ArcMap<String, PermitValue>,   // 接口权限缓存
    user_cache: Cache<u32, (u32, i64)>,                     // 用户/角色/更新时间映射缓存
    path_permits_cache: Cache<String, PermitValue>,  // 实际访问路径的权限缓存
}

#[derive(Clone, Serialize, Deserialize)]
struct TokenCacheItem {
    uid: String,
    exp: u64,
}

struct JwtData {
    key: String, // jwt密钥
    iss: String, // jwt发布者
}

pub const ACCESS_TOKEN: &str = "access_token";
pub const COOKIE_NAME: &str = "Cookie";

const TOKEN_VERIFIED: &str = "Token-Verified";
const UID_NAME: &str = "uid";
const USER_ROLE_CACHE_SIZE: u64 = 256; //USER_ROLE_CACHE的缓存大小
const PATH_CACHE_SIZE: u64 = 256; //PATH_CACHE的缓存大小

#[async_trait::async_trait]
impl httpserver::HttpMiddleware for Authentication {
    async fn handle<'a>(&'a self, mut ctx: HttpContext, next: Next<'a>) -> HttpResponse {
        use hyper::StatusCode;

        let (uid, opt_err) = match self.parse_user_id(&ctx).await {
            Ok(uid) => (uid, None),
            Err(e) => (String::with_capacity(0), Some(e)),
        };
        if !uid.is_empty() {
            ctx.uid.push_str(&uid);
        }

        // 权限校验通过，执行下一步
        if self.auth(ctx.id, &uid, ctx.req.uri().path()).await {
            next.run(ctx).await
        } else {
            if let Some(e) = opt_err {
                if e.to_string() == "Incorrect exp" {
                    return Resp::fail_with_status(StatusCode::UNAUTHORIZED, 401, "Unauthorized");
                }
                log_error!(ctx.id, "令牌格式错误: {e:?}");
                return Resp::fail("令牌格式错误");
            }

            if uid.is_empty() {
                log_debug!(
                    ctx.id,
                    "权限校验失败[{}], 用户尚未登录",
                    ctx.req.uri().path()
                );
                Resp::fail_with_status(StatusCode::UNAUTHORIZED, 401, "Unauthorized")
            } else {
                log_debug!(
                    ctx.id,
                    "权限校验失败[{}], 当前用户没有访问权限",
                    ctx.req.uri().path()
                );
                Resp::fail_with_status(StatusCode::FORBIDDEN, 403, "Forbidden")
            }
        }
    }
}

impl Authentication {
    pub async fn new(content_path: &str, jwt_key: &str, jwt_iss: &str,
        local_cache_size: u64, local_cache_expire: Duration,
        redis_prefix: String, redis_expire_secs: u64
    ) -> Result<Self> {
        let jwt_data = if jwt_key.is_empty() {
            None
        } else {
            Some(JwtData {
                key: jwt_key.to_string(),
                iss: jwt_iss.to_string(),
            })
        };

        let token_cache = MultiCache::new(
            local_cache_size,
            local_cache_expire,
            UniRedisImpl::new(&redis_prefix, redis_expire_secs),
            redis_prefix,
            Duration::from_secs(redis_expire_secs));

        let path_cache = mini_moka::sync::Cache::builder()
            .max_capacity(PATH_CACHE_SIZE)
            .time_to_idle(local_cache_expire)
            .build_with_hasher(FnvBuildHasher::default());

        let user_cache = mini_moka::sync::Cache::builder()
            .max_capacity(USER_ROLE_CACHE_SIZE)
            .time_to_idle(local_cache_expire)
            .build_with_hasher(FnvBuildHasher::default());

        let result = Authentication {
            content_path: String::from(content_path),
            jwt_data,
            token_cache,
            role_permits_map: ArcSwap::from_pointee(load_roles().await.dot()?),
            path_permits_map: ArcSwap::from_pointee(load_permits().await.dot()?),
            user_cache,
            path_permits_cache: path_cache,
        };

        Ok(result)
    }

    /// 权限校验, 返回true表示有权访问, false表示无权访问
    pub async fn auth(&self, req_id: u32, uid: &str, mut path: &str) -> bool {
        let uid: u32 = if uid.is_empty() {
            0
        } else {
            match uid.parse() {
                Ok(n) => n,
                Err(_) => {
                    log_error!(req_id, "令牌中的uid格式不是数字格式");
                    return false;
                }
            }
        };

        // 忽略路径中的上下文("/api")开头部分
        let cp = &self.content_path;
        if !cp.is_empty() && !path.starts_with(cp.as_str()) {
            log_warn!(req_id, "校验权限失败, 请求路径: {}", path);
            return false;
        }

        path = &path[self.content_path.len()..];
        let orig_path = String::from(path);
        log_trace!(req_id, "权限校验路径: {}", path);

        let user_permits = self.get_user_permits(uid).await;

        // 优先从缓存中读取，加快匹配速度
        if let Some(ps) = self.path_permits_cache.get(&orig_path) {
            log_trace!(req_id, "在缓存中找到匹配路径: {}", orig_path);
            return Self::check_permit(uid, &user_permits, &ps);
        }

        // 末尾的'/'不参与匹配
        let path_bytes = path.as_bytes();
        let mut end_pos = path_bytes.len();
        if end_pos > 1 && path_bytes[end_pos - 1] == b'/' {
            end_pos -= 1;
        }

        let permits = self.path_permits_map.load();
        // 递归每个父路径进行权限匹配, 有权限访问则直接返回true, 否则继续循环
        loop {
            path = &path[..end_pos];

            if let Some(ps) = permits.get(path) {
                log_trace!(req_id, "找到匹配路径: {}", path);
                self.path_permits_cache.insert(orig_path, ps.clone());
                return Self::check_permit(uid, &user_permits, ps);
            }

            // 找到上一级目录, 找不到退出循环
            end_pos = match path.rfind('/') {
                Some(n) if n > 0 => n,
                _ => break,
            };
        }

        // 路径都不匹配，尝试与根路径的权限匹配
        if let Some(ps) = permits.get("/") {
            return Self::check_permit(uid, &user_permits, ps);
        }

        false
    }

    /// 校验jwt token, 返回token携带的uid值, 获取失败返回空字符串
    pub async fn decode_token(&self, req_id: u32, token: &str) -> Result<String> {
        // 获取token的签名值
        let sign = match &jwt::get_sign(token) {
            Some(sign) => sign.to_string(),
            None => return Err(anyhow!("找不到签名部分")).dot(),
        };
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::new(0, 0))
            .as_secs();

        // 优先从缓存中读取签名，减少解密次数
        if let Some(cache_item) = self.token_cache.get(&sign).await {
            // 校验token过期时间
            if cache_item.exp >= now {
                log_trace!(req_id, "使用缓存校验token: {sign}");
                return Ok(cache_item.uid);
            } else {
                // 缓存项过期
                self.token_cache.del(&sign).await;
                log_trace!(req_id, "token过期: {sign}");
                return Err(anyhow!("Incorrect exp")).dot();
            }
        }

        // 如果使用了api网关，则jwt令牌已被网关校验过
        let claims = match &self.jwt_data {
            Some(jwt_data) => jwt::decode(token, &jwt_data.key, &jwt_data.iss).dot()?,
            None => Self::get_claims(token).dot()?,
        };

        let exp = jwt::get_exp(&claims).dot()?;

        if let Some(Value::String(uid)) = claims.get(UID_NAME) {
            log_trace!(req_id, "将token加入缓存: {}", sign);
            self.token_cache.set(sign, TokenCacheItem {uid: String::from(uid), exp}).await;
            return Ok(uid.to_string());
        }

        Err(anyhow!("找不到uid")).dot()
    }

    pub fn get_token_from_header(ctx: &HttpContext) -> Option<Cow<str>> {
        // 从请求头部获取Authorization字段
        if let Some(auth) = ctx.req.headers().get(jwt::AUTHORIZATION) {
            match auth.to_str() {
                Ok(auth) => {
                    // 判断是否以Bearer开头
                    if auth.len() > jwt::BEARER.len() && auth.starts_with(jwt::BEARER) {
                        return Some(Cow::Borrowed(&auth[jwt::BEARER.len()..]));
                    } else {
                        log_warn!(ctx.id, "请求头部的令牌格式错误: {auth}");
                    }
                }
                Err(e) => log_warn!(ctx.id, "请求头部的令牌值错误: {e:?}"),
            }
        }

        None
    }

    /// 启动数据变化监听服务, 在用户/角色/权限表变化时重新载入
    pub async fn start_listen(
        self: &Arc<Self>,
        role_chan: String,
        api_chan: String,
        user_chan: String,
        logout_chan: String) -> Result<()>
    {
        start_listen(self, role_chan, api_chan, user_chan, logout_chan).await
    }

    // 启动基于tokio的异步定时清理任务
    pub fn start_recycle_task(self: &Arc<Self>, task_interval: u64) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(task_interval));
        let that = self.clone();
        tokio::spawn(async move {
            interval.tick().await;
            loop {
                interval.tick().await;
                that.recycle().await;
            }
        });
    }

    async fn parse_user_id(&self, ctx: &HttpContext) -> Result<String> {
        let mut uid = String::new();

        // 获取token
        let token = match self.get_token(ctx) {
            Some(s) => s,
            None => return Ok(uid),
        };
        log_trace!(ctx.id, "token: {}", token);

        // 判断token是否有效（当用户退出登录后，token无效，会写入redis）
        if self.jwt_data.is_some() && get_invalid_token(&token).await {
            return Ok(uid);
        }

        let uid_str = self.decode_token(ctx.id, &token).await.dot()?;
        // 解码token成功，将登录用户id写入ctx上下文环境
        match uid_str.parse::<u32>() {
            Ok(uid_int) => {
                // 用户记录没有更新，token有效
                if !self.check_user_updated(uid_int, &token).await {
                    uid.push_str(&uid_str);
                    log_trace!(ctx.id, "uid: {}", uid_str);
                } else {
                    log_debug!(ctx.id, "用户信息已更新，token失效");
                }
                Ok(uid)
            }
            Err(e) => {
                log_error!(ctx.id, "token校验失败，uid格式错误: {uid_str}");
                Err(e.into())
            }
        }
    }

    /// 从jwt token中获取claims的内容并进行json反序列化
    fn get_claims(token: &str) -> Result<Value> {
        let mut iter = token.split('.');
        if iter.next().is_none() {
            return Err(anyhow!("令牌格式错误: 未找到签名部分")).dot();
        }

        if let Some(body) = iter.next() {
            let body_bs = URL_SAFE_NO_PAD.decode(body).dot()?;
            return serde_json::from_slice(&body_bs).dot();
        }

        Err(anyhow!("令牌格式错误: 找不到claims内容")).dot()
    }

    /// 从header/url/cookie中获取令牌
    fn get_token<'a>(&self, ctx: &'a HttpContext) -> Option<Cow<'a, str>> {
        // 启用api网关模式
        if self.jwt_data.is_none() {
            match ctx.req.headers().get(TOKEN_VERIFIED) {
                Some(flag) => {
                    let b = flag.as_bytes() == b"true";
                    log_trace!(ctx.id, "api网关校验令牌结果: {}", b);
                    if !b {
                        return None;
                    }
                }
                None => return None,
            }
        }

        Self::get_token_from_header(ctx).or_else(|| Self::get_access_token(ctx))
    }

    /// 从url参数中解析access_token
    fn get_access_token(ctx: &HttpContext) -> Option<Cow<str>> {
        // 优先从url中获取access_token参数
        if let Some(query) = ctx.req.uri().query() {
            let token = form_urlencoded::parse(query.as_bytes())
                .filter(|(k, _)| k.starts_with(ACCESS_TOKEN))
                .map(|(_, v)| v)
                .next();

            if token.is_some() {
                return token;
            };
        };

        // url中找不到, 尝试从cookie中获取access_token
        if let Some(cookie_str) = ctx.req.headers().get(COOKIE_NAME) {
            match cookie_str.to_str() {
                Ok(c_str) => {
                    for cookie in cookie::Cookie::split_parse_encoded(c_str) {
                        match cookie {
                            Ok(c) => if c.name() == ACCESS_TOKEN {
                                return Some(Cow::Owned(c.value().to_owned()));
                            }
                            Err(e) => log_warn!(ctx.id, "解析cookie值格式失败: {e:?}, cookie: {c_str}"),
                        }
                    }
                }
                Err(e) => log_warn!(ctx.id, "cookie值不是utf8格式: {e:?}"),
            };
        }

        None
    }

    /// 根据用户id加载用户权限
    async fn get_user_permits(&self, uid: u32) -> String {
        if uid != 0 {
            let rid = match self.user_cache.get(&uid) {
                Some((rid, _)) => rid,
                None => {
                    let rid_updated = get_role_and_updated(uid).await;
                    self.user_cache.insert(uid, rid_updated);
                    rid_updated.0
                }
            };

            if rid != 0 {
                if let Some(permits) =  self.role_permits_map.load().get(&rid) {
                    return permits.clone();
                }
            }
        }

        String::with_capacity(0)
    }

    /// 校验用户访问许可是否在给定的索引列表内
    fn check_permit(uid: u32, permits: &str, ps: &[i16]) -> bool {
        for i in ps {
            if *i == ANONYMOUS_CODE
                || (*i == PUBLIC_CODE && uid != 0)
                || crate::utils::bits::get(permits, *i as usize)
            {
                return true;
            }
        }

        false
    }

    // 删除缓存里过期的项，需要用户自行调用，避免占用独立的线程资源
    async fn recycle(&self) {
        log::trace!("执行token缓存清理任务...");

        let now = crate::utils::time::unix_timestamp();
        let ds: Vec<String> = self.token_cache.mem_iter()
            .filter(|v| v.value().exp < now)
            .map(|v| v.key().clone())
            .collect();
        let ds_count = ds.len();

        for k in &ds {
            self.token_cache.del(k).await;
            log::trace!("清理过期的token: {k}");
        }

        if ds_count > 0 {
            log::trace!("总计清理token过期项: {}", ds_count);
        } else {
            log::trace!("缓存清理完成，没有需要清理的缓存项");
        }
    }

    fn get_exp(token: &str) -> u64 {
        match jwt::decode(token, "", "") {
            Ok(claims) => match jwt::get_exp(&claims) {
                Ok(exp) => return exp,
                Err(e) => log::error!("获取token过期时间失败: {e:?}"),
            },
            Err(e) => log::error!("token解析失败: {e:?}"),
        };
        0
    }

    fn get_created(token: &str) -> i64 {
        match jwt::decode(token, "", "") {
            Ok(claims) => match claims.get("created") {
                Some(created) => match created.as_i64() {
                    Some(created) => return created,
                    None => log::error!("token解析失败，created字段不是i64类型"),
                },
                None => log::error!("token解析失败，找不到created字段"),
            },
            Err(e) => log::error!("token解析失败: {e:?}"),
        }
        0
    }

    /// 校验用户是否在token创建之后进行了更新, 返回true表示数据已更新
    async fn check_user_updated(&self, user_id: u32, token: &str) -> bool {
        let created = Self::get_created(token);
        if created == 0 {
            return true;
        }

        // 优先从缓存中读取最后更新时间
        if let Some((_, updated)) = self.user_cache.get(&user_id) {
            return updated >= created;
        }

        let (rid, updated) = get_role_and_updated(user_id).await;
        // 保存到缓存
        self.user_cache.insert(user_id, (rid, updated));
        if updated != 0 {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    "token用户更新校验: 用户id = {}, 令牌创建 = {}, 用户更新 = {}",
                    user_id,
                    LocalTime::from_unix_timestamp(created),
                    LocalTime::from_unix_timestamp(updated),
                );
            }
            return updated >= created;
        }
        false
    }
}

async fn get_role_and_updated(user_id: u32) -> (u32, i64) {
    use crate::entities::sys_user::SysUser;
    match SysUser::select_role_and_updated(user_id).await {
        Ok(r) => {
            if let Some((rid, updated)) = r {
                return (rid, updated.timestamp());
            }
        }
        Err(e) => log::error!("数据库查询出错: {e:?}"),
    }
    (0, 0)
}

/// 从数据库中加载角色信息表, 返回所有角色id与对应的权限
async fn load_roles() -> Result<HashMap<u32, String>> {
    use crate::entities::sys_role::SysRole;

    let mut roles = HashMap::new();
    let roles_data = SysRole::select_all().await.dot()?;

    for r in &roles_data {
        let rid = r.role_id.unwrap();
        let ps = r.permissions.as_ref().unwrap().as_str();
        roles.insert(rid, String::from(ps));
    }

    if log::log_enabled!(log::Level::Trace) {
        log::trace!("权限模块加载角色数据: {roles:?}");
    } else {
        log::debug!("权限模块加载角色数据成功!");
    }

    Ok(roles)
}

/// 从数据库中加载权限信息表, 返回所有路径对应的权限索引组
async fn load_permits() -> Result<HashMap<String, Arc<Vec<i16>>>> {
    use crate::entities::sys_api::SysApi;

    let mut permits = HashMap::new();
    let api_data = SysApi::select_all().await.dot()?;

    for a in &api_data {
        let pcode = a.permission_code.unwrap();
        let api_path = String::from(a.api_path.as_ref().unwrap());
        let v = permits.entry(api_path).or_insert_with(Vec::new);
        v.push(pcode);
    }

    if log::log_enabled!(log::Level::Trace) {
        log::trace!("权限模块加载权限数据: {permits:?}");
    } else {
        log::debug!("权限模块加载权限数据成功!");
    }

    Ok(permits.into_iter().map(|(k, v)| (k, Arc::new(v))).collect())
}

async fn start_listen(
    auth: &Arc<Authentication>,
    role_chan: String,
    api_chan: String,
    user_chan: String,
    logout_chan: String) -> Result<()>
{
    use crate::services::mq::{subscribe, Msg};

    let that = auth.clone();
    subscribe(role_chan, move |_| {
        let that = that.clone();
        async move {
            log::debug!("收到角色变动消息，正在重新加载角色数据...");
            that.role_permits_map.store(Arc::new(load_roles().await.dot()?));
            Ok(())
        }
    }).await.dot()?;

    let that = auth.clone();
    subscribe(api_chan, move |_| {
        let that = that.clone();
        async move {
            log::debug!("收到接口变动消息，正在重新加载权限数据...");
            that.path_permits_map.store(Arc::new(load_permits().await.dot()?));
            that.path_permits_cache.invalidate_all();
            Ok(())
        }
    }).await.dot()?;

    let that = auth.clone();
    subscribe(user_chan.clone(), move |msg: Arc<Msg>| {
        let that = that.clone();
        async move {
            log::debug!("处理消息订阅[用户表变化], 清除用户角色信息缓存完成");

            let id_str = msg.get_payload();
            if !id_str.is_empty() {
                if let Ok(id) = id_str.parse::<u32>() {
                    that.user_cache.invalidate(&id);
                    return Ok(());
                }
            }

            that.user_cache.invalidate_all();
            Ok(())
        }
    }).await.dot()?;

    // 监听用户退出登录消息
    if auth.jwt_data.is_some() {
        subscribe(logout_chan, move |msg: Arc<Msg>| {
            let msg = msg.clone();
            async move {
                let token = msg.get_payload();
                set_invalid_token(token).await;
                log::debug!("处理消息订阅[用户表变化], 清除用户角色信息缓存完成");
                Ok(())
            }
        }).await.dot()?;
    }
    Ok(())
}

// 从redis中获取无效token，返回true表示获取到了，false表示未找到
async fn get_invalid_token(token: &str) -> bool {
    let sign = match jwt::get_sign(token) {
        Some(sign) => sign,
        None => return false,
    };
    let key = get_invalid_token_key(sign);

    crate::services::uri::get::<String>(&key).await.is_some()
}

// 在redis中设置无效token
pub async fn set_invalid_token(token: &str) {
    let sign = match jwt::get_sign(token) {
        Some(sign) => sign,
        None => {
            log::error!("获取token签名失败");
            return;
        }
    };
    let key = get_invalid_token_key(sign);
    // 解析token, 无需校验签名
    let exp = Authentication::get_exp(token);
    if exp == 0 {
        return;
    }

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    crate::services::uri::set(&key, "1", exp - now).await;
}

// 格式化无效token的key
fn get_invalid_token_key(sign: &str) -> String {
    format!("{}:{}:{}", AppConf::get().redis_pre, consts::INVALID_TOKEN_KEY, sign)
}

pub const ANONYMOUS_CODE: i16 = -2;
pub const PUBLIC_CODE: i16 = -1;

use crate::{services::uri::UniRedisImpl, utils::{consts, multi_cache::MultiCache, staticmut::StaticMut}, AppConf};
pub static mut AUTHENTICATION: StaticMut<Arc<Authentication>> = StaticMut::new();

pub fn get_authentication() -> Arc<Authentication> {
    unsafe { AUTHENTICATION.get().clone() }
}

pub fn init_authentication(auth: Arc<Authentication>) {
    unsafe {
        AUTHENTICATION.init(auth);
    }
}
