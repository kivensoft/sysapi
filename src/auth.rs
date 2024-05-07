//! 权限校验中间件
use anyhow_ext::{bail, Context, Result};
use arc_swap::ArcSwap;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use compact_str::{CompactString, ToCompactString};
use cookie::Cookie;
use fnv::FnvBuildHasher;
use httpserver::{
    log_debug, log_error, log_trace, log_warn, HttpContext, HttpResponse, Next, Resp
};
use hyper::StatusCode;
use serde_json::Value;
use std::{borrow::Cow, collections::HashMap, sync::{Arc, OnceLock}};

use crate::{
    entities::{sys_api::SysApi, sys_role::SysRole, sys_user::SysUser},
    services::rmq,
    utils,
};

type Cache<K, V> = mini_moka::sync::Cache<K, V, FnvBuildHasher>;
type PermitValue = Arc<Vec<i16>>;
type ArcMap<K, V> = ArcSwap<HashMap<K, V>>;

pub struct Authentication;

#[derive(Clone)]
struct TokenCacheItem {
    uid: CompactString,
    exp: u64,
}

struct JwtData {
    key: CompactString, // jwt密钥
    iss: CompactString, // jwt发布者
}

/// 全局静态变量
struct GlobalVal {
    content_path: CompactString,                    // 上下文路径
    jwt_data: Option<JwtData>,                      // 令牌校验参数
    token_cache: Cache<String, TokenCacheItem>,     // jwt验证解码缓存
    roles: ArcMap<u32, CompactString>,              // 角色权限缓存
    permits: ArcMap<CompactString, PermitValue>,    // 接口权限缓存
    user_role_cache: Cache<u32, u32>,               // 用户角色映射缓存
    path_cache: Cache<CompactString, PermitValue>,  // 实际访问路径的权限缓存
}

pub const ACCESS_TOKEN: &str = "access_token";
pub const COOKIE_NAME: &str = "Cookie";

const TOKEN_VERIFIED: &str = "Token-Verified";
const UID_NAME: &str = "uid";
const USER_ROLE_CACHE_SIZE: u64 = 256; //USER_ROLE_CACHE的缓存大小
const PATH_CACHE_SIZE: u64 = 256; //PATH_CACHE的缓存大小

static GLOBAL_VAL: OnceLock<GlobalVal> = OnceLock::new();

#[async_trait::async_trait]
impl httpserver::HttpMiddleware for Authentication {
    async fn handle<'a>(&'a self, mut ctx: HttpContext, next: Next<'a>) -> HttpResponse {
        // 解析token并, 设置userId
        let mut token_err = None;
        let mut uid = "";
        if let Some(token) = get_token(&ctx) {
            log_trace!(ctx.id, "token: {}", token);
            match decode_token(ctx.id, &token) {
                // 解码token成功，将登录用户id写入ctx上下文环境
                Ok(user_id) => {
                    ctx.uid = user_id;
                    uid = &ctx.uid;
                    if !uid.is_empty() {
                        log_trace!(ctx.id, "uid: {}", uid);
                    }
                }
                Err(e) => token_err = Some(e),
            }
        };

        if auth(ctx.id, uid, ctx.req.uri().path()).await {
            next.run(ctx).await
        } else {
            if let Some(e) = token_err {
                log_error!(ctx.id, "令牌格式错误: {e:?}");
                return Resp::fail("令牌格式错误");
            }

            if uid.is_empty() {
                log_debug!(ctx.id, "权限校验失败[{}], 用户尚未登录", ctx.req.uri().path());
                Resp::fail_with_status(StatusCode::UNAUTHORIZED, 401, "Unauthorized")
            } else {
                log_debug!(ctx.id, "权限校验失败[{}], 当前用户没有访问权限", ctx.req.uri().path());
                Resp::fail_with_status(StatusCode::FORBIDDEN, 403, "Forbidden")
            }
        }
    }
}

/// 初始化全局对象数据, 必须在使用Authentication前调用
///
/// Arguments:
///
/// * `key`: jwt密钥, key为空时，表示不使用api网关
/// * `issuer`: jwt发布者
/// * `cache_size`: jwt验证解码缓存最大数量
/// * `task_interval`: jwt令牌缓存定时清理周期时间（秒为单位）
///
pub async fn init(content_path: &str, key: &str, issuer: &str, cache_size: u64, task_interval: u64) {
    debug_assert!(GLOBAL_VAL.get().is_none());

    let jwt_data = if key.is_empty() {
        None
    } else {
        Some(JwtData {
            key: key.to_compact_string(),
            iss: issuer.to_compact_string(),
        })
    };

    let token_cache = mini_moka::sync::Cache::builder()
        .max_capacity(cache_size)
        .build_with_hasher(FnvBuildHasher::default());

    let path_cache = mini_moka::sync::Cache::builder()
        .max_capacity(PATH_CACHE_SIZE)
        .build_with_hasher(FnvBuildHasher::default());

    let user_role_cache = mini_moka::sync::Cache::builder()
        .max_capacity(USER_ROLE_CACHE_SIZE)
        .build_with_hasher(FnvBuildHasher::default());

    let global_val = GlobalVal {
        content_path: CompactString::new(content_path),
        jwt_data,
        token_cache,
        roles: ArcSwap::from_pointee(load_roles().await.unwrap()),
        permits: ArcSwap::from_pointee(load_permits().await.unwrap()),
        user_role_cache,
        path_cache,
    };

    let _ = GLOBAL_VAL.set(global_val);

    start_listen().await.expect("订阅数据变动监听事件失败");
    start_recycle_task(task_interval);

}

/// 权限校验, 返回true表示有权访问, false表示无权访问
pub async fn auth(req_id: u32, uid: &str, mut path: &str) -> bool {
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

    let gv = global_val();

    // 忽略路径中的上下文("/api")开头部分
    if !gv.content_path.is_empty() && !path.starts_with(gv.content_path.as_str()) {
        log_error!(req_id, "校验权限错误, 请求路径[{}]格式错误", path);
        return false;
    }

    path = &path[gv.content_path.len()..];
    let orig_path = CompactString::new(path);
    log_trace!(req_id, "权限校验路径: {}", path);

    let user_permits = get_user_permits(uid).await;

    // 优先从缓存中读取，加快匹配速度
    if let Some(ps) = global_val().path_cache.get(&orig_path) {
        log_trace!(req_id, "在缓存中找到匹配路径: {}", orig_path);
        return check_permit(uid, &user_permits, &ps);
    }

    // 末尾的'/'不参与匹配
    let path_bytes = path.as_bytes();
    let mut end_pos = path_bytes.len();
    if end_pos > 1 && path_bytes[end_pos - 1] == b'/' {
        end_pos -= 1;
    }

    let permits = global_val().permits.load();
    // 递归每个父路径进行权限匹配, 有权限访问则直接返回true, 否则继续循环
    loop {
        path = &path[..end_pos];

        if let Some(ps) = permits.get(path) {
            log_trace!(req_id, "找到匹配路径: {}", path);
            global_val().path_cache.insert(orig_path, ps.clone());
            return check_permit(uid, &user_permits, ps);
        }

        // 找到上一级目录, 找不到退出循环
        end_pos = match path.rfind('/') {
            Some(n) if n > 0 => n,
            _ => break,
        };
    }

    if let Some(ps) = permits.get("/") {
        return check_permit(uid, &user_permits, ps);
    }

    false
}

/// 校验jwt token, 返回token携带的uid值
pub fn decode_token(req_id: u32, token: &str) -> Result<CompactString> {
    // 获取token的签名值
    let sign = match &jwt::get_sign(token) {
        Some(sign) => sign.to_string(),
        None => bail!("找不到签名部分"),
    };
    let now = localtime::unix_timestamp();
    let gv = global_val();

    // 优先从缓存中读取签名，减少解密次数
    if let Some(cache_item) = gv.token_cache.get(&sign) {
        // 校验token过期时间
        if cache_item.exp >= now {
            log_trace!(req_id, "使用缓存校验token: {}", sign);
            return Ok(cache_item.uid);
        } else {
            // 缓存项过期，执行删除操作
            gv.token_cache.invalidate(&sign);
            log_trace!(req_id, "缓存项过期被删除: {sign}");
            return Ok(CompactString::with_capacity(0));
        }
    }

    // 如果使用了api网关，则jwt令牌已被网关校验过
    let claims = match &gv.jwt_data {
        Some(jwt_data) => jwt::decode(token, &jwt_data.key, &jwt_data.iss)?,
        None => get_claims(token)?,
    };

    let exp = jwt::get_exp(&claims).context("获取exp失败")?;

    if let Some(Value::String(uid)) = claims.get(UID_NAME) {
        log_trace!(req_id, "将token加入缓存: {}", sign);
        gv.token_cache.insert(sign, TokenCacheItem { uid: uid.into(), exp, });
        return Ok(uid.to_compact_string());
    }

    bail!("找不到uid");
}

/// 从jwt token中获取claims的内容并进行json反序列化
fn get_claims(token: &str) -> Result<Value> {
    let mut iter = token.split('.');
    if iter.next().is_none() {
        bail!("令牌格式错误: 未找到签名部分");
    }

    if let Some(body) = iter.next() {
        let body_bs = URL_SAFE_NO_PAD.decode(body).context("令牌base64解码失败")?;
        return serde_json::from_slice(&body_bs).context("反序列化token内容异常");
    }

    bail!("令牌格式错误: 找不到claims内容");
}

/// 从header/url/cookie中获取令牌
pub fn get_token(ctx: &HttpContext) -> Option<Cow<str>> {
    // 启用api网关模式
    if global_val().jwt_data.is_none() {
        match ctx.req.headers().get(TOKEN_VERIFIED) {
            Some(flag) => {
                let b = flag.as_bytes() == b"true";
                log_trace!(ctx.id, "api网关校验令牌结果: {}", b);
                if !b { return None; }
            }
            None => return None
        }
    }

    match ctx.req.headers().get(jwt::AUTHORIZATION) {
        Some(auth) => match auth.to_str() {
            Ok(auth) => {
                if auth.len() > jwt::BEARER.len() && auth.starts_with(jwt::BEARER) {
                    Some(Cow::Borrowed(&auth[jwt::BEARER.len()..]))
                } else {
                    log_warn!(ctx.id, "请求头部的令牌格式错误: {}", auth);
                    None
                }
            }
            Err(e) => {
                log_warn!(ctx.id, "请求头部的令牌值错误: {e:?}");
                None
            }
        },
        None => get_access_token(ctx),
    }
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
                for cookie in Cookie::split_parse_encoded(c_str) {
                    match cookie {
                        Ok(c) => {
                            if c.name() == ACCESS_TOKEN {
                                return Some(Cow::Owned(c.value().to_owned()));
                            }
                        }
                        Err(e) => {
                            log_warn!(ctx.id, "解析cookie值格式失败: {e:?}, cookie: {c_str}");
                        }
                    }
                }
            }
            Err(e) => log_warn!(ctx.id, "cookie值不是utf8格式: {e:?}"),
        };
    }

    None
}

// 启动基于tokio的异步定时清理任务
fn start_recycle_task(task_interval: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(task_interval));
    tokio::spawn(async move {
        interval.tick().await;
        loop {
            interval.tick().await;
            recycle();
        }
    });
}

// 删除缓存里过期的项，需要用户自行调用，避免占用独立的线程资源
fn recycle() {
    log::trace!("执行token缓存清理任务...");

    let now = utils::time::unix_timestamp();
    let gv = global_val();

    let ds: Vec<String> = gv.token_cache
        .iter()
        .filter(|v| v.value().exp < now)
        .map(|v| v.key().clone())
        .collect();
    let ds_count = ds.len();

    for k in ds {
        gv.token_cache.invalidate(&k);
        log::trace!("清理过期的token: {k}");
    }

    if ds_count > 0 {
        log::trace!("总计清理token过期项: {}", ds_count);
    } else {
        log::trace!("缓存清理完成，没有需要清理的缓存项");
    }
}

/// 启动数据变化监听服务, 在用户/角色/权限表变化时重新载入
async fn start_listen() -> Result<()> {
    use rmq::{make_channel, subscribe, ChannelName};

    subscribe(make_channel(ChannelName::ModRole), |_| async {
        global_val().roles.store(Arc::new(load_roles().await?));
        Ok(())
    })
    .await?;

    subscribe(make_channel(ChannelName::ModApi), |_| async {
        global_val().permits.store(Arc::new(load_permits().await?));
        global_val().path_cache.invalidate_all();
        Ok(())
    })
    .await?;

    subscribe(make_channel(ChannelName::ModUser), |_| async {
        global_val().user_role_cache.invalidate_all();
        log::trace!("处理消息订阅[用户表变化], 清除用户角色信息缓存完成");
        Ok(())
    })
    .await?;

    Ok(())
}

/// 根据用户id加载用户权限
async fn get_user_permits(uid: u32) -> CompactString {
    if uid == 0 {
        return CompactString::with_capacity(0);
    }

    let mut rid = global_val().user_role_cache.get(&uid).unwrap_or(0);

    if rid == 0 {
        rid = match SysUser::select_role_by_id(uid).await {
            Ok(v) => match v {
                Some(n) => {
                    global_val().user_role_cache.insert(uid, n);
                    n
                }
                None => 0,
            },
            Err(e) => {
                log::error!("数据库查询出错: {e:?}");
                0
            }
        };
    }

    match global_val().roles.load().get(&rid) {
        Some(permits) => permits.clone(),
        None => CompactString::with_capacity(0),
    }
}

/// 校验用户访问许可是否在给定的索引列表内
fn check_permit(uid: u32, permits: &str, ps: &[i16]) -> bool {
    for i in ps {
        if *i == utils::ANONYMOUS_PERMIT_CODE
            || (*i == utils::PUBLIC_PERMIT_CODE && uid != 0)
            || utils::bits::get(permits, *i as usize)
        {
            return true;
        }
    }

    false
}

/// 获取全局变量的引用
fn global_val() -> &'static GlobalVal {
    debug_assert!(GLOBAL_VAL.get().is_some());
    match GLOBAL_VAL.get() {
        Some(val) => val,
        None => unsafe { std::hint::unreachable_unchecked() },
    }
}

/// 从数据库中加载角色信息表, 返回所有角色id与对应的权限
async fn load_roles() -> Result<HashMap<u32, CompactString>> {
    let mut roles = HashMap::new();
    let roles_data = SysRole::select_all().await.context("加载角色信息失败")?;

    for r in &roles_data {
        let rid = r.role_id.unwrap();
        let ps = r.permissions.as_ref().unwrap().as_str();
        roles.insert(rid, CompactString::new(ps));
    }

    log::trace!("权限模块加载角色数据: {roles:?}");
    Ok(roles)
}

/// 从数据库中加载权限信息表, 返回所有路径对应的权限索引组
async fn load_permits() -> Result<HashMap<CompactString, Arc<Vec<i16>>>> {
    let mut permits = HashMap::new();
    let api_data = SysApi::select_all().await.context("加载路径权限信息失败")?;

    for a in &api_data {
        let pcode = a.permission_code.unwrap();
        let api_path = CompactString::new(a.api_path.as_ref().unwrap());
        let v = permits.entry(api_path).or_insert_with(Vec::new);
        v.push(pcode);
    }

    log::trace!("权限模块加载权限数据: {permits:?}");

    Ok(permits.into_iter().map(|(k, v)| (k, Arc::new(v))).collect())
}
