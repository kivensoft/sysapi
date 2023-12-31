use anyhow::{Result, Context};
use base64::{engine::general_purpose, Engine};
use compact_str::CompactString;
use cookie::Cookie;
use httpserver::{HttpContext, Next, Resp, HttpResult};
use hyper::StatusCode;
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use rclite::Arc;
use std::{borrow::Cow, collections::HashMap, num::NonZeroUsize};

use crate::{
    entities::{sys_user::SysUser, sys_role::SysRole, sys_api::SysApi},
    services::rmq, utils
};

pub struct Authentication;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenBody {
    pub uid: u32,
}

/// 全局静态变量
struct GlobalVal {
    roles: RwLock<Arc<HashMap<u32, CompactString>>>,
    permits: RwLock<Arc<HashMap<CompactString, Vec<i16>>>>,
    user_role_cache: Mutex<LruCache<u32, u32>>,
}

pub const ACCESS_TOKEN: &str = "access_token";
pub const COOKIE_NAME: &str = "Cookie";
pub const API_PATH_PRE: &str = "/api"; // 接口请求的统一路径前缀, 权限判断时忽略该前缀

const USER_ROLE_CACHE_SIZE: Option<NonZeroUsize> = NonZeroUsize::new(128); //USER_ROLE_CACHE的缓存大小

static mut GLOBAL_VAL: Option<GlobalVal> = None;

/// 初始化全局对象数据, 必须在使用Authentication前调用
pub async fn init() {
    let roles = RwLock::new(Arc::new(load_roles().await.expect("加载角色列表错误")));
    let permits = RwLock::new(Arc::new(load_permits().await.expect("加载权限路径列表错误")));
    let user_role_cache = Mutex::new(LruCache::new(USER_ROLE_CACHE_SIZE.unwrap()));

    unsafe {
        debug_assert!(GLOBAL_VAL.is_none());
        GLOBAL_VAL = Some(GlobalVal { roles, permits, user_role_cache });
    }

    start_listen().await.expect("权限校验服务订阅数据变动监听事件失败");
}

#[async_trait::async_trait]
impl httpserver::HttpMiddleware for Authentication {
    async fn handle<'a>(&'a self, mut ctx: HttpContext, next: Next<'a>,) -> HttpResult {
        // 解析token并, 设置userId
        if let Some(token) = get_token(&ctx) {
            log::trace!("校验 token: [{token}]");
            match decode_token(&token) {
                // 解码token成功，将登录用户id写入ctx上下文环境
                Ok(val) => ctx.uid = val,
                Err(e) => {
                    log::error!("[{:08x}] AUTH verify token error: {:?}", ctx.id, e);
                    // anyhow::bail!("无效的令牌");
                }
            }
        }

        if auth(ctx.uid, ctx.req.uri().path()).await {
            next.run(ctx).await
        }
        else if ctx.uid == 0 {
            log::trace!("权限校验失败[{}], 用户尚未登录", ctx.req.uri().path());
            Resp::fail_with_status(StatusCode::UNAUTHORIZED, 401, "Unauthorized")
        }
        else {
            log::trace!("权限校验失败[{}], 当前用户没有访问权限", ctx.req.uri().path());
            Resp::fail_with_status(StatusCode::FORBIDDEN, 403, "Forbidden")
        }

    }
}

/// 启动数据变化监听服务, 在用户/角色/权限表变化时重新载入
async fn start_listen() -> Result<()> {
    use rmq::{subscribe, make_channel, ChannelName};

    subscribe(make_channel(ChannelName::ModRole), |_| async {
        let roles = load_roles().await.context("处理订阅消息[角色信息变化]失败")?;
        *global_val().roles.write() = Arc::new(roles);
        Ok(())
    }).await?;

    subscribe(make_channel(ChannelName::ModApi), |_| async {
        let permits = load_permits().await.context("处理订阅消息[路径权限信息变化]失败")?;
        *global_val().permits.write() = Arc::new(permits);
        Ok(())
    }).await?;

    subscribe(make_channel(ChannelName::ModUser), |_| async {
        global_val().user_role_cache.lock().await.clear();
        log::trace!("处理消息订阅[用户表变化], 清除用户角色信息缓存完成");
        Ok(())
    }).await?;

    Ok(())
}

/// 权限校验, 返回true表示有权访问, false表示无权访问
pub async fn auth(uid: u32, mut path: &str) -> bool {
    let permits = global_val().permits.read().clone();
    let user_permits = get_user_permits(uid).await;

    // 忽略路径中的"/api"开头部分
    if !path.starts_with(API_PATH_PRE) {
        log::error!("request path [{path}] format error!");
        return false
    }
    path = &path[4..];

    // 末尾的'/'不参与匹配
    let path_bytes = path.as_bytes();
    let mut end_pos = path_bytes.len();
    if end_pos > 1 && path_bytes[end_pos - 1] == b'/' {
        end_pos -= 1;
    }

    // 递归每个父路径进行权限匹配, 有权限访问则直接返回true, 否则继续循环
    loop {
        path = &path[0..end_pos];

        if let Some(ps) = permits.get(path) {
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

/// 根据用户id加载用户权限
async fn get_user_permits(uid: u32) -> CompactString {
    if uid == 0 {
        return CompactString::new("");
    }

    let mut rid = match global_val().user_role_cache.lock().await.get(&uid) {
        Some(v) => *v,
        None => 0,
    };

    if rid == 0 {
        rid = match SysUser::select_role_by_id(uid).await {
            Ok(v) => match v {
                Some(n) => {
                    global_val().user_role_cache.lock().await.put(uid, n);
                    n
                },
                None => 0,
            },
            Err(e) => {
                log::error!("数据库查询出错: {e:?}");
                0
            }
        };
    }

    match global_val().roles.read().get(&rid) {
        Some(permits) => permits.clone(),
        None => CompactString::new(""),
    }
}

/// 校验jwt token, 返回token携带的uid值
pub fn decode_token(token: &str) -> Result<u32> {
    if let Some((_, token)) = token.split_once('.') {
        if let Some((body, _)) = token.split_once('.') {
            let body_bs = general_purpose::URL_SAFE_NO_PAD.decode(body)?;
            let claim = serde_json::from_slice::<TokenBody>(&body_bs).context("反序列化token内容异常")?;
            return Ok(claim.uid);
        }
    }

    anyhow::bail!("token body format error");
}

/// 从url参数或cookie中解析access_token
fn get_token(ctx: &HttpContext) -> Option<Cow<str>> {
    match ctx.req.headers().get(jwt::AUTHORIZATION) {
        Some(auth) => match auth.to_str() {
            Ok(auth) => {
                if auth.len() > jwt::BEARER.len() && auth.starts_with(jwt::BEARER) {
                    Some(Cow::Borrowed(&auth[jwt::BEARER.len()..]))
                } else {
                    log::warn!("Authorization is not jwt token");
                    None
                }
            }
            Err(e) => {
                log::warn!("Authorization value is invalid: {e:?}");
                None
            },
        },
        None => get_access_token(ctx),
    }
}

/// 从url参数中解析access_token
fn get_access_token(ctx: &HttpContext) -> Option<Cow<str>> {
    // 优先从url中获取access_token参数
    if let Some(query) = ctx.req.uri().query() {
        let url_params = querystring::querify(query);
        if let Some(param) = url_params.iter().find(|v| v.0 == ACCESS_TOKEN) {
            match urlencoding::decode(param.1) {
                Ok(token) => return Some(token),
                Err(e) => log::warn!("request uri query is not utf8 text: {e:?}"),
            }
        };
    };

    // url中找不到, 尝试从cookie中获取access_token
    if let Some(cookie_str) = ctx.req.headers().get(COOKIE_NAME) {
        match cookie_str.to_str() {
            Ok(c_str) => {
                for cookie in Cookie::split_parse_encoded(c_str) {
                    match cookie {
                        Ok(c) => if c.name() == ACCESS_TOKEN {
                            return Some(Cow::Owned(c.value().to_owned()));
                        },
                        Err(e) => log::warn!("cookie value [{}] parse encode error: {e:?}", c_str),
                    }
                }
            },
            Err(e) => log::warn!("cookie value is not utf8 text: {e:?}")
        };
    }

    None
}

/// 校验用户访问许可是否在给定的索引列表内
fn check_permit(uid: u32, permits: &str, ps: &[i16]) -> bool {
    for i in ps {
        match *i {
            utils::ANONYMOUS_PERMIT_CODE => return true,
            utils::PUBLIC_PERMIT_CODE => if uid != 0 {
                return true
            },
            idx => if utils::bits::get(permits, idx as usize) {
                return true
            },
        }
    }

    false
}

/// 获取全局变量的引用
fn global_val() -> &'static GlobalVal {
    unsafe {
        debug_assert!(GLOBAL_VAL.is_some());
        match &GLOBAL_VAL {
            Some(val) => &val,
            None => std::hint::unreachable_unchecked()
        }
    }
}

/// 从数据库中加载角色信息表, 返回所有角色id与对应的权限
async fn load_roles() -> Result<HashMap<u32, CompactString>> {
    let mut roles = HashMap::new();
    let roles_data = SysRole::select_all().await?;

    for r in &roles_data {
        let rid = r.role_id.unwrap();
        let ps = r.permissions.as_ref().unwrap().as_str();
        roles.insert(rid, CompactString::new(ps));
    }

    log::trace!("权限模块加载角色数据: {roles:?}");
    Ok(roles)
}

/// 从数据库中加载权限信息表, 返回所有路径对应的权限索引组
async fn load_permits() -> Result<HashMap<CompactString, Vec<i16>>> {
    let mut permits = HashMap::new();
    let api_data = SysApi::select_all().await?;
    // permits.insert(CompactString::new("/login"), vec![ANONYMOUS_PERMIT]);

    for a in &api_data {
        let pcode = a.permission_code.unwrap();
        let api_path = CompactString::new(a.api_path.as_ref().unwrap());
        let v = permits.entry(api_path).or_insert_with(Vec::new);
        v.push(pcode);
    }

    log::trace!("权限模块加载权限数据: {permits:?}");
    Ok(permits)
}
