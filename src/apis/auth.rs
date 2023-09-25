//! 用户登录相关接口

use std::net::Ipv4Addr;
use crate::{
    AppConf, AppGlobal, auth,
    db::{sys_user::SysUser, sys_user_state::SysUserState},
    services::{rcache, rmq},
    utils,
};
use anyhow::{Result, Context};
use base64::{engine::general_purpose, Engine};
use compact_str::CompactString;
use cookie::{Cookie, time::Duration};
use httpserver::{HttpContext, Resp, HttpResult};
use itoa::Buffer;
use localtime::LocalTime;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

/// 允许最大的登录失败次数
const MAX_FAIL_COUNT: u32 = 10;
/// 登录失败达到限制值后的禁止登录时长(单位: 秒)
const DISABLE_LOGIN_TTL: u32 = 300;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRes {
    token  : String,
    key    : String,
    expire : LocalTime,
    user_id: u32,
}

/// 用户登录接口
pub async fn login(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        username: CompactString,
        password: CompactString,
        use_cookie: Option<bool>,
    }

    type Res = LoginRes;

    let ip = ctx.remote_ip();
    let param: Req = ctx.into_json().await?;

    // 校验失败登录次数
    check_login_count(&param.username).await?;

    // 加载账号对应的记录
    let user = get_user_by_account(&param.username).await?;

    // 校验口令是否正确
    check_password(&param.username, &param.password, user.password.as_ref().unwrap()).await?;

    let user_id = user.user_id.unwrap();
    // 更新用户登录次数，时间等状态和清空缓冲中对应账号的失败次数信息
    update_login_state(&param.username, user_id, ip).await?;

    // 生成登录返回结果
    let token = gen_token(user_id)?;
    let key = gen_refresh_token(user.username.as_ref().unwrap(), &token);
    let expire = (utils::time::unix_timestamp() + AppGlobal::get().jwt_ttl as u64) as i64;
    let expire = LocalTime::from_unix_timestamp(expire);

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::Login);
        let msg = serde_json::to_string(&SysUser {
                user_id: Some(user_id),
                ..Default::default()
            }).expect("json序列化失败");

        if let Err(e) = rmq::publish(&chan, &msg).await {
            log::error!("发送登录消息失败: {e:?}");
        }
    });

    if param.use_cookie.unwrap_or(false) {
        let cookie = Cookie::build(auth::ACCESS_TOKEN, token.to_owned())
            .max_age(Duration::seconds(AppGlobal::get().jwt_ttl as i64))
            .finish();
        let body = hyper::Body::from(
            serde_json::to_vec(&httpserver::ApiResult {
                code: 200,
                message: None,
                data: Some(&Res { token, key, expire, user_id }),
            })?
        );
        Ok(hyper::Response::builder()
                .header(httpserver::CONTENT_TYPE, httpserver::APPLICATION_JSON)
                .header("Set-Cookie", cookie.to_string())
                .body(body).context("response build error")?)
    } else {
        Resp::ok(&Res { token, key, expire, user_id })
    }

}

/// 退出登录
pub async fn logout(ctx: HttpContext) -> HttpResult {
    let token = get_auth_token(&ctx)?;

    let key = gen_invlid_key(token)?;
    rcache::set(&key, "1", AppGlobal::get().jwt_ttl as usize).await?;

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::Logout);
        let msg = serde_json::to_string(&SysUser {
                user_id: Some(ctx.uid()),
                ..Default::default()
            }).expect("json序列化失败");

        if let Err(e) = rmq::publish(&chan, &msg).await {
            log::error!("发送退出消息失败: {e:?}");
        }
    });

    Resp::ok_with_empty()
}

/// 获取新的token
pub async fn refresh(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize)]
    struct Req {
        key: CompactString,
    }

    type Res = LoginRes;

    let user_id = ctx.uid();
    let token = get_auth_token(&ctx)?.to_owned();
    let param: Req = ctx.into_json().await?;

    // 加载当前登录用户信息
    let user = match SysUser::select_by_id(&user_id).await? {
        Some(user) => user,
        None => return Resp::fail("用户不存在"),
    };
    let username: &str = user.username.as_ref().unwrap();

    // 校验key是否正确
    let right_key = gen_refresh_token(username, &token);
    if param.key != right_key {
        return Resp::fail("key error");
    }

    // 生成返回结果
    let token = gen_token(user_id)?;
    let key = gen_refresh_token(user.username.as_ref().unwrap(), &token);
    let expire = (utils::time::unix_timestamp() + (AppGlobal::get().jwt_ttl as u64) * 60) as i64;
    let expire = LocalTime::from_unix_timestamp(expire);

    Resp::ok(&Res {
        token,
        key,
        expire,
        user_id,
    })
}

/// 鉴权接口, 提供给其它微服务调用本接口进行鉴权操作
pub async fn authenticate(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        user_id: Option<u32>,                   // user_id/token两个参数只需提供1个
        token  : Option<String>,
        path   : Option<CompactString>,         // path/paths两个只需提供1个
        paths  : Option<Vec<CompactString>>,
    }

    #[derive(Serialize, Default)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        user_id : Option<u32>,
        status  : Option<u32>,
        statuses: Option<Vec<u32>>,
    }

    let param: Req = ctx.into_json().await?;

    // path和paths必须有1个, 可校验1个或多个接口
    if param.path.is_none() && param.paths.is_none() {
        return Resp::fail("path/paths must have a value");
    }

    // user_id和token都为空的情况下, 表示用户未登录
    let uid = param.user_id
        .unwrap_or_else(|| {
            param.token.as_ref()
                .map(|token| {
                    auth::decode_token(token)
                        .unwrap_or_else(|e| {
                            log::error!("解析token参数发生错误: {e:?}");
                            0
                        })
                })
                .unwrap_or(0)
        });

    let user_id = if uid == 0 { None } else { Some(uid) };

    if let Some(path) = &param.path {
        let status = auth_status(uid, auth::auth(uid, path).await);

        return Resp::ok(&Res {
            user_id,
            status: Some(status),
            statuses: None,
        });
    }

    let paths = param.paths.unwrap();
    let mut statuses = Vec::with_capacity(paths.len());
    for path in paths.iter() {
        let status = auth_status(uid, auth::auth(uid, path).await);
        statuses.push(status);
    }

    Resp::ok(&Res {
        user_id,
        status: None,
        statuses: Some(statuses),
    })
}

/// 生成token
fn gen_token(user_id: u32) -> Result<String> {
    let ac = AppConf::get();
    jwt::encode(
        &serde_json::json!({ "uid": user_id }),
        &ac.jwt_key,
        &ac.jwt_iss,
        AppGlobal::get().jwt_ttl as u64,
    )
}

/// 基于原有token,生成新的token
fn gen_refresh_token(username: &str, token: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(token);
    hasher.update(&AppConf::get().refresh_key);
    hasher.update(username);

    general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// 校验失败登录次数
async fn check_login_count(account: &str) -> Result<()> {
    // 获取缓冲中保存的当前用户登录失败次数
    let cache_key = gen_fail_key(account);
    let fail_count = match rcache::get(&cache_key).await? {
        Some(s) => match s.parse() {
            Ok(n) => n,
            Err(e) => {
                log::error!("读取缓存{cache_key}失败: {e:?}");
                0
            }
        },
        None => 0,
    };

    // 判断当前登录次数是否达到限制值
    if fail_count >= MAX_FAIL_COUNT {
        let ttl = rcache::ttl(&cache_key).await?;
        if ttl > 0 {
            anyhow::bail!("账号已锁定, 请过{}后再进行登录", gen_time_desc(ttl as u32));
        }
    }

    Ok(())
}

/// 根据登录账号(用户名/邮件/手机号)查找, 返回用户记录
async fn get_user_by_account(account: &str) -> Result<SysUser> {
    // 加载账号对应的记录
    let user = SysUser::select_by_account(account).await?;
    let user = match user {
        Some(user) => user,
        None => anyhow::bail!("账号不存在"),
    };

    // 校验账号是否有效
    if user.disabled.unwrap() != 0 {
        anyhow::bail!("账号已被禁用");
    }

    // 校验账号是否有效
    if user.disabled.unwrap() != 0 {
        anyhow::bail!("账号已被禁用");
    }

    Ok(user)
}

/// 校验登录口令
async fn check_password(account: &str, password: &str, pw_hash: &str) -> Result<()> {
    // 校验口令是否正确
    if !utils::unix_crypt::verify(password, pw_hash)? {
        let cache_key = gen_fail_key(account);
        let count = rcache::incr(&cache_key, DISABLE_LOGIN_TTL as usize).await?;
        let n = MAX_FAIL_COUNT - count as u32;

        if n > 0 {
            anyhow::bail!(format!("口令错误, 您还可以尝试{n}次"));
        } else {
            anyhow::bail!(format!(
                "账号已锁定, 请过{}后再进行登录",
                gen_time_desc(DISABLE_LOGIN_TTL)
            ));
        }
    }

    Ok(())
}

/// 生成用于记录口令错误次数的键名
fn gen_fail_key(account: &str) -> String {
    format!( "{}:{}:{}", AppConf::get().cache_pre, rcache::CK_LOGIN_FAIL, account)
}

/// 秒值转换为基于友好时间表示的时间字符串
fn gen_time_desc(mut secs: u32) -> CompactString {
    let mut num_buf = Buffer::new();
    let mut time_desc = CompactString::new("");

    if secs >= 3600 {
        time_desc.push_str(num_buf.format(secs / 3600));
        time_desc.push_str("小时");
        secs %= 3600;
    }

    if secs >= 60 {
        time_desc.push_str(num_buf.format(secs / 60));
        time_desc.push_str("分钟");
        secs %= 60;
    }

    if !time_desc.is_empty() && secs != 0 {
        time_desc.push_str(num_buf.format(secs));
        time_desc.push_str("秒");
    }

    time_desc
}

/// 更新用户登录次数、最后登陆时间、最后登录ip等信息
async fn update_login_state(account: &str, user_id: u32, ip: Ipv4Addr) -> Result<()> {
    let ip = ip.to_string();
    let now = LocalTime::now();

    match SysUserState::select_by_id(&user_id).await? {
        Some(mut val) => {
            val.total_login = Some(val.total_login.unwrap() + 1);
            val.last_login_time = Some(now);
            val.last_login_ip = Some(ip);
            SysUserState::update_by_id(&val).await?;
        }
        None => {
            let val = SysUserState {
                user_id: Some(user_id),
                total_login: Some(1),
                last_login_time: Some(now),
                last_login_ip: Some(ip),
            };
            SysUserState::insert(&val).await?;
        }
    };

    // 清空缓存
    let cache_key = gen_fail_key(account);
    rcache::del(std::slice::from_ref(&cache_key)).await?;

    Ok(())
}

/// 生成用户退出登陆后的无效token键名, 用于判断token是否仍然有效
fn gen_invlid_key(token: &str) -> Result<String> {
    let sign = match jwt::get_sign(token) {
        Some(sign) => sign,
        None => {
            log::error!("jwt token中未找到sign");
            anyhow::bail!("token格式错误")
        }
    };

    let ac = AppConf::get();

    Ok(format!("{}:{}:{}", ac.cache_pre, rcache::CK_INVALID_TOKEN, sign))
}

/// 从http请求头中获取token
fn get_auth_token(ctx: &HttpContext) -> Result<&str> {
    let token = match ctx.header(jwt::AUTHORIZATION) {
        Some(token) => token,
        None => anyhow::bail!("token不存在"),
    };
    let token = match token.to_str() {
        Ok(token) => token,
        Err(e) => {
            log::error!("从http获取token错误: {e:?}");
            anyhow::bail!("http请求头部错误")
        }
    };

    Ok(token)
}

/// 根据权限校验结果及用户id值返回相应的状态码
fn auth_status(user_id: u32, auth_result: bool) -> u32 {
    if auth_result {
        200
    } else if user_id == 0 {
        401
    } else {
        403
    }
}
