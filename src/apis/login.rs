//! 用户登录相关接口
use crate::{
    auth,
    entities::sys_user::{self, SysUser},
    services::{rcache, rmq},
    utils, AppConf, AppGlobal,
};
use compact_str::{CompactString, ToCompactString};
use httpserver::{fail_if, http_bail, log_error, log_info, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};


#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRes {
    token: String,
    key: String,
    expire: LocalTime,
    user_id: u32,
}

/// 用户登录接口
pub async fn login(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        username: CompactString,
        password: CompactString,
    }

    type Res = LoginRes;

    let rid = ctx.id;
    let ip = ctx.remote_ip();
    let param: Req = ctx.parse_json()?;

    fail_if!(param.username.is_empty(), "用户名不能为空");
    fail_if!(param.password.is_empty(), "密码不能为空");

    log_info!(rid, "用户{}尝试登录", param.username);
    let user = sys_user::user_login(&param.username, &param.password, &ip).await?;
    let user_id = user.user_id.unwrap();

    // 生成登录返回结果
    let token = sys_user::create_jwt_token(user_id)?;
    let key = sys_user::create_refresh_token(&token, &AppConf::get().jwt_refresh);
    let expire = {
        let jwt_ttl = AppGlobal::get().jwt_ttl;
        let exp = (utils::time::unix_timestamp() + jwt_ttl as u64) as i64;
        LocalTime::from_unix_timestamp(exp)
    };

    // 发布用户登录消息
    let msg = serde_json::to_string(&SysUser {
        user_id: Some(user_id),
        ..Default::default()
    })?;
    rmq::publish_async(rmq::make_channel(rmq::ChannelName::Login), msg);
    log_info!(rid, "用户[{}:{}]登录成功", user.username.unwrap(), user.user_id.unwrap());

    Resp::ok(&Res {
        token,
        key,
        expire,
        user_id,
    })
}

/// 退出登录
pub async fn logout(ctx: HttpContext) -> HttpResponse {
    let rid = ctx.id;
    let token = match auth::get_token(&ctx) {
        Some(s) => s,
        None => http_bail!("缺少令牌"),
    };
    let sign = match jwt::get_sign(&token) {
        Some(sign) => sign,
        None => http_bail!("令牌格式错误"),
    };
    let cache_key = format!(
        "{}:{}:{}",
        AppConf::get().cache_pre,
        rcache::CK_INVALID_TOKEN,
        sign
    );

    // 将退出登录用户令牌的相关信息记录到缓存
    rcache::set(&cache_key, "1", AppGlobal::get().jwt_ttl as u64).await;

    // 发布用户退出登录消息
    let msg = serde_json::to_string(&SysUser {
        user_id: Some(ctx.uid.parse().unwrap()),
        ..Default::default()
    })?;
    rmq::publish_async(rmq::make_channel(rmq::ChannelName::Logout), msg);
    log_info!(rid, "用户[{}]登出", ctx.uid);

    Resp::ok_with_empty()
}

/// 获取新的token
pub async fn refresh(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    struct Req {
        key: CompactString,
    }

    type Res = LoginRes;

    let rid = ctx.id;
    let user_id: u32 = ctx.uid.parse().unwrap();
    let token = match auth::get_token(&ctx) {
        Some(s) => String::from(s),
        None => http_bail!("缺少令牌"),
    };
    let param: Req = ctx.parse_json()?;

    // 校验key是否正确
    let jwt_refresh_key = &AppConf::get().jwt_refresh;
    let right_key = sys_user::create_refresh_token(&token, jwt_refresh_key);
    if param.key != right_key {
        http_bail!("密钥错误");
    }

    // 生成返回结果
    let token = sys_user::create_jwt_token(user_id)?;
    let key = sys_user::create_refresh_token(&token, jwt_refresh_key);
    let expire = (utils::time::unix_timestamp() + (AppGlobal::get().jwt_ttl as u64) * 60) as i64;
    let expire = LocalTime::from_unix_timestamp(expire);
    log_info!(rid, "用户[{}]刷新令牌", user_id);

    Resp::ok(&Res {
        token,
        key,
        expire,
        user_id,
    })
}

/// 鉴权接口, 提供给其它微服务调用本接口进行鉴权操作
pub async fn authenticate(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        user_id: Option<u32>, // user_id/token两个参数只需提供1个
        token: Option<String>,
        path: Option<CompactString>, // path/paths两个只需提供1个
        paths: Option<Vec<CompactString>>,
    }

    #[derive(Serialize, Default)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        user_id: Option<u32>,
        status: Option<u32>,
        statuses: Option<Vec<u32>>,
    }

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;

    // path和paths必须有1个, 可校验1个或多个接口
    if param.path.is_none() && param.paths.is_none() {
        http_bail!("path/paths must have a value");
    }

    // user_id和token都为空的情况下, 表示用户未登录
    let uid = match &param.user_id {
        Some(v) => *v,
        None => match &param.token {
            Some(token) => match auth::decode_token(rid, token) {
                Ok(uid) => match uid.parse() {
                    Ok(n) => n,
                    Err(_) => {
                        log_error!(rid, "token的uid格式不为整数");
                        http_bail!("token的uid格式错误")
                    }
                },
                Err(e) => {
                    log_error!(rid, "解码token错误: {e:?}");
                    http_bail!("无效token")
                }
            },
            None => 0,
        },
    };

    let user_id = if uid == 0 { None } else { Some(uid) };
    let uid_str = uid.to_compact_string();

    if let Some(path) = &param.path {
        let status = auth_status(uid, auth::auth(rid, &uid_str, path).await);

        return Resp::ok(&Res {
            user_id,
            status: Some(status),
            statuses: None,
        });
    }

    let paths = param.paths.unwrap();
    let mut statuses = Vec::with_capacity(paths.len());
    for path in paths.iter() {
        let status = auth_status(uid, auth::auth(rid, &uid_str, path).await);
        statuses.push(status);
    }

    Resp::ok(&Res {
        user_id,
        status: None,
        statuses: Some(statuses),
    })
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
