//! 用户登录相关接口
use crate::{
    auth::{self, Authentication},
    entities::sys_user,
    services::mq,
    utils::{self, audit, consts},
    AppConf, AppGlobal,
};
use httpserver::{fail_if, http_bail, if_else, log_info, HttpContext, HttpResponse, Resp};
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
        username: String,
        password: String,
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

    // 写入审计日志
    let audit_data = serde_json::json!({
        "userId": user_id,
        "username": param.username.to_string(),
        "ip": ip.to_string(),
    });
    audit::log_json(audit::LOGIN, user_id, &audit_data);

    // 发布用户登录消息
    let chan = format!("{}:{}", AppConf::get().redis_pre, consts::CC_LOGIN);
    mq::publish_async(chan, user_id.to_string());
    log_info!(rid, "用户[{}:{}]登录成功", user.username.unwrap(), user.user_id.unwrap());

    Resp::ok(&Res { token, key, expire, user_id })
}

/// 退出登录
pub async fn logout(ctx: HttpContext) -> HttpResponse {
    if let Some(token) = Authentication::get_token_from_header(&ctx) {
        // 写入审计日志
        audit::log(audit::LOGOUT, ctx.user_id(), String::new());
        // 发送用户登出通知消息
        let chan = format!("{}:{}", AppConf::get().redis_pre, consts::CC_LOGOUT);
        mq::publish_async(chan, token.to_string());
        log_info!(ctx.id, "用户[{}]登出", ctx.uid);
    };

    Resp::ok_with_empty()
}

/// 获取新的token
pub async fn refresh_token(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    struct Req {
        key: String,
    }

    type Res = LoginRes;

    let user_id: u32 = ctx.uid.parse().unwrap();
    let token = match Authentication::get_token_from_header(&ctx) {
        Some(s) => String::from(s),
        None => http_bail!("缺少令牌"),
    };
    let param: Req = ctx.parse_json()?;

    // 校验key是否正确
    let jwt_refresh_key = &AppConf::get().jwt_refresh;
    let right_key = sys_user::create_refresh_token(&token, jwt_refresh_key);
    fail_if!(param.key.as_str() != right_key.as_str(), "密钥错误");

    // 生成返回结果
    let token = sys_user::create_jwt_token(user_id)?;
    let key = sys_user::create_refresh_token(&token, jwt_refresh_key);
    let expire = (utils::time::unix_timestamp() + (AppGlobal::get().jwt_ttl as u64) * 60) as i64;
    let expire = LocalTime::from_unix_timestamp(expire);
    log_info!(ctx.id, "用户[{}]刷新令牌", user_id);

    // 写入审计日志
    audit::log(audit::REFRESH, ctx.user_id(), param.key.to_string());

    Resp::ok(&Res {
        token,
        key,
        expire,
        user_id,
    })
}

/// 鉴权接口, 提供给其它微服务调用本接口进行鉴权操作
/// 返回值中的字段 status/statuses 为鉴权的结果, 200: 允许访问, 401: 用户尚未登录, 403: 无权访问
pub async fn authenticate(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        path: Option<String>, // path/paths两个只需提供1个
        paths: Option<Vec<String>>,
    }

    #[derive(Serialize, Default)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        user_id: Option<u32>,
        status: Option<u32>,
        statuses: Option<Vec<u32>>,
    }

    let auth = auth::get_authentication();
    let param: Req = ctx.parse_json()?;

    // path和paths必须有1个, 可校验1个或多个接口
    if param.path.is_none() && param.paths.is_none() {
        http_bail!("path/paths must have a value");
    }

    // user_id和token都为空的情况下, 表示用户未登录
    let uid = ctx.user_id();
    let user_id = if_else!(uid != 0, Some(uid), None);
    let uid_str = &ctx.uid;

    if let Some(path) = &param.path {
        let status = auth_status(uid, auth.auth(ctx.id, uid_str, path).await);

        return Resp::ok(&Res {
            user_id,
            status: Some(status),
            statuses: None,
        });
    }

    let paths = param.paths.unwrap();
    let mut statuses = Vec::with_capacity(paths.len());
    for path in paths.iter() {
        let status = auth_status(uid, auth.auth(ctx.id, uid_str, path).await);
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
