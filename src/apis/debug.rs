//! 内部调试接口
use crate::{services::{mq, uri}, utils::consts, AppConf, AppGlobal};
use anyhow_ext::Context;
use httpserver::{HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};

/// 重置权限
pub async fn reset_permission(_ctx: HttpContext) -> HttpResponse {
    let prefix = format!("{}:{}:", AppConf::get().redis_pre, consts::gmc::MOD_KEY);
    let empty = String::with_capacity(0);
    let keys = [consts::gmc::SYS_ROLE, consts::gmc::SYS_API, consts::gmc::SYS_USER];

    for k in keys {
        let mut key = prefix.clone();
        key.push_str(k);
        mq::publish(&key, &empty).await.dot()?;
    }

    Resp::ok_with_empty()
}

/// 清空redis缓存
pub async fn redis_clear(_ctx: HttpContext) -> HttpResponse {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        startup: LocalTime, // 应用启动时间
    }

    let app_global = AppGlobal::get();

    Resp::ok(&Res {
        startup: LocalTime::from_unix_timestamp(app_global.startup_time),
    })
}

/// 获取指定的缓存项
pub async fn redis_get(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    struct Req {
        key: String,
        zflag: Option<bool>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        value: Option<String>,
    }

    let req_param: Req = ctx.parse_json()?;
    let value = if req_param.zflag.is_some() && req_param.zflag.unwrap() {
        uri::get_lz4(&req_param.key).await
    } else {
        uri::get(&req_param.key).await
    };
    let value = match value {
        Some(v) => Some(String::from_utf8(v).context("校验字符串utf8格式失败")?),
        None => None,
    };

    Resp::ok(&Res { value })
}

/// 设置缓存项
pub async fn redis_set(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    struct Req {
        key: String,
        value: String,
        ttl: u64,
    }

    let param: Req = ctx.parse_json()?;
    uri::set(&param.key, param.value.as_str(), param.ttl).await;

    Resp::ok_with_empty()
}
