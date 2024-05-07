//! 内部调试接口
use crate::{services::{rcache, rmq::ChannelName}, utils::mq_util::{emit, RecChanged}, AppGlobal};
use anyhow_ext::Context;
use compact_str::CompactString;
use httpserver::{HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};

/// 重置权限
pub async fn reset_permission(ctx: HttpContext) -> HttpResponse {
    emit(ctx.id, ChannelName::ModRole, &RecChanged::with_all());
    emit(ctx.id, ChannelName::ModApi, &RecChanged::with_all());
    emit(ctx.id, ChannelName::ModUser, &RecChanged::with_all());
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
        key: CompactString,
        zflag: Option<bool>,
    }

    #[derive(Serialize)]
    // #[serde(rename_all = "camelCase")]
    struct Res {
        value: Option<String>,
    }

    let req_param: Req = ctx.parse_json()?;
    let value = if req_param.zflag.is_some() && req_param.zflag.unwrap() {
        rcache::lz4_get(&req_param.key).await
    } else {
        rcache::get(&req_param.key).await
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
        key: CompactString,
        value: CompactString,
        ttl: u64,
    }

    let param: Req = ctx.parse_json()?;
    rcache::set(&param.key, param.value.as_str(), param.ttl).await;

    Resp::ok_with_empty()
}
