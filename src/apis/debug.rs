//! 内部调试接口

use crate::{services::rcache, AppGlobal};
use compact_str::CompactString;
use httpserver::{HttpContext, Resp, HttpResult};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};

/// 重置权限
pub async fn reset_permission(_ctx: HttpContext) -> HttpResult {
    // Resp::ok_with_empty()
    todo!()
}

/// 清空redis缓存
pub async fn redis_clear(_ctx: HttpContext) -> HttpResult {
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
pub async fn redis_get(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize)]
    struct Req {
        key: CompactString,
    }

    #[derive(Serialize)]
    // #[serde(rename_all = "camelCase")]
    struct Res {
        value: Option<String>,
    }

    let req_param: Req = ctx.into_json().await?;
    let value = rcache::get(&req_param.key).await?;

    Resp::ok(&Res { value })
}

/// 设置缓存项
pub async fn redis_set(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize)]
    struct Req {
        key: CompactString,
        value: CompactString,
        ttl: u64,
    }

    let param: Req = ctx.into_json().await?;
    rcache::set(&param.key, &param.value, param.ttl as usize).await?;

    Resp::ok_with_empty()
}
