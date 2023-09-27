//! 实用工具接口

use crate::{AppGlobal, utils::unix_crypt, auth};
use compact_str::{format_compact, CompactString, ToCompactString};
use fast_qr::{
    convert::{image::ImageBuilder, Builder, Shape},
    QRBuilder,
};
use httpserver::{HttpContext, Resp, HttpResult};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};

/// 服务测试，测试服务是否存活
pub async fn ping(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize)]
    struct Req {
        reply: Option<CompactString>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        reply: CompactString,
        now: LocalTime,
        server: CompactString,
    }

    let reply = match ctx.into_opt_json::<Req>().await? {
        Some(ping_params) => ping_params.reply,
        None => None,
    }
    .unwrap_or(CompactString::new("pong"));

    Resp::ok(&Res {
        reply,
        now: LocalTime::now(),
        server: format_compact!("{}/{}", crate::APP_NAME, crate::APP_VER),
    })
}

/// 服务状态
pub async fn status(ctx: HttpContext) -> HttpResult {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        startup:      LocalTime,    // 服务启动时间
        resp_count:   u32,          // 总响应次数
        content_path: &'static str, // 上下文路径
        app_name:     &'static str, // 应用名称
        app_ver:      &'static str, // 应用版本
    }

    let app_global = AppGlobal::get();

    Resp::ok(&Res {
        startup: LocalTime::from_unix_timestamp(app_global.startup_time),
        resp_count: ctx.id(),
        content_path: auth::API_PATH_PRE,
        app_name: crate::APP_NAME,
        app_ver: crate::APP_VER,
    })
}

/// 获取客户端ip
pub async fn ip(ctx: HttpContext) -> HttpResult {
    #[derive(Serialize)]
    // #[serde(rename_all = "camelCase")]
    struct Res {
        ip: CompactString,
    }

    let ip = ctx.remote_ip().to_compact_string();

    Resp::ok(&Res { ip })
}

/// 生成二维码
pub async fn qrcode(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize, Serialize)]
    struct Req {
        text: String,
        width: Option<u32>,
    }

    #[derive(Serialize)]
    struct Res {
        text: LocalTime,
    }

    let param: Req = ctx.into_json().await?;
    let width = param.width.unwrap_or(200);

    let qrcode = QRBuilder::new(param.text).build()?;

    let img = ImageBuilder::default()
        .shape(Shape::RoundedSquare)
        .background_color([255, 255, 255, 0])
        .fit_width(width)
        .to_pixmap(&qrcode)
        .encode_png()?;

    Ok(hyper::Response::builder()
        .header(httpserver::CONTENT_TYPE, "image/png")
        .body(hyper::Body::from(img))?)
}

/// 生成账号对应的密码
pub async fn gen_pass(ctx: HttpContext) -> HttpResult {
    #[derive(Deserialize, Serialize)]
    struct Req {
        pass: CompactString,
    }

    type Res = Req;

    let param: Req = ctx.into_json().await?;
    let digest = unix_crypt::encrypt(&param.pass)?;

    Resp::ok(&Res {
        pass: CompactString::new(digest),
    })
}
