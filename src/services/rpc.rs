use anyhow::{Context, Result};
use compact_str::CompactString;
use httpserver::ApiResult;
use hyper::{body::Buf, client::HttpConnector, Body, Client, Method, Request};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::Duration;

use crate::{AppConf, AppGlobal};

const CONNECT_TIMEOUT: u32 = 3;
const REG_PATH: &str = "/api/gw/reg";
const CFG_PATH: &str = "/api/gw/cfg";
const TOKEN_PATH: &str = "/api/gw/token";
pub const REG_ACTION: &str = "向网关服务器注册服务";
const CFG_ACTION: &str = "向网关服务器获取配置信息";
const TOKEN_ACTION: &str = "请求网关服务器生成token";

static SERVICE_PATHS: [&str; 1] = ["/api/sys"];


#[derive(Deserialize)]
pub struct CfgItem {
    pub key: String,
    pub value: String,
}


/// 启动与网关之间的心跳任务(每30秒发送1次注册信息，表明服务仍然在线)
pub async fn start_heart_break_task(hb_interval: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(hb_interval));
    let mut reg_status = true;

    // tokio的定时器，第一次执行时会立即完成
    interval.tick().await;

    loop {
        interval.tick().await;
        // 只打印初次注册成功或失败的信息，连续成功或失败不再打印
        match reg_to_gateway().await {
            Ok(_) => {
                if !reg_status {
                    reg_status = true;
                    log::info!("{REG_ACTION}成功");
                }
            }
            Err(e) => {
                if reg_status {
                    reg_status = false;
                    log::error!("注册服务失败: {e:?}");
                }
            }
        }
    }
}

/// 将本地服务注册到网关服务器
pub async fn reg_to_gateway() -> Result<()> {
    #[derive(Serialize)]
    struct Req<'a> {
        endpoint: &'a str,
        paths: &'a [&'a str],
    }

    let _: Option<()> = post(
        REG_PATH,
        &Req {
            endpoint: &AppConf::get().listen,
            paths: &SERVICE_PATHS,
        },
    )
    .await
    .context(REG_ACTION)?;

    Ok(())
}

/// 远程调用网关服务器的token生成服务
#[allow(dead_code)]
pub async fn gen_token<T: Serialize>(claim: &T) -> Result<String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Req<'a, T> {
        ttl: u32,
        claim: &'a T,
    }

    #[derive(Deserialize)]
    struct Res {
        token: String,
    }

    let req = Req {
        ttl: AppGlobal::get().jwt_ttl,
        claim,
    };

    post::<_, Res>(TOKEN_PATH, &req)
        .await
        .context(TOKEN_ACTION)?
        .map(|v| v.token)
        .ok_or(anyhow::anyhow!("{TOKEN_ACTION}失败"))
}


pub async fn load_config(cfg_name: &str) -> Result<Vec<CfgItem>> {

    #[derive(Serialize)]
    struct Req {
        group: CompactString,
    }

    #[derive(Deserialize)]
    struct Res {
        config: Option<Vec<CfgItem>>,
    }

    // 获取配置信息
    let res = post::<_, Res>(
            CFG_PATH,
            &Req {
                group: CompactString::new(cfg_name),
            },
        )
        .await
        .context(CFG_ACTION)?
        .and_then(|res| res.config)
        .unwrap_or_else(|| Vec::with_capacity(0));

    Ok(res)
}

async fn post<T, R>(path: &str, param: &T) -> Result<Option<R>>
where
    T: Serialize,
    R: DeserializeOwned,
{
    use compact_str::format_compact as fmt;

    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{}{}", AppConf::get().gateway, path))
        .body(Body::from(serde_json::to_vec(param)?))
        .with_context(|| fmt!("远程调用{path}, 构建请求体对象失败"))?;

    let mut hc = HttpConnector::new();
    hc.set_connect_timeout(Some(Duration::from_secs(CONNECT_TIMEOUT as u64)));

    let res = Client::builder()
        .build(hc)
        .request(req)
        .await
        .with_context(|| fmt!("远程调用{path}, 构建客户端对象体失败"))?;

    let body = hyper::body::aggregate(res)
        .await
        .with_context(|| fmt!("远程调用{path}, 读取结果失败"))?;

    if body.remaining() > 0 {
        let ar: ApiResult<R> = serde_json::from_reader(body.reader())
            .with_context(|| fmt!("远程调用{path}, 反序列化结果失败"))?;

        if ar.is_ok() {
            Ok(ar.data)
        } else {
            let msg = ar.message.unwrap_or_else(|| "<错误消息为空>".to_owned());
            anyhow::bail!("远程调用{path}, {msg}")
        }
    } else {
        anyhow::bail!("http调用{path}失败, 回复内容为空")
    }
}
