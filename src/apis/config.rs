//! 系统配置接口

use crate::{db::{sys_config::SysConfig, PageQuery}, services::rmq};
use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysConfig>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysConfig::select_page(param.data(), param.page()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysConfig::select_by_id(param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysConfig;
    type Res = SysConfig;

    let mut param: Req = ctx.into_json().await?;

    check_required!(param, cfg_name, cfg_value);

    param.updated_time = Some(LocalTime::now());

    let id = check_result!(match param.cfg_id {
        Some(id) => SysConfig::update_by_id(&param).await.map(|_| id),
        None => SysConfig::insert(&param).await.map(|(_, id)| id),
    });

    tokio::spawn(async move {
        let msg = SysConfig { cfg_id: Some(id), ..Default::default() };
        let chan = rmq::make_channel(rmq::ChannelName::ModConfig);
        let op = match param.cfg_id {
            Some(_) => rmq::RecChanged::publish_update(&chan, msg).await,
            None => rmq::RecChanged::publish_insert(&chan, msg).await,
        };
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok( &Res {
        cfg_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let r = SysConfig::delete_by_id(param.id).await;
    check_result!(r);

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::ModConfig);
        let op = rmq::RecChanged::publish_delete(&chan, SysConfig {
            cfg_id: Some(param.id),
            ..Default::default()
        }).await;
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok_with_empty()
}
