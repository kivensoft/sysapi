//! 系统配置接口
use crate::{
    entities::{sys_config::SysConfig, PageQuery},
    services::rmq::ChannelName,
    utils::mq_util::{emit, RecChanged},
};
use anyhow_ext::anyhow;
use httpserver::{check_required, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysConfig>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysConfig::select_page(param.inner, pg)
        .await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysConfig::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysConfig;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;

    check_required!(param, cfg_name, cfg_value);

    param.cfg_id = None;
    param.updated_time = Some(LocalTime::now());

    let id = SysConfig::insert(param).await?.1;

    let res = SysConfig {
        cfg_id: Some(id),
        ..Default::default()
    };
    emit(rid, ChannelName::ModConfig, &RecChanged::with_insert(&res));

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysConfig;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;
    let cfg_id = param.cfg_id;

    check_required!(param, cfg_id, cfg_name, cfg_value);

    param.updated_time = Some(LocalTime::now());

    SysConfig::update_by_id(param).await?;

    let res = SysConfig {
        cfg_id,
        ..Default::default()
    };
    emit(rid, ChannelName::ModConfig, &RecChanged::with_update(&res));

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysConfig::delete_by_id(param.id).await?;

    let ev_data = SysConfig {
        cfg_id: Some(param.id),
        ..Default::default()
    };
    emit(
        rid,
        ChannelName::ModConfig,
        &RecChanged::with_delete(&ev_data),
    );

    Resp::ok_with_empty()
}
