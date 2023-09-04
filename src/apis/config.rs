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
    let rec = SysConfig::select_by_id(&param.id).await;

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

    let typ = match param.cfg_id {
        Some(_) => rmq::RecordChangedType::Update,
        None => rmq::RecordChangedType::Insert,
    };
    rmq::publish_rec_change_spawm(rmq::ChannelName::ModConfig, typ, SysConfig {
        cfg_id: Some(id),
        ..Default::default()
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
    let r = SysConfig::delete_by_id(&param.id).await;
    check_result!(r);

    rmq::publish_rec_change_spawm(rmq::ChannelName::ModConfig,
        rmq::RecordChangedType::Delete,
        SysConfig {
            cfg_id: Some(param.id),
            ..Default::default()
        }
    );

    Resp::ok_with_empty()
}
