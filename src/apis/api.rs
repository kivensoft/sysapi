//! 系统接口权限管理

use crate::{
    db::{PageQuery, sys_api::{SysApi, SysApiExt}},
    services::rmq
};
use httpserver::{HttpContext, Resp, HttpResult, check_result};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysApiExt>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysApi::select_page(param.data(), param.page()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysApi::select_by_id(&param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysApi;
    type Res = SysApi;

    let mut param: Req = ctx.into_json().await?;

    param.updated_time = Some(LocalTime::now());

    let id = match param.api_id {
        Some(id) => SysApi::update_by_id(&param).await.map(|_| id),
        None => SysApi::insert(&param).await.map(|(_, id)| id),
    };
    let id = check_result!(id);

    tokio::spawn(async move {
        let msg = SysApi { api_id: Some(id), ..Default::default() };
        let chan = rmq::make_channel(rmq::ChannelName::ModApi);
        let op = match param.api_id {
            Some(_) => rmq::RecChanged::publish_update(&chan, msg).await,
            None => rmq::RecChanged::publish_insert(&chan, msg).await,
        };
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok( &Res {
        api_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let r = SysApi::delete_by_id(&param.id).await;
    check_result!(r);

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::ModApi);
        let op = rmq::RecChanged::publish_delete(&chan, SysApi {
            api_id: Some(param.id),
            ..Default::default()
        }).await;
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok_with_empty()
}

/// 返回权限的树形结构
pub async fn items(_ctx: HttpContext) -> HttpResult {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        apis: Vec<SysApi>,
    }

    let apis = check_result!(SysApi::select_all().await);

    Resp::ok(&Res { apis })
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResult {
    type Req = Vec<SysApi>;

    let mut param: Req = ctx.into_json().await?;
    let now = Some(LocalTime::now());

    for item in param.iter_mut() {
        item.updated_time = now.clone();
    }

    check_result!(SysApi::batch_update_permission_id(&param).await);

    Resp::ok_with_empty()
}
