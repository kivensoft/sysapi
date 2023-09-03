//! 角色表接口

use crate::{db::{PageQuery, sys_role::SysRole}, services::rmq};
use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysRole>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysRole::select_page(param.data(), param.page()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysRole::select_by_id(param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysRole;
    type Res = SysRole;

    let mut param: Req = ctx.into_json().await?;
    check_required!(param, client_type, role_name);

    param.updated_time = Some(LocalTime::now());

    if param.permissions.is_none() {
        param.permissions = Some("".to_owned());
    }

    let id = check_result!(match param.role_id {
        Some(id) => SysRole::update_by_id(&param).await.map(|_| id),
        None => SysRole::insert(&param).await.map(|(_, id)| id),
    });

    tokio::spawn(async move {
        let msg = SysRole { role_id: Some(id), ..Default::default() };
        let chan = rmq::make_channel(rmq::ChannelName::ModRole);
        let op = match param.role_id {
            Some(_) => rmq::RecChanged::publish_update(&chan, msg).await,
            None => rmq::RecChanged::publish_insert(&chan, msg).await,
        };
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });


    Resp::ok( &Res {
        role_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    check_result!(SysRole::delete_by_id(param.id).await);

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::ModRole);
        let op = rmq::RecChanged::publish_delete(&chan, SysRole {
            role_id: Some(param.id),
            ..Default::default()
        }).await;
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResult {
    type Req = SysRole;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        roles: Vec<SysRole>,
    }

    let param: Option<Req> = ctx.into_opt_json().await?;
    let client_type = match param {
        Some(v) => v.client_type,
        None => None,
    };
    let rec = SysRole::select_by_type(client_type).await;
    let rec = check_result!(rec);

    Resp::ok(&Res { roles: rec })
}
