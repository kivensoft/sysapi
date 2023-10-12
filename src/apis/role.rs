//! 角色表接口

use crate::{entities::{PageQuery, sys_role::SysRole, PageData}, services::rmq};
use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysRole>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysRole::select_page(param.data(), param.to_page_info()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysRole::select_by_id(&param.id).await;

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
    check_required!(param, role_name);

    if param.role_type.is_none() {
        param.role_type = Some(String::with_capacity(0))
    }
    param.updated_time = Some(LocalTime::now());

    if param.permissions.is_none() {
        param.permissions = Some("".to_owned());
    }

    let id = check_result!(match param.role_id {
        Some(id) => SysRole::update_by_id(&param).await.map(|_| id),
        None => SysRole::insert(&param).await.map(|(_, id)| id),
    });

    let typ = match param.role_id {
        Some(_) => rmq::RecordChangedType::Update,
        None => rmq::RecordChangedType::Insert,
    };
    rmq::publish_rec_change_spawm(rmq::ChannelName::ModRole, typ, SysRole {
        role_id: Some(id),
        ..Default::default()
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
    check_result!(SysRole::delete_by_id(&param.id).await);

    rmq::publish_rec_change_spawm(rmq::ChannelName::ModRole,
        rmq::RecordChangedType::Delete,
        SysRole {
            role_id: Some(param.id),
            ..Default::default()
        }
    );

    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResult {
    type Req = SysRole;
    type Res = PageData<SysRole>;

    let param: Option<Req> = ctx.into_opt_json().await?;
    let role_type = match param {
        Some(v) => v.role_type,
        None => None,
    };
    let list = check_result!(SysRole::select_by_role_type(role_type).await);

    Resp::ok(&Res { total: list.len() as u32, list, })
}
