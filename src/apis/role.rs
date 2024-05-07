//! 角色表接口
use crate::{
    entities::{sys_role::SysRole, PageData, PageQuery},
    services::rmq::ChannelName,
    utils::mq_util::{emit, RecChanged},
};
use anyhow_ext::anyhow;
use httpserver::{check_required, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysRole>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysRole::select_page(param.inner, pg)
        .await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysRole::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysRole;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;
    check_required!(param, role_name);

    param.role_id = None;
    if param.role_type.is_none() {
        param.role_type = Some(String::with_capacity(0))
    }
    param.updated_time = Some(LocalTime::now());

    if param.permissions.is_none() {
        param.permissions = Some("".to_owned());
    }

    let id = SysRole::insert(param).await?.1;

    let res = SysRole {
        role_id: Some(id),
        ..Default::default()
    };
    emit(rid, ChannelName::ModRole, &RecChanged::with_insert(&res));

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysRole;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;
    check_required!(param, role_id, role_name);

    if param.role_type.is_none() {
        param.role_type = Some(String::with_capacity(0))
    }
    param.updated_time = Some(LocalTime::now());

    if param.permissions.is_none() {
        param.permissions = Some("".to_owned());
    }

    let role_id = param.role_id;
    SysRole::update_by_id(param).await?;

    let res = SysRole {
        role_id,
        ..Default::default()
    };
    emit(rid, ChannelName::ModRole, &RecChanged::with_update(&res));

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysRole::delete_by_id(param.id).await?;

    emit(
        rid,
        ChannelName::ModRole,
        &RecChanged::with_delete(&SysRole {
            role_id: Some(param.id),
            ..Default::default()
        }),
    );

    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResponse {
    type Req = SysRole;
    type Res = PageData<SysRole>;

    let param: Option<Req> = ctx.parse_json_opt()?;
    let role_type = match param {
        Some(v) => v.role_type,
        None => None,
    };
    let list = SysRole::select_by_role_type(role_type)
        .await?;

    Resp::ok(&Res {
        total: list.len() as u32,
        list,
    })
}
