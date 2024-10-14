//! 角色表接口
use crate::{
    entities::{sys_role::SysRole, PageData, PageQuery},
    utils::audit,
};
use anyhow_ext::anyhow;
use httpserver::{check_required, http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysRole>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysRole::select_page(param.inner, pg).await?;

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
    // 写入数据库
    let id = param.clone().insert_with_notify().await?.1;

    // 写入审计日志
    param.role_id = Some(id);
    param.updated_time = None;
    audit::log_json(audit::ROLE_ADD, ctx.user_id(), &param);

    Resp::ok(&SysRole {
        role_id: Some(id),
        ..Default::default()
    })
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysRole;

    let mut param: Req = ctx.parse_json()?;
    check_required!(param, role_id, role_name);

    let orig = match SysRole::select_by_id(param.role_id.unwrap()).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };

    if param.role_type.is_none() {
        param.role_type = Some(String::with_capacity(0))
    }
    param.updated_time = Some(LocalTime::now());

    if param.permissions.is_none() {
        param.permissions = Some("".to_owned());
    }

    let audit_data = audit::diff(&param, &orig, &[SysRole::ROLE_ID], &[SysRole::UPDATED_TIME]);

    let role_id = param.role_id;
    param.update_with_notify().await?;

    audit::log_text(audit::ROLE_UPD, ctx.user_id(), audit_data);

    Resp::ok(&SysRole {
        role_id,
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let audit_data = match SysRole::select_by_id(param.id).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };
    SysRole::delete_with_notify(param.id).await?;
    audit::log_json(audit::ROLE_DEL, ctx.user_id(), &audit_data);

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
    let list = SysRole::select_by_role_type(role_type).await?;

    Resp::ok(&Res::with_list(list))
}
