//! 接口权限管理
use crate::{
    entities::{
        self,
        sys_api::{SysApi, SysApiVo},
        sys_dict::{DictType, SysDict, BUILTIN_GROUP_CODE, BUILTIN_GROUP_NAME},
        sys_permission::{
            SysPermission, BUILTIN_ANONYMOUS_CODE, BUILTIN_ANONYMOUS_NAME, BUILTIN_PUBLIC_CODE,
            BUILTIN_PUBLIC_NAME,
        },
        PageData, PageQuery,
    },
    utils::audit,
};
use anyhow_ext::anyhow;
use httpserver::{check_required, http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysApiVo>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysApi::select_page(param.inner, pg).await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysApi::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysApi;

    let mut param: Req = ctx.parse_json()?;

    param.api_id = None;
    param.updated_time = Some(LocalTime::now());

    // 写入数据库
    let id = param.clone().insert_with_notify().await?.1;
    // 写入审计日志
    param.api_id = Some(id);
    param.updated_time = None;
    audit::log_json(audit::API_ADD, ctx.user_id(), &param);

    Resp::ok(&SysApi {
        api_id: Some(id),
        ..Default::default()
    })
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysApi;

    let mut param: Req = ctx.parse_json()?;

    check_required!(param, api_id);

    let api_id = param.api_id.unwrap();
    let orig = match SysApi::select_by_id(api_id).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };
    param.updated_time = Some(LocalTime::now());
    let audit_data = audit::diff(&param, &orig, &[SysApi::API_ID], &[SysApi::UPDATED_TIME]);
    // 写入数据库
    param.update_with_notify().await?;

    // 写入审计日志
    audit::log_text(audit::API_UPD, ctx.user_id(), audit_data);

    Resp::ok(&SysApi {
        api_id: Some(api_id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;

    let audit_data = match SysApi::select_by_id(param.id).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };

    SysApi::delete_with_notify(param.id).await?;
    audit::log_json(audit::API_DEL, ctx.user_id(), &audit_data);

    Resp::ok_with_empty()
}

/// 返回所有接口信息
pub async fn items(_ctx: HttpContext) -> HttpResponse {
    type Res = PageData<SysApi>;

    let list = SysApi::select_all().await?;
    Resp::ok(&Res::with_list(list))
}

/// 重新排序权限
pub async fn repermission(ctx: HttpContext) -> HttpResponse {
    type Req = Vec<SysApi>;

    let param: Req = ctx.parse_json()?;

    SysApi::batch_update_permission_code(&param).await?;

    Resp::ok_with_empty()
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResponse {
    type Req = Vec<u32>;

    let param: Req = ctx.parse_json()?;

    let all_records = SysApi::select_all().await?;
    if all_records.len() != param.len() {
        http_bail!("记录已经变动, 请刷新后重新排序");
    }

    SysApi::batch_update_id(&param, all_records.clone()).await?;
    audit::log_json(audit::API_REARRANGE, ctx.user_id(), &all_records);

    Resp::ok_with_empty()
}

/// 返回权限组列表(带内置权限项)
pub async fn groups(_ctx: HttpContext) -> HttpResponse {
    type Res = entities::PageData<SysDict>;

    let list = SysDict::select_by_type(DictType::PermissionGroup as u8).await?;
    let mut list = list.as_ref().clone();

    // 添加内置权限组
    let dict = SysDict {
        dict_code: Some(BUILTIN_GROUP_CODE.to_string()),
        dict_name: Some(String::from(BUILTIN_GROUP_NAME)),
        ..Default::default()
    };
    list.insert(0, dict);

    Resp::ok(&Res::with_list(list))
}

/// 返回权限列表(带内置权限项)
pub async fn permissions(_ctx: HttpContext) -> HttpResponse {
    type Res = entities::PageData<SysPermission>;

    let mut list = SysPermission::select_all().await?;

    // 添加内置权限项
    list.insert(
        0,
        SysPermission {
            group_code: Some(BUILTIN_GROUP_CODE),
            permission_code: Some(BUILTIN_ANONYMOUS_CODE),
            permission_name: Some(BUILTIN_ANONYMOUS_NAME.to_owned()),
            ..Default::default()
        },
    );
    list.insert(
        1,
        SysPermission {
            group_code: Some(BUILTIN_GROUP_CODE),
            permission_code: Some(BUILTIN_PUBLIC_CODE),
            permission_name: Some(BUILTIN_PUBLIC_NAME.to_owned()),
            ..Default::default()
        },
    );

    Resp::ok(&Res::with_list(list))
}
