//! 接口权限管理
use crate::{
    entities::{
        self,
        sys_api::{SysApi, SysApiVo},
        sys_dict::{DictType, SysDict},
        sys_permission::SysPermission,
        PageData, PageQuery,
    },
    services::rmq::ChannelName,
    utils::{
        self,
        mq_util::{emit, RecChanged},
    },
};
use anyhow_ext::anyhow;
use httpserver::{check_required, http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysApiVo>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysApi::select_page(param.inner, pg)
        .await?;

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

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;

    param.api_id = None;
    param.updated_time = Some(LocalTime::now());

    let id = SysApi::insert(param).await?.1;

    let res = SysApi {
        api_id: Some(id),
        ..Default::default()
    };
    emit(rid, ChannelName::ModApi, &RecChanged::with_insert(&res));

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysApi;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;

    check_required!(param, api_id);

    let api_id = param.api_id;
    param.updated_time = Some(LocalTime::now());

    SysApi::update_by_id(param).await?;

    let res = SysApi {
        api_id,
        ..Default::default()
    };
    emit(rid, ChannelName::ModApi, &RecChanged::with_update(&res));

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysApi::delete_by_id(param.id).await?;

    emit(
        rid,
        ChannelName::ModApi,
        &RecChanged::with_delete(&SysApi {
            api_id: Some(param.id),
            ..Default::default()
        }),
    );

    Resp::ok_with_empty()
}

/// 返回所有接口信息
pub async fn items(_ctx: HttpContext) -> HttpResponse {
    type Res = PageData<SysApi>;

    let list = SysApi::select_all().await?;

    Resp::ok(&Res {
        total: list.len() as u32,
        list,
    })
}

/// 重新排序权限
pub async fn repermission(ctx: HttpContext) -> HttpResponse {
    type Req = Vec<SysApi>;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;

    SysApi::batch_update_permission_code(&param)
        .await?;
    emit(rid, ChannelName::ModApi, &RecChanged::<()>::with_all());

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
    SysApi::batch_update_id(&param, all_records).await?;

    Resp::ok_with_empty()
}

/// 返回权限组列表(带内置权限项)
pub async fn groups(_ctx: HttpContext) -> HttpResponse {
    type Res = entities::PageData<SysDict>;

    let mut list = SysDict::select_by_type(DictType::PermissionGroup as u16)
        .await?;

    // 添加内置权限组
    list.insert(
        0,
        SysDict {
            dict_code: Some(utils::INNER_GROUP_CODE),
            dict_name: Some(String::from(utils::INNER_GROUP_NAME)),
            ..Default::default()
        },
    );

    Resp::ok(&Res {
        total: list.len() as u32,
        list,
    })
}

/// 返回权限列表(带内置权限项)
pub async fn permissions(_ctx: HttpContext) -> HttpResponse {
    type Res = entities::PageData<SysPermission>;

    let mut list = SysPermission::select_all().await?;

    // 添加内置权限项
    list.insert(
        0,
        SysPermission {
            group_code: Some(utils::INNER_GROUP_CODE),
            permission_code: Some(utils::ANONYMOUS_PERMIT_CODE),
            permission_name: Some(utils::ANONYMOUS_PERMIT_NAME.to_owned()),
            ..Default::default()
        },
    );
    list.insert(
        1,
        SysPermission {
            group_code: Some(utils::INNER_GROUP_CODE),
            permission_code: Some(utils::PUBLIC_PERMIT_CODE),
            permission_name: Some(utils::PUBLIC_PERMIT_NAME.to_owned()),
            ..Default::default()
        },
    );

    Resp::ok(&Res {
        total: list.len() as u32,
        list,
    })
}
