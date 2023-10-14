//! 系统接口权限管理

use crate::{
    entities::{
        sys_api::{SysApi, SysApiVo},
        sys_dict::{DictType, SysDict},
        sys_permission::SysPermission,
        PageQuery, PageData, self
    },
    services::rmq::{ChannelName},
    utils::{self, pub_rec::{RecChanged, type_from_id, emit}}
};
use gensql::FastStr;
use httpserver::{HttpContext, Resp, HttpResult, check_result};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysApiVo>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysApi::select_page(param.data(), param.to_page_info()).await;
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

    let mut param: Req = ctx.into_json().await?;

    param.updated_time = Some(LocalTime::now());

    let id = match param.api_id {
        Some(id) => SysApi::update_by_id(&param).await.map(|_| id),
        None => SysApi::insert(&param).await.map(|(_, id)| id),
    };
    let id = check_result!(id);

    let res = SysApi { api_id: Some(id), ..Default::default() };
    let type_ = type_from_id(&param.api_id);
    emit(ChannelName::ModApi, &RecChanged::new(type_, &res));

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let r = SysApi::delete_by_id(&param.id).await;
    check_result!(r);

    emit(ChannelName::ModApi, &RecChanged::with_delete(&SysApi {
        api_id: Some(param.id),
        ..Default::default()
    }));

    Resp::ok_with_empty()
}

/// 返回所有接口信息
pub async fn items(_ctx: HttpContext) -> HttpResult {
    type Res = PageData<SysApi>;

    let list = check_result!(SysApi::select_all().await);

    Resp::ok(&Res { total: list.len() as u32, list })
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
    emit(ChannelName::ModApi, &RecChanged::<()>::with_all());

    Resp::ok_with_empty()
}

/// 返回权限组列表(带内置权限项)
pub async fn groups(_ctx: HttpContext) -> HttpResult {
    type Res = entities::PageData<SysDict>;

    let groups = SysDict::select_by_type(DictType::PermissionGroup as u16).await;
    let groups = check_result!(groups);

    // 添加内置权限组
    let mut list = Vec::with_capacity(groups.len() + 1);
    list.push(SysDict {
        dict_code: Some(utils::INNER_GROUP_CODE),
        dict_name: Some(FastStr::new(utils::INNER_GROUP_NAME)),
        ..Default::default()
    });
    for item in groups.into_iter() {
        list.push(item);
    }

    Resp::ok(&Res { total: list.len() as u32, list })
}

/// 返回权限列表(带内置权限项)
pub async fn permissions(_ctx: HttpContext) -> HttpResult {
    type Res = entities::PageData<SysPermission>;

    let apis = check_result!(SysPermission::select_all().await);

    // 添加内置权限项
    let mut list = Vec::with_capacity(apis.len() + 1);
    list.push(SysPermission {
        group_code: Some(utils::INNER_GROUP_CODE),
        permission_code: Some(utils::ANONYMOUS_PERMIT_CODE),
        permission_name: Some(utils::ANONYMOUS_PERMIT_NAME.to_owned()),
        ..Default::default()
    });
    list.push(SysPermission {
        group_code: Some(utils::INNER_GROUP_CODE),
        permission_code: Some(utils::PUBLIC_PERMIT_CODE),
        permission_name: Some(utils::PUBLIC_PERMIT_NAME.to_owned()),
        ..Default::default()
    });
    for item in apis.into_iter() {
        list.push(item);
    }

    Resp::ok(&Res { total: list.len() as u32, list })
}
