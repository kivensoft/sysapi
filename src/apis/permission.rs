//! 系统权限表接口

use std::collections::HashMap;

use crate::{
    entities::{
        PageQuery, PageData,
        sys_permission::{SysPermission, SysPermissionRearrange},
        sys_dict::{SysDict, DictType}
    },
    services::rmq::{ChannelName},
    utils::pub_rec::{type_from_id, emit, RecChanged}
};

use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TreeItem<'a> {
    #[serde(flatten)]
    dict: &'a SysDict,
    permissions: Vec<&'a SysPermission>,
}

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysPermission>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysPermission::select_page(param.data(), param.to_page_info()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysPermission::select_by_id(&param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysPermission;

    let mut param: Req = ctx.into_json().await?;
    check_required!(param, group_code, permission_code, permission_name);

    param.updated_time = Some(LocalTime::now());

    // 新增记录需要设置permission_code的值为最大值 + 1
    if param.permission_id.is_none() {
        let max_code = SysPermission::select_max_code().await;
        param.permission_code = Some(check_result!(max_code).map_or(0, |v| v + 1));
    }

    let id = check_result!(match param.permission_id {
        Some(id) => SysPermission::update_by_id(&param).await.map(|_| id),
        None => SysPermission::insert(&param).await.map(|(_, id)| id),
    });

    let res = SysPermission { permission_id: Some(id), ..Default::default() };
    let type_ = type_from_id(&param.permission_id);
    emit(ChannelName::ModConfig, &RecChanged::new(type_, &res));

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let r = SysPermission::delete_by_id(&param.id).await;
    check_result!(r);

    emit(ChannelName::ModPermission, &RecChanged::with_delete(&SysPermission {
        permission_id: Some(param.id),
        ..Default::default()
    }));

    Resp::ok_with_empty()
}

/// 返回权限的字典表
pub async fn items(_ctx: HttpContext) -> HttpResult {
    type Res = PageData<SysPermission>;

    let list = check_result!(SysPermission::select_all().await);

    Resp::ok(&Res { total: list.len() as u32, list, })
}

/// 返回权限的树形结构
pub async fn tree(_ctx: HttpContext) -> HttpResult {
    type Res<'a> = PageData<TreeItem<'a>>;

    let pg_type = DictType::PermissionGroup as u16;
    let dicts = check_result!(SysDict::select_by_type(pg_type).await);
    let permissions = check_result!(SysPermission::select_all().await);
    let tree = make_tree(&dicts, &permissions);

    Resp::ok(&Res { total: tree.len() as u32, list: tree, })
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResult {
    type Req = Vec<SysPermissionRearrange>;

    let param: Req = ctx.into_json().await?;
    check_result!(SysPermission::rearrange(&param).await);

    emit(ChannelName::ModPermission, &RecChanged::<()>::with_all());
    emit(ChannelName::ModApi, &RecChanged::<()>::with_all());
    emit(ChannelName::ModRole, &RecChanged::<()>::with_all());
    emit(ChannelName::ModMenu, &RecChanged::<()>::with_all());

    Resp::ok_with_empty()
}

fn make_tree<'a>(dicts: &'a [SysDict], permissions: &'a [SysPermission]) -> Vec<TreeItem<'a>> {
    let mut pmap: HashMap<_, Vec<_>> = HashMap::new();
    for item in permissions.iter() {
        pmap.entry(item.group_code.unwrap())
            .and_modify(|v| v.push(item))
            .or_insert_with(|| vec![item]);
    }

    let result = dicts.iter()
        .map(|dict| {
            let permissions = pmap.remove(&(dict.dict_code.unwrap() as i16))
                .unwrap_or_default();

            TreeItem {
                dict,
                permissions,
            }
        })
        .collect();

    result
}
