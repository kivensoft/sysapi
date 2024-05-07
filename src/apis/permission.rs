//! 权限定义表接口
use crate::{
    entities::{
        sys_dict::{DictType, SysDict},
        sys_permission::{SysPermission, SysPermissionRearrange},
        PageData, PageQuery,
    },
    services::rmq::ChannelName,
    utils::mq_util::{emit, RecChanged},
};
use anyhow_ext::anyhow;
use httpserver::{check_required, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TreeItem<'a> {
    #[serde(flatten)]
    dict: &'a SysDict,
    permissions: Vec<&'a SysPermission>,
}

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysPermission>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysPermission::select_page(param.inner, pg)
        .await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysPermission::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysPermission;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;
    check_required!(param, group_code, permission_code, permission_name);

    param.permission_id = None;
    param.updated_time = Some(LocalTime::now());

    // 新增记录需要设置permission_code的值为最大值 + 1
    let max_code = SysPermission::select_max_code().await?;
    param.permission_code = Some(max_code.map_or(0, |v| v + 1));

    let id = SysPermission::insert(param).await?.1;

    let res = SysPermission {
        permission_id: Some(id),
        ..Default::default()
    };
    emit(
        rid,
        ChannelName::ModPermission,
        &RecChanged::with_insert(&res),
    );

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysPermission;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;
    check_required!(
        param,
        permission_id,
        group_code,
        permission_code,
        permission_name
    );

    let permission_id = param.permission_id;

    param.updated_time = Some(LocalTime::now());

    SysPermission::update_by_id(param).await?;

    let res = SysPermission {
        permission_id,
        ..Default::default()
    };
    emit(
        rid,
        ChannelName::ModPermission,
        &RecChanged::with_update(&res),
    );

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysPermission::delete_by_id(param.id).await?;

    emit(
        rid,
        ChannelName::ModPermission,
        &RecChanged::with_delete(&SysPermission {
            permission_id: Some(param.id),
            ..Default::default()
        }),
    );

    Resp::ok_with_empty()
}

/// 返回权限的字典表
pub async fn items(_ctx: HttpContext) -> HttpResponse {
    type Res = PageData<SysPermission>;

    let list = SysPermission::select_all().await?;

    Resp::ok(&Res {
        total: list.len() as u32,
        list,
    })
}

/// 返回权限的树形结构
pub async fn tree(_ctx: HttpContext) -> HttpResponse {
    type Res<'a> = PageData<TreeItem<'a>>;

    let pg_type = DictType::PermissionGroup as u16;
    let dicts = SysDict::select_by_type(pg_type).await?;
    let permissions = SysPermission::select_all().await?;
    let tree = make_tree(&dicts, &permissions);

    Resp::ok(&Res {
        total: tree.len() as u32,
        list: tree,
    })
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResponse {
    type Req = Vec<SysPermissionRearrange>;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysPermission::rearrange(&param).await?;

    emit(
        rid,
        ChannelName::ModPermission,
        &RecChanged::<()>::with_all(),
    );
    emit(rid, ChannelName::ModApi, &RecChanged::<()>::with_all());
    emit(rid, ChannelName::ModRole, &RecChanged::<()>::with_all());
    emit(rid, ChannelName::ModMenu, &RecChanged::<()>::with_all());

    Resp::ok_with_empty()
}

fn make_tree<'a>(dicts: &'a [SysDict], permissions: &'a [SysPermission]) -> Vec<TreeItem<'a>> {
    let mut pmap: HashMap<_, Vec<_>> = HashMap::new();
    for item in permissions.iter() {
        pmap.entry(item.group_code.unwrap())
            .and_modify(|v| v.push(item))
            .or_insert_with(|| vec![item]);
    }

    let result = dicts
        .iter()
        .map(|dict| {
            let permissions = pmap.remove(&dict.dict_code.unwrap()).unwrap_or_default();

            TreeItem { dict, permissions }
        })
        .collect();

    result
}
