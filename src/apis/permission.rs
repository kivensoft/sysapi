//! 系统权限表接口

use std::collections::HashMap;

use crate::{
    db::{
        PageQuery,
        sys_permission::{SysPermission, SysPermissionRearrange},
        sys_dict::{SysDict, DictType}
    },
    services::rmq
};

use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysPermission>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysPermission::select_page(param.data(), param.page()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysPermission::select_by_id(param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysPermission;
    type Res = SysPermission;

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

    tokio::spawn(async move {
        let msg = SysPermission { permission_id: Some(id), ..Default::default() };
        let chan = rmq::make_channel(rmq::ChannelName::ModPermission);
        let op = match param.permission_id {
            Some(_) => rmq::RecChanged::publish_update(&chan, msg).await,
            None => rmq::RecChanged::publish_insert(&chan, msg).await,
        };
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok( &Res {
        permission_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let r = SysPermission::delete_by_id(param.id).await;
    check_result!(r);

    let chan = rmq::make_channel(rmq::ChannelName::ModPermission);
    rmq::RecChanged::publish_delete(&chan, SysPermission {
        permission_id: Some(param.id),
        ..Default::default()
    }).await?;

    Resp::ok_with_empty()
}

/// 返回权限的字典表
pub async fn items(_ctx: HttpContext) -> HttpResult {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        permissions: Vec<SysPermission>,
    }

    let permissions = check_result!(SysPermission::select_all().await);

    Resp::ok(&Res { permissions })
}

/// 返回权限的树形结构
pub async fn tree(_ctx: HttpContext) -> HttpResult {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res<'a> {
        tree: Vec<TreeItem<'a>>,
    }

    let pg_type = DictType::PermissionGroup as u16;
    let dicts = check_result!(SysDict::select_by_type(pg_type).await);
    let permissions = check_result!(SysPermission::select_all().await);
    let tree = make_tree(&dicts, &permissions);

    Resp::ok(&Res { tree })
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResult {
    type Req = Vec<SysPermissionRearrange>;

    let param: Req = ctx.into_json().await?;
    check_result!(SysPermission::rearrange(&param).await);

    Resp::ok_with_empty()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TreeItem<'a> {
    #[serde(flatten)]
    dict: &'a SysDict,
    permissions: Vec<&'a SysPermission>,
}

fn make_tree<'a>(dicts: &'a [SysDict], permissions: &'a [SysPermission]) -> Vec<TreeItem<'a>> {
    let mut pmap: HashMap<_, Vec<_>> = HashMap::new();
    for item in permissions.iter() {
        pmap.entry(item.group_code.unwrap())
            .and_modify(|v| v.push(item))
            .or_insert_with(|| {
                let mut vec = Vec::new();
                vec.push(item);
                vec
            });
    }

    let result = dicts.iter()
        .map(|dict| {
            let permissions = pmap.remove(&dict.dict_code.unwrap())
                .unwrap_or_else(Vec::new);

            TreeItem {
                dict,
                permissions,
            }
        })
        .collect();

    result
}
