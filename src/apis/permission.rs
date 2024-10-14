//! 权限定义表接口
use crate::{
    apis::UseBuilltinReq,
    entities::{
        sys_dict::{SysDict, BUILTIN_GROUP_CODE},
        sys_permission::{
            SysPermission, SysPermissionRearrange, BUILTIN_ANONYMOUS_CODE, BUILTIN_ANONYMOUS_NAME,
            BUILTIN_PUBLIC_CODE, BUILTIN_PUBLIC_NAME,
        },
        PageData, PageQuery,
    },
    utils::{audit, IntStr},
};
use anyhow_ext::anyhow;
use httpserver::{check_required, http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TreeItem<'a> {
    #[serde(flatten)]
    group: &'a IntStr,
    permissions: Vec<&'a SysPermission>,
}

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysPermission>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysPermission::select_page(param.inner, pg).await?;

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

    let mut param: Req = ctx.parse_json()?;
    check_required!(param, group_code, permission_code, permission_name);

    param.permission_id = None;
    param.updated_time = Some(LocalTime::now());

    // 新增记录需要设置permission_code的值为最大值 + 1
    let max_code = SysPermission::select_max_code().await?;
    param.permission_code = Some(max_code.map_or(0, |v| v + 1));

    let mut audit_data = param.clone();
    // 写入数据库
    let id = SysPermission::insert_with_notify(param).await?.1;

    // 写入审计日志
    audit_data.permission_id = Some(id);
    audit_data.updated_time = None;
    audit::log_json(audit::PERMISSIONS_ADD, ctx.user_id(), &audit_data);

    Resp::ok(&SysPermission {
        permission_id: Some(id),
        ..Default::default()
    })
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysPermission;

    let mut param: Req = ctx.parse_json()?;
    check_required!(param, permission_id, permission_name);

    let permission_id = param.permission_id.unwrap();
    let orig = match SysPermission::select_by_id(permission_id).await? {
        Some(r) => r,
        None => http_bail!("记录不存在"),
    };

    param.updated_time = Some(LocalTime::now());
    let audit_diff = audit::diff(
        &param,
        &orig,
        &[SysPermission::PERMISSION_ID],
        &[SysPermission::UPDATED_TIME],
    );
    // 写入数据库
    param.update_with_notify().await?;

    // 写入审计日志
    audit::log_text(audit::PERMISSIONS_UPD, ctx.user_id(), audit_diff);

    Resp::ok(&SysPermission {
        permission_id: Some(permission_id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let orig = match SysPermission::select_by_id(param.id).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };
    // 写入数据库
    SysPermission::delete_with_notify(param.id).await?;
    // 写入审计日志
    audit::log_json(audit::PERMISSIONS_DEL, ctx.user_id(), &orig);

    Resp::ok_with_empty()
}

/// 返回权限的字典表
pub async fn items(ctx: HttpContext) -> HttpResponse {
    type Req = UseBuilltinReq;
    type Res = PageData<SysPermission>;

    let param = ctx.parse_json_opt::<Req>()?;
    let use_builltin = match param {
        Some(p) => p.use_builtin.unwrap_or(false),
        None => false,
    };

    let mut list = SysPermission::select_all().await?;
    if use_builltin {
        list.insert(
            0,
            SysPermission {
                group_code: Some(BUILTIN_GROUP_CODE),
                permission_code: Some(BUILTIN_ANONYMOUS_CODE),
                permission_name: Some(BUILTIN_ANONYMOUS_NAME.to_string()),
                ..Default::default()
            },
        );
        list.insert(
            1,
            SysPermission {
                group_code: Some(BUILTIN_GROUP_CODE),
                permission_code: Some(BUILTIN_PUBLIC_CODE),
                permission_name: Some(BUILTIN_PUBLIC_NAME.to_string()),
                ..Default::default()
            },
        );
    }

    Resp::ok(&Res::with_list(list))
}

/// 返回权限的树形结构
pub async fn tree(_ctx: HttpContext) -> HttpResponse {
    type Res<'a> = PageData<TreeItem<'a>>;

    let groups = SysDict::get_permission_groups(false).await?;
    let permissions = SysPermission::select_all().await?;
    let tree = make_tree(&groups, &permissions);

    Resp::ok(&Res::with_list(tree))
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResponse {
    type Req = Vec<SysPermissionRearrange>;

    let param: Req = ctx.parse_json()?;
    // 写入数据库
    SysPermission::rearrange(&param).await?;
    // 写入审计日志
    audit::log_json(audit::PERMISSIONS_REARRANGE, ctx.user_id(), &param);
    Resp::ok_with_empty()
}

fn make_tree<'a>(groups: &'a [IntStr], permissions: &'a [SysPermission]) -> Vec<TreeItem<'a>> {
    let mut pmap: HashMap<_, Vec<_>> = HashMap::new();
    for item in permissions.iter() {
        pmap.entry(item.group_code.unwrap())
            .and_modify(|v| v.push(item))
            .or_insert_with(|| vec![item]);
    }

    let result = groups
        .iter()
        .map(|group| {
            let permissions = pmap.remove(&(group.key as i8)).unwrap_or_default();
            TreeItem { group, permissions }
        })
        .collect();

    result
}
