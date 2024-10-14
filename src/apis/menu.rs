//! 菜单表接口
use crate::{
    entities::{
        sys_menu::{SysMenu, SysMenuExt, SysMenuVo, TOP_MENU_CODE},
        PageData, PageQuery,
    },
    utils::audit,
};
use anyhow_ext::{anyhow, Result};
use httpserver::{check_required, http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysMenu>;

    let mut param: Req = ctx.parse_json()?;
    if let Some(menu_code) = param.inner.menu_code.as_mut() {
        if menu_code == TOP_MENU_CODE {
            menu_code.clear();
        }
        menu_code.push_str("__");
    }
    let pg = param.page_info();
    let page_data = SysMenu::select_page(param.inner, pg).await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysMenu::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysMenuVo;

    let mut param: Req = ctx.parse_json()?;

    let param_ext = &mut param.inner;
    let param_base = &mut param_ext.inner;

    check_required!(
        param_base,
        client_type,
        permission_code,
        menu_name,
        menu_link
    );
    check_required!(param_ext, parent_menu_code);

    param_base.menu_id = None;
    param_base.updated_time = Some(LocalTime::now());

    let pmc = param_ext.parent_menu_code.as_ref().unwrap();
    if param_base.menu_code.is_none() || !param_base.menu_code.as_ref().unwrap().starts_with(pmc) {
        let max_code = SysMenu::select_max_code(pmc).await?;
        let next_code = match max_code {
            Some(v) => {
                let s = &v[v.len() - 2..];
                log::debug!("s = {s}");
                let n = match s.parse::<u32>() {
                    Ok(n) => n + 1,
                    Err(e) => {
                        log::error!("parse {s} to u32 error: {e:?}");
                        return Resp::internal_server_error();
                    }
                };
                if n >= 100 {
                    return Resp::fail("子菜单项数量不能超过100");
                }
                format!("{n:02}")
            }
            None => "01".to_owned(),
        };
        param_base.menu_code = Some(format!("{pmc}{next_code}"));
    }

    let id = param.inner.inner.clone().insert_with_notify().await?.1;

    param.inner.inner.menu_id = Some(id);
    param.inner.inner.updated_time = None;
    audit::log_json(audit::MENU_ADD, ctx.user_id(), &param.inner.inner);

    Resp::ok(&SysMenu {
        menu_id: Some(id),
        ..Default::default()
    })
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysMenuVo;

    let mut param: Req = ctx.parse_json()?;
    let param_ext = &mut param.inner;
    let param_base = &mut param_ext.inner;

    check_required!(
        param_base,
        menu_id,
        client_type,
        permission_code,
        menu_name,
        menu_link
    );
    check_required!(param_ext, parent_menu_code);

    let audit_orig = match SysMenu::select_by_id(param_base.menu_id.unwrap()).await? {
        Some(v) => v,
        None => http_bail!("菜单不存在"),
    };

    param_base.updated_time = Some(LocalTime::now());

    let pmc = param_ext.parent_menu_code.as_ref().unwrap();
    if param_base.menu_code.is_none() || !param_base.menu_code.as_ref().unwrap().starts_with(pmc) {
        let max_code = SysMenu::select_max_code(pmc).await?;
        let next_code = match max_code {
            Some(v) => {
                let s = &v[v.len() - 2..];
                log::debug!("s = {s}");
                let n = match s.parse::<u32>() {
                    Ok(n) => n + 1,
                    Err(e) => {
                        log::error!("parse {s} to u32 error: {e:?}");
                        return Resp::internal_server_error();
                    }
                };
                if n >= 100 {
                    return Resp::fail("子菜单项数量不能超过100");
                }
                format!("{n:02}")
            }
            None => "01".to_owned(),
        };
        param_base.menu_code = Some(format!("{pmc}{next_code}"));
    }

    let menu_id = param_base.menu_id;
    let audit_diff = audit::diff(
        param_base,
        &audit_orig,
        &[SysMenu::MENU_ID],
        &[SysMenu::UPDATED_TIME],
    );
    SysMenu::update_with_notify(param.inner.inner).await?;
    audit::log_text(audit::MENU_UPD, ctx.user_id(), audit_diff);

    Resp::ok(&SysMenu {
        menu_id,
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let audit_data = match SysMenu::select_by_id(param.id).await? {
        Some(menu) => menu,
        None => http_bail!("记录不存在"),
    };

    SysMenu::delete_with_notify(param.id).await?;
    audit::log_json(audit::MENU_DEL, ctx.user_id(), &audit_data);

    Resp::ok_with_empty()
}

/// 返回权限的树形结构
pub async fn top_level(_ctx: HttpContext) -> HttpResponse {
    type Res = PageData<SysMenu>;

    let list = SysMenu::select_top_level(true).await?;

    Resp::ok(&Res::with_list(list))
}

/// 返回权限的树形结构
pub async fn tree(ctx: HttpContext) -> HttpResponse {
    type Req = SysMenu;
    type Res = PageData<SysMenuVo>;

    let param: Req = ctx.parse_json()?;
    check_required!(param, client_type);

    let menus = SysMenu::select_by_client_type(param.client_type.unwrap()).await?;

    let mut menu_map: HashMap<_, Vec<_>> = HashMap::new();
    for item in menus.iter() {
        let menu_code = match &item.menu_code {
            Some(s) => s,
            None => return Resp::fail("菜单项有编码为空的记录"),
        };
        let parent_code = &menu_code[0..menu_code.len() - 2];

        menu_map
            .entry(parent_code)
            .and_modify(|v| v.push(item))
            .or_insert_with(|| vec![item]);
    }

    let mut top_menu = SysMenuVo {
        inner: SysMenuExt {
            inner: SysMenu {
                menu_code: Some("".to_owned()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    build_tree(&mut top_menu, &menu_map)?;

    let list = top_menu.menus.unwrap_or_default();

    Resp::ok(&Res::with_list(list))
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResponse {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        client_type: i16,
        menus: Vec<SysMenuVo>,
    }

    let param: Req = ctx.parse_json()?;

    let mut list = Vec::new();
    tree_to_list(&mut list, "", &param.menus);

    SysMenu::batch_update_rearrange(param.client_type, &list).await?;
    audit::log_json(audit::MENU_REARRANGE, ctx.user_id(), &param);

    Resp::ok_with_empty()
}

fn build_tree<'a>(
    menu: &'a mut SysMenuVo,
    menu_map: &'a HashMap<&'a str, Vec<&'a SysMenu>>,
) -> Result<()> {
    let menu_code = menu.inner.inner.menu_code.as_ref().unwrap();

    if let Some(children) = menu_map.get(menu_code.as_str()) {
        menu.menus = Some(
            children
                .iter()
                .map(|v| SysMenuVo {
                    inner: SysMenuExt {
                        inner: (*v).clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect(),
        );

        for item in menu.menus.as_mut().unwrap().iter_mut() {
            build_tree(item, menu_map)?;
        }
    }

    Ok(())
}

/// 递归调用，将树形结构转换为列表结构
fn tree_to_list(list: &mut Vec<SysMenu>, parent_menu_code: &str, tree_menus: &[SysMenuVo]) {
    for (i, item) in tree_menus.iter().enumerate() {
        let menu_code = format!("{parent_menu_code}{:02}", i + 1);
        list.push(SysMenu {
            menu_id: item.inner.inner.menu_id,
            menu_code: Some(menu_code.to_string()),
            ..Default::default()
        });

        if let Some(menus) = &item.menus {
            tree_to_list(list, &menu_code, menus);
        }
    }
}
