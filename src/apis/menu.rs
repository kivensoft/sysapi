//! 菜单表接口

use std::collections::HashMap;

use crate::{
    db::{PageQuery, sys_menu::{SysMenu, SysMenuExt}},
    services::rmq
};

use anyhow::Result;
use compact_str::format_compact;
use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysMenu>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysMenu::select_page(param.data(), param.to_page_info()).await;
    let page_data = check_result!(page_data);

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysMenu::select_by_id(&param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysMenuExt;
    type Res = SysMenu;

    let mut param: Req = ctx.into_json().await?;

    check_required!(param.inner, client_type, permission_code,
            menu_name, menu_link);
    check_required!(param, parent_menu_code);

    param.inner.updated_time = Some(LocalTime::now());

    let pmc = param.parent_menu_code.as_ref().unwrap();
    if param.inner.menu_code.is_none()
            || !param.inner.menu_code.as_ref().unwrap().starts_with(pmc) {
        let max_code = SysMenu::select_max_code(pmc).await;
        let next_code = match check_result!(max_code) {
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
            },
            None => "01".to_owned(),
        };
        param.inner.menu_code = Some(format!("{pmc}{next_code}"));
    }

    let id = check_result!(match param.inner.menu_id {
        Some(id) => SysMenu::update_by_id(&param.inner).await.map(|_| id),
        None => SysMenu::insert(&param.inner).await.map(|(_, id)| id),
    });

    let typ = match param.inner.menu_id {
        Some(_) => rmq::RecordChangedType::Update,
        None => rmq::RecordChangedType::Insert,
    };
    rmq::publish_rec_change_spawm(rmq::ChannelName::ModMenu, typ, SysMenu {
        menu_id: Some(id),
        ..Default::default()
    });

    Resp::ok( &Res {
        menu_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let op = SysMenu::delete_by_id(&param.id).await;
    check_result!(op);

    rmq::publish_rec_change_spawm(rmq::ChannelName::ModMenu,
        rmq::RecordChangedType::Delete,
        SysMenu {
            menu_id: Some(param.id),
            ..Default::default()
        }
    );

    Resp::ok_with_empty()
}

/// 返回权限的树形结构
pub async fn top_level(_ctx: HttpContext) -> HttpResult {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        menus: Vec<SysMenu>,
    }

    let menus = check_result!(SysMenu::select_top_level().await);

    Resp::ok(&Res { menus })
}

/// 返回权限的树形结构
pub async fn tree(ctx: HttpContext) -> HttpResult {
    type Req = SysMenu;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        menus: Vec<SysMenuExt>,
    }

    let param: Req = ctx.into_json().await?;
    check_required!(param, client_type);

    let menus = SysMenu::select_by_client_type(param.client_type.unwrap()).await;
    let menus = check_result!(menus);

    let mut menu_map :HashMap<_, Vec<_>> = HashMap::new();
    for item in menus.iter() {
        let menu_code = match &item.menu_code {
            Some(s) => s,
            None => return Resp::fail("菜单项有编码为空的记录"),
        };
        let parent_code = &menu_code[0..menu_code.len() - 2];

        menu_map.entry(parent_code)
            .and_modify(|v| v.push(item))
            .or_insert_with(|| {
                vec![item]
            });
    }

    let mut top_menu = SysMenuExt {
        inner: SysMenu {
            menu_code: Some("".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    };
    build_tree(&mut top_menu, &menu_map)?;


    Resp::ok(&Res { menus: top_menu.menus.unwrap_or_default() })
}

/// 重新排序权限
pub async fn rearrange(ctx: HttpContext) -> HttpResult {
    type Req = Vec<SysMenuExt>;

    let param: Req = ctx.into_json().await?;

    let mut list = Vec::new();
    tree_to_list(&mut list, "", &param);
    check_result!(SysMenu::batch_update_rearrange(&list).await);

    rmq::publish_rec_change_spawm::<Option<()>>(rmq::ChannelName::ModMenu,
        rmq::RecordChangedType::All, None);


    Resp::ok_with_empty()
}

fn build_tree<'a>(menu: &'a mut SysMenuExt,
        menu_map: &'a HashMap<&'a str, Vec<&'a SysMenu>>) -> Result<()> {

    let menu_code = menu.inner.menu_code.as_ref().unwrap();

    if let Some(children) = menu_map.get(menu_code.as_str()) {
        menu.menus = Some(
            children.iter().map(|v| SysMenuExt{
                inner: (*v).clone(),
                parent_menu_code: None,
                menus: None,
            }).collect()
        );

        for item in menu.menus.as_mut().unwrap().iter_mut() {
            build_tree(item, menu_map)?;
        }
    }

    Ok(())
}

fn tree_to_list(list: &mut Vec<SysMenu>, parent_menu_code: &str,
        tree_menus: &[SysMenuExt]) {

    for (i, item) in tree_menus.iter().enumerate() {
        let menu_code = format_compact!("{parent_menu_code}{:02}", i + 1);
        list.push(SysMenu {
            menu_id: item.inner.menu_id,
            menu_code: Some(menu_code.to_string()),
            ..Default::default()
        });

        if let Some(menus) = &item.menus {
            tree_to_list(list, &menu_code, menus);
        }
    }
}
