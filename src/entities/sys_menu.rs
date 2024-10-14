//! 菜单表
use std::{collections::HashMap, sync::Arc};

use super::{PageData, PageInfo};
use crate::{
    entities::{
        sys_dict::{DictType, SysDict},
        sys_permission::SysPermission,
    },
    services::gmc,
};
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;

pub const TOP_MENU_CODE: &str = "00";
pub const TOP_MENU_NAME: &str = "<顶级菜单>";
pub const TOP_MENU_CLIENT_TYPE: i16 = -1;

const CATEGORY: &str = crate::utils::consts::gmc::SYS_MENU;

/// 系统接口表
#[table("t_sys_menu")]
pub struct SysMenu {
    /// 菜单id
    #[table(id)]
    menu_id: u32,
    /// 客户端类型
    client_type: i16,
    /// 菜单代码
    menu_code: String,
    /// 权限代码
    permission_code: i16,
    /// 菜单名称
    menu_name: String,
    /// 菜单链接
    menu_link: String,
    /// 菜单图标
    menu_icon: String,
    /// 菜单描述
    menu_desc: String,
    /// 更新时间
    updated_time: LocalTime,
}

#[table]
pub struct SysMenuExt {
    #[serde(flatten)]
    inner: SysMenu,
    /// 客户端类型名称
    client_type_name: String,
    /// 权限名称
    permission_name: String,
    /// 权限组代码
    group_code: i8,
    /// 权限组名称
    group_name: String,
    /// 父菜单代码
    parent_menu_code: String,
    /// 父菜单名称
    parent_menu_name: String,
}

#[table]
pub struct SysMenuVo {
    #[serde(flatten)]
    pub inner: SysMenuExt, // 子菜单列表
    #[table(ignore)]
    pub menus: Vec<SysMenuVo>, // 子菜单列表
}

impl SysMenu {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let client_type = self.client_type;
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(client_type).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let client_type = self.client_type;
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(client_type).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(record) => {
                let client_type = record.client_type;
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(client_type).await;
                }
                ret
            }
            None => Ok(false),
        }
    }

    /// 数据变更通知
    pub async fn notify_changed(ty: Option<i16>) {
        let ty = match ty {
            Some(n) => format!("{n}"),
            None => String::new(),
        };
        crate::services::gmc::get_cache().notify(CATEGORY, &ty).await
    }

    /// 系统菜单分页查询
    ///
    /// Arguments:
    ///
    /// * `value`: 查询参数
    /// * `page`: 查询分页参数
    ///
    ///
    pub async fn select_page(value: SysMenu, page: PageInfo) -> DbResult<PageData<SysMenuVo>> {
        type T = SysMenu;
        type C = SysDict;
        type P = SysPermission;
        type G = SysDict;

        let (t, c, p, g, t1) = ("t", "c", "p", "g", "t1");

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_all_with_table(t)
            .select_as(c, C::DICT_NAME, SysMenuExt::CLIENT_TYPE_NAME)
            .select(p, P::PERMISSION_NAME)
            .select_as(g, G::DICT_CODE, SysMenuExt::GROUP_CODE)
            .select_as(g, G::DICT_NAME, SysMenuExt::GROUP_NAME)
            .select_as(t1, T::MENU_NAME, SysMenuExt::PARENT_MENU_NAME)
            .from_as(Self::TABLE_NAME, t)
            .left_join(P::TABLE_NAME, p, |j| {
                j.on_eq(P::PERMISSION_CODE, t, T::PERMISSION_CODE)
            })
            .left_join(T::TABLE_NAME, t1, |j| {
                j.on(&format!(
                    "{}.{} = left({}.{1}, length({2}.{1}) - 2)",
                    t1,
                    T::MENU_CODE,
                    t
                ))
            })
            .left_join(C::TABLE_NAME, c, |j| {
                j.on_eq(C::DICT_CODE, t, T::CLIENT_TYPE)
                    .on_eq_val(C::DICT_TYPE, DictType::ClientCategory as u16)
            })
            .left_join(G::TABLE_NAME, g, |j| {
                j.on_eq(G::DICT_CODE, p, P::GROUP_CODE)
                    .on_eq_val(G::DICT_TYPE, DictType::PermissionGroup as u16)
            })
            .where_sql(|w| {
                w.eq_opt(t, T::CLIENT_TYPE, value.client_type)
                    .eq_opt(t, T::PERMISSION_CODE, value.permission_code)
                    .like_opt(t, T::MENU_NAME, value.menu_name)
                    .like_opt(t, T::MENU_LINK, value.menu_link)
                    .expr_opt(t, T::MENU_CODE, "like", value.menu_code)
            })
            .order_by(t, T::MENU_CODE)
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 根据客户端类型异步查询系统菜单列表，优先从缓存中获取数据，并在首次运行时启动菜单变化的消息订阅处理函数。
    ///
    /// Arguments:
    ///
    /// * `client_type`: 客户端类型，表示正在检索其菜单项的客户端类型。该函数获取菜单项
    pub async fn select_by_client_type(client_type: i16) -> DbResult<Arc<Vec<SysMenu>>> {
        let mut buf = itoa::Buffer::new();
        let ct_str = buf.format(client_type);
        if let Some(list) = gmc::get_cache().get_json(CATEGORY, ct_str).await {
            log::trace!("加载菜单项时使用缓存: client_type = {}", client_type);
            return Ok(list);
        }

        // 使用gensql构建SQL查询语句和参数
        let (sql, params) = gensql::SelectSql::new()
            .from(SysMenu::TABLE_NAME)
            .where_sql(|w| w.eq("", SysMenu::CLIENT_TYPE, client_type))
            .order_by("", SysMenu::MENU_CODE)
            .build();

        // 执行SQL查询并转换结果到SysMenu对象列表
        let menus = Arc::new(gensql::sql_query_fast(sql, params).await?);
        // 将查询结果写入缓存
        if !menus.is_empty() {
            gmc::get_cache().put_json(CATEGORY, ct_str, menus.clone()).await;
        }

        // 返回查询结果
        Ok(menus)
    }

    /// 获取所有菜单
    pub async fn select_all() -> DbResult<Vec<SysMenu>> {
        // 构建SQL语句
        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .order_by("", Self::MENU_CODE)
            .build();
        // 执行SQL查询并返回结果
        gensql::sql_query_fast(sql, params).await
    }

    /// 获取顶级菜单列表
    pub async fn select_top_level(use_top_level: bool) -> DbResult<Vec<SysMenu>> {
        // 构建SQL语句
        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| w.add_sql(&format!("and {} like '__'", Self::MENU_CODE)))
            .build();

        // 执行SQL查询
        let mut list = gensql::sql_query_fast(sql, params).await?;

        if use_top_level {
            list.insert(
                0,
                SysMenu {
                    menu_code: Some(TOP_MENU_CODE.to_string()),
                    menu_name: Some(TOP_MENU_NAME.to_string()),
                    menu_link: Some("#".to_string()),
                    client_type: Some(TOP_MENU_CLIENT_TYPE),
                    ..Default::default()
                },
            );
            list.retain(|v| v.menu_link.as_ref().unwrap() == "#");
        };

        Ok(list)
    }

    /// 查询指定父菜单代码的子菜单最大代码值
    ///
    /// Arguments:
    ///
    /// * `parent_menu_code`: 父菜单代码
    ///
    /// Returns:
    ///
    /// * `Option<String>` 子菜单最大代码值
    /// * `None` 不存在子菜单
    pub async fn select_max_code(parent_menu_code: &str) -> DbResult<Option<String>> {
        let pmc = format!("{}__", parent_menu_code);
        let (sql, params) = gensql::SelectSql::new()
            .select("", &format!("max({})", Self::MENU_CODE))
            .from(Self::TABLE_NAME)
            .where_sql(
                |w| w.add_value(&format!("{} like ?", Self::MENU_CODE), &pmc), // 添加条件，代码以指定字符串开头
            )
            .build();

        gensql::sql_query_one(sql, params).await // 执行查询并等待结果
    }

    /// 批量更新菜单排列顺序
    ///
    /// Arguments:
    ///
    /// * `client_type`: 要更新的菜单分类
    /// * `menus`: 要更新的菜单记录
    ///
    pub async fn batch_update_rearrange(client_type: i16, menus: &[SysMenu]) -> DbResult<()> {
        // 加载原有记录，与要变更的记录比较，只修改有变化的
        let old_menus = Self::select_by_client_type(client_type).await?;
        let old_menus: HashMap<_, _> = old_menus
            .iter()
            .map(|v| (v.menu_id.unwrap(), v.menu_code.as_ref().unwrap()))
            .collect();
        let menus: Vec<_> = menus
            .iter()
            .filter(|v| match old_menus.get(v.menu_id.as_ref().unwrap()) {
                Some(code) => *code != v.menu_code.as_ref().unwrap(),
                None => true,
            })
            .collect();

        // 更新变化的数据，使用事务模式
        let sql = format!(
            "update {} set {} = ? where {} = ?",
            Self::TABLE_NAME,
            Self::MENU_CODE,
            Self::MENU_ID
        );
        let mut trans = gensql::start_transaction().await?;
        for item in menus.iter() {
            let params = gensql::to_values![item.menu_code, item.menu_id];
            gensql::db_log_sql_params(&sql, &params);
            trans.exec(&sql, params).await?;
        }
        trans.commit().await?;
        Self::notify_changed(None).await;

        Ok(())
    }

}
