//! 菜单表
use super::{PageData, PageInfo};
use crate::{
    entities::{
        sys_dict::{DictType, SysDict},
        sys_permission::SysPermission,
    },
    services::{rcache, rmq},
    AppConf,
};
use compact_str::format_compact;
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;
use tokio::sync::OnceCell;

static SUBSCRIBE_INIT: OnceCell<bool> = OnceCell::const_new();

/// 系统接口表
#[table("t_sys_menu")]
pub struct SysMenu {
    /// 菜单id
    #[table(id)]
    menu_id: u32,
    /// 客户端类型
    client_type: u16,
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
    group_code: i16,
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
    /// 系统菜单分页查询
    ///
    /// Arguments:
    ///
    /// * `value`: 查询参数
    /// * `page`: 查询分页参数
    ///
    /// Returns:
    ///
    /// 分页结果列表
    pub async fn select_page(value: SysMenu, page: PageInfo) -> DbResult<PageData<SysMenuVo>> {
        type T = SysMenu;
        type C = SysDict;
        type P = SysPermission;
        type G = SysDict;

        let (t, c, p, g, t1) = ("t", "c", "p", "g", "t1");

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_columns_with_table(t, &Self::FIELDS)
            .select_as(c, C::DICT_NAME, SysMenuExt::CLIENT_TYPE_NAME)
            .select_with_table(p, P::PERMISSION_NAME)
            .select_as(g, G::DICT_CODE, SysMenuExt::GROUP_CODE)
            .select_as(g, G::DICT_NAME, SysMenuExt::GROUP_NAME)
            .select_as(t1, T::MENU_NAME, SysMenuExt::PARENT_MENU_NAME)
            .from_alias(Self::TABLE_NAME, t)
            .left_join(P::TABLE_NAME, p, |j|
                j.on_eq(P::PERMISSION_CODE, t, T::PERMISSION_CODE)
            )
            .left_join(T::TABLE_NAME, t1, |j|
                j.on(&format_compact!(
                    "{}.{} = left({}.{1}, length({2}.{1}) - 2)",
                    t1,
                    T::MENU_CODE,
                    t
                ))
            )
            .left_join(C::TABLE_NAME, c, |j|
                j.on_eq(C::DICT_CODE, t, T::CLIENT_TYPE)
                    .on_eq_val(C::DICT_TYPE, DictType::ClientType as u16)
            )
            .left_join(G::TABLE_NAME, g, |j|
                j.on_eq(G::DICT_CODE, p, P::GROUP_CODE)
                    .on_eq_val(G::DICT_TYPE, DictType::PermissionGroup as u16)
            )
            .where_sql(|w|
                w.eq_opt(t, T::CLIENT_TYPE, value.client_type)
                    .eq_opt(t, T::PERMISSION_CODE, value.permission_code)
                    .like_opt(t, T::MENU_NAME, value.menu_name)
                    .like_opt(t, T::MENU_LINK, value.menu_link)
                    .like_right_opt(t, T::MENU_CODE, value.menu_code)
            )
            .order_by_with_table(t, T::MENU_CODE)
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(n) => n,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0)
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 根据客户端类型异步查询系统菜单列表，优先从缓存中获取数据，并在首次运行时启动菜单变化的消息订阅处理函数。
    ///
    /// Arguments:
    ///
    /// * `client_type`: 客户端类型
    /// 16 位整数 (‘u16’)，表示正在检索其菜单项的客户端类型。该函数获取菜单项
    ///
    /// Returns:
    ///
    /// 查询成功则返回指定客户端类型的系统菜单列表，若失败则返回错误信息。
    pub async fn select_by_client_type(client_type: u16) -> DbResult<Vec<SysMenu>> {
        // 首次调用时初始化菜单消息订阅处理函数
        SUBSCRIBE_INIT.get_or_init(subscribe_init).await;

        // 尝试从缓存中读取菜单项
        let cache_key = format_compact!(
            "{}:{}:{}",
            AppConf::get().cache_pre,
            rcache::CK_MENUS,
            client_type
        );
        if let Some(cache_val) = rcache::json_lz4_get(&cache_key).await {
            // 设置缓存过期时间并直接返回缓存中的菜单项
            rcache::expire(&cache_key, rcache::DEFAULT_TTL as i64).await;
            log::trace!("加载菜单项时使用缓存: client_type = {}", client_type);
            return Ok(cache_val);
        }

        // 定义查询字段
        const FIELDS: [&str; 6] = [
            SysMenu::MENU_ID,
            SysMenu::MENU_CODE,
            SysMenu::PERMISSION_CODE,
            SysMenu::MENU_NAME,
            SysMenu::MENU_LINK,
            SysMenu::MENU_ICON,
        ];

        // 使用gensql构建SQL查询语句和参数
        let (sql, params) = gensql::SelectSql::new()
            .select_columns_with_table("", &FIELDS)
            .from(SysMenu::TABLE_NAME)
            .where_sql(|w|
                w.eq("", SysMenu::CLIENT_TYPE, client_type)
            )
            .order_by_with_table("", SysMenu::MENU_CODE)
            .build();

        // 执行SQL查询并转换结果到SysMenu对象列表
        let menus = gensql::sql_query_fast(sql, params).await?;

        // 将查询结果写入缓存
        rcache::json_lz4_set(&cache_key, &menus, rcache::DEFAULT_TTL as u64).await;

        // 返回查询结果
        Ok(menus)
    }

    /// 获取所有系统菜单
    ///
    pub async fn select_all() -> DbResult<Vec<SysMenu>> {
        // 构建SQL语句
        let (sql, params) = gensql::SelectSql::new()
            .select_columns(&Self::FIELDS)
            .from(Self::TABLE_NAME)
            .order_by(Self::MENU_CODE)
            .build();
        // 执行SQL查询并返回结果
        gensql::sql_query_fast(sql, params).await
    }

    /// 获取顶级菜单列表
    pub async fn select_top_level() -> DbResult<Vec<SysMenu>> {
        // 构建SQL语句
        let (sql, params) = gensql::SelectSql::new()
            .select_columns(&Self::FIELDS)
            .from(Self::TABLE_NAME)
            .where_sql(|w|
                w.add_sql(&format_compact!("and {} like '__'", Self::MENU_CODE))
            )
            .build();

        // 执行SQL查询
        gensql::sql_query_fast(sql, params).await
    }

    /// 查询指定父菜单代码的子菜单最大代码值
    ///
    /// Arguments:
    ///
    /// * `parent_menu_code`: 父菜单代码
    ///
    /// Returns:
    ///
    /// `Option<String>` 子菜单最大代码值
    ///
    /// `None` 不存在子菜单
    pub async fn select_max_code(parent_menu_code: &str) -> DbResult<Option<String>> {
        let pmc = format!("{}__", parent_menu_code);
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::MENU_CODE))
            .from(Self::TABLE_NAME)
            .where_sql(|w|
                w.add_value(&format_compact!("{} like ?", Self::MENU_CODE), &pmc) // 添加条件，代码以指定字符串开头
            )
            .build();

        gensql::sql_query_one(sql, params).await // 执行查询并等待结果
    }

    /// 批量更新菜单排列顺序
    ///
    /// Arguments:
    ///
    /// * `menus`: 要更新的菜单记录
    ///
    pub async fn batch_update_rearrange(menus: &[SysMenu]) -> DbResult<()> {
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

        Ok(())
    }
}

// 订阅菜单变化频道，实现菜单变化时清空缓存
async fn subscribe_init() -> bool {
    let channel = rmq::make_channel(rmq::ChannelName::ModMenu);
    let chan_id = rmq::subscribe(channel, |_| async move {
        let cache_key_pre = format_compact!("{}:{}:*", AppConf::get().cache_pre, rcache::CK_MENUS);
        let keys = rcache::keys(&cache_key_pre).await;
        log::trace!("收到菜单变化消息, 删除缓存项: {keys:?}");
        if let Some(keys) = keys {
            rcache::del(&keys).await;
        }
        Ok(())
    })
    .await
    .expect("订阅菜单变化频道失败");

    if chan_id == 0 {
        log::error!("订阅菜单变化频道失败: 该频道已经被订阅");
    }

    true
}
