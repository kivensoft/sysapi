use anyhow::Result;
use compact_str::format_compact;
use gensql::{table_define, get_conn, query_one_sql, query_all_sql, row_map, vec_value, Queryable, Row, FromValue, table_flatten};
use localtime::LocalTime;
use tokio::sync::OnceCell;

use crate::{
    AppConf, services::{rcache, rmq},
    entities::{sys_permission::SysPermission, sys_dict::{SysDict, DictType}}
};

use super::{PageData, PageInfo};

static SUBSCRIBE_INIT: OnceCell<bool> = OnceCell::const_new();

table_define!{"t_sys_menu", SysMenu,
    menu_id:            u32,
    client_type:        u16,
    menu_code:          String,
    permission_code:    i16,
    menu_name:          String,
    menu_link:          String,
    menu_icon:          String,
    menu_desc:          String,
    updated_time:       LocalTime,
}

table_flatten!{SysMenuVo, SysMenu,
    client_type_name: String,
    permission_name:  String,

    group_code:       i16,
    group_name:       String,

    parent_menu_code: String,
    parent_menu_name: String,

    menus:            Vec<SysMenuVo>,
}


impl SysMenu {
    /// 查询记录
    pub async fn select_page(value: &SysMenu, page: PageInfo) -> Result<PageData<SysMenuVo>> {
        type T = SysMenu;
        type C = SysDict;
        type P = SysPermission;
        type G = SysDict;
        type T1 = SysMenu;

        const T: &str = "t";
        const C: &str = "c";
        const P: &str = "p";
        const G: &str = "g";
        const T1: &str = "t1";

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_slice(T, Self::FIELDS)
                .select_as(C, C::DICT_NAME, SysMenuVo::CLIENT_TYPE_NAME)
                .select_ext(P, P::PERMISSION_NAME)
                .select_as(G, G::DICT_CODE, SysMenuVo::GROUP_CODE)
                .select_as(G, G::DICT_NAME, SysMenuVo::GROUP_NAME)
                .select_as(T1, T1::MENU_NAME, SysMenuVo::PARENT_MENU_NAME)
            .from_alias(Self::TABLE, T)
            .left_join(P::TABLE, P)
                .on_eq(P, P::PERMISSION_CODE, T, T::PERMISSION_CODE)
                .end_join()
            .left_join(T1::TABLE, T1)
                .on(&format_compact!("{}.{} = left({}.{}, length({}.{}) - 2)",
                    T1, T1::MENU_CODE, T, T::MENU_CODE, T, T::MENU_CODE))
                .end_join()
            .left_join(C::TABLE, C)
                .on_eq(C, C::DICT_CODE, T, T::CLIENT_TYPE)
                .on_eq_val(C, C::DICT_TYPE, &(DictType::ClientType as u16))
                .end_join()
            .left_join(G::TABLE, G)
                .on_eq(G, G::DICT_CODE, P, P::GROUP_CODE)
                .on_eq_val(G, G::DICT_TYPE, &(DictType::PermissionGroup as u16))
                .end_join()
            .where_sql()
                .eq_opt(T, Self::CLIENT_TYPE, &value.client_type)
                .eq_opt(T, Self::PERMISSION_CODE, &value.permission_code)
                .like_opt(T, Self::MENU_NAME, &value.menu_name)
                .like_opt(T, Self::MENU_LINK, &value.menu_link)
                .like_right_opt(T, Self::MENU_CODE, &value.menu_code)
                .end_where()
            .order_by(T, Self::MENU_CODE)
            .build_with_page(page.index, page.size, page.total)?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            page.total.unwrap_or(0)
        } else {
            conn.query_one_sql(&tsql, &params).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, |row: Row| {
            let mut row_iter = row.unwrap().into_iter();

            macro_rules! fv {
                () => { FromValue::from_value(row_iter.next().unwrap()) };
            }

            let mut res = SysMenuVo {
                inner: SysMenu {
                    menu_id:         fv!(),
                    client_type:     fv!(),
                    menu_code:       fv!(),
                    permission_code: fv!(),
                    menu_name:       fv!(),
                    menu_link:       fv!(),
                    menu_icon:       fv!(),
                    menu_desc:       fv!(),
                    updated_time:    fv!(),
                },
                client_type_name: fv!(),
                permission_name:  fv!(),
                group_code:       fv!(),
                group_name:       fv!(),
                parent_menu_code: None,
                parent_menu_name: fv!(),
                menus:            None,
            };

            if let Some(mc) = &res.inner.menu_code {
                if mc.len() >= 2 {
                    let pmc = &mc[0..mc.len() - 2];
                    res.parent_menu_code = Some(pmc.to_owned());
                }
            }

            res
        }).await?;

        Ok(PageData { total, list, })
    }

    /// 加载所有记录
    pub async fn select_by_client_type(client_type: u16) -> Result<Vec<SysMenu>> {
        // 首次运行时启动菜单变化的消息订阅处理函数
        SUBSCRIBE_INIT.get_or_init(|| async {
            let channel = rmq::make_channel(rmq::ChannelName::ModMenu);
            let chan_id = rmq::subscribe(channel, |_| async {
                let cache_key_pre = format_compact!("{}:{}:*",
                        AppConf::get().cache_pre, rcache::CK_MENUS);
                let keys = rcache::keys(&cache_key_pre).await?;
                log::trace!("收到菜单变化消息, 删除缓存项: {keys:?}");
                rcache::del(&keys).await?;
                Ok(())
            }).await.expect("订阅菜单变化频道失败");

            if chan_id == 0 {
                log::error!("订阅菜单变化频道失败: 该频道已经被订阅");
            }

            true
        }).await;

        // 优先从缓存中读取
        let cache_key = format_compact!("{}:{}:{}",
                AppConf::get().cache_pre, rcache::CK_MENUS, client_type);
        if let Some(cache_val) = rcache::get(&cache_key).await? {
            rcache::expire(&cache_key, rcache::DEFAULT_TTL as usize).await?;
            return Ok(serde_json::from_str(&cache_val)?);
        }

        type T = SysMenu;
        const FIELDS: [&str; 6] = [
            T::MENU_ID, T::MENU_CODE, T::PERMISSION_CODE,
            T::MENU_NAME, T::MENU_LINK, T::MENU_ICON,
        ];

        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", &FIELDS)
            .from(Self::TABLE)
            .where_sql()
            .eq("", Self::CLIENT_TYPE, &client_type)
            .end_where()
            .order_by("", Self::MENU_CODE)
            .build();

        let menus = query_all_sql(&sql, &params, row_map!(SysMenu,
                menu_id,
                menu_code,
                permission_code,
                menu_name,
                menu_link,
                menu_icon,
            )).await?;

        // 查询结果写入缓存
        let cache_val = serde_json::to_string(&menus)?;
        rcache::set(&cache_key, &cache_val, rcache::DEFAULT_TTL as usize).await?;

        Ok(menus)
    }

    pub async fn select_all() -> Result<Vec<SysMenu>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .order_by("", Self::MENU_CODE)
            .build();
        query_all_sql(sql, params, Self::row_map).await
    }

    pub async fn select_top_level() -> Result<Vec<SysMenu>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
            .add_sql(&format_compact!("and {} like '__'", Self::MENU_CODE))
            .end_where()
            .build();

        query_all_sql(&sql, &params, Self::row_map).await
    }

    pub async fn select_max_code(parent_menu_code: &str) -> Result<Option<String>> {
        let pmc = format!("{}__", parent_menu_code);
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::MENU_CODE))
            .from(Self::TABLE)
            .where_sql()
            .add_value(&format_compact!("{} like ?", Self::MENU_CODE), &pmc)
            .end_where()
            .build();

        query_one_sql(&sql, &params).await
    }

    pub async fn batch_update_rearrange(menus: &[SysMenu]) -> Result<()> {
        let sql = format!("update {} set {} = ? where {} = ?",
                Self::TABLE, Self::MENU_CODE, Self::MENU_ID);

        let mut conn = get_conn().await?;
        let mut conn = conn.start_transaction().await?;
        let trans = &mut conn;

        for item in menus.iter() {
            let params = vec_value![item.menu_code, item.menu_id];
            gensql::log_sql_params(&sql, &params);
            trans.exec_sql(&sql, params).await?;
        }

        conn.commit().await?;

        Ok(())
    }

}
