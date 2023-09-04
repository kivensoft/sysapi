use anyhow::Result;
use compact_str::format_compact;
use gensql::{table_define, get_conn, query_one_sql, query_all_sql, row_map, vec_value, Queryable};
use localtime::LocalTime;
use serde::{Serialize, Deserialize};
use tokio::sync::OnceCell;

use crate::{AppConf, services::{rcache, rmq}};

use super::{PageData, PageInfo};

static SUBSCRIBE_INIT: OnceCell<bool> = OnceCell::const_new();

table_define!("t_sys_menu", SysMenu,
    menu_id:            u32       => MENU_ID,
    client_type:        u16       => CLIENT_TYPE,
    menu_code:          String    => MENU_CODE,
    permission_code:    i32       => PERMISSION_CODE,
    menu_name:          String    => MENU_NAME,
    menu_link:          String    => MENU_LINK,
    menu_icon:          String    => MENU_ICON,
    menu_desc:          String    => MENU_DESC,
    updated_time:       LocalTime => UPDATED_TIME,
);

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SysMenuExt {
    #[serde(flatten)]
    pub inner: SysMenu,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_menu_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub menus: Option<Vec<SysMenuExt>>,
}

impl SysMenu {
    /// 查询记录
    pub async fn select_page(value: &SysMenu, page: PageInfo) -> Result<PageData<SysMenu>> {
        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq_opt("", Self::CLIENT_TYPE, &value.client_type)
            .and_eq_opt("", Self::PERMISSION_CODE, &value.permission_code)
            .and_like_opt("", Self::MENU_NAME, &value.menu_name)
            .and_like_opt("", Self::MENU_LINK, &value.menu_link)
            .and_like_right_opt("", Self::MENU_CODE, &value.menu_code)
            .end_where()
            .order_by("", Self::MENU_CODE)
            .build_with_page()?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            conn.query_one_sql(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

    /// 加载所有记录
    pub async fn select_by_client_type(client_type: u16) -> Result<Vec<SysMenu>> {
        // 首次运行时启动菜单变化的消息订阅处理函数
        SUBSCRIBE_INIT.get_or_init(|| async {
            let channel = rmq::make_channel(rmq::ChannelName::ModMenu);
            let sub_succeed = rmq::subscribe(&channel, |_| async {
                let cache_key_pre = format_compact!("{}:{}:*",
                        AppConf::get().cache_pre, rcache::CK_MENUS);
                let keys = rcache::keys(&cache_key_pre).await?;
                log::trace!("收到菜单变化消息, 删除缓存项: {keys:?}");
                rcache::del(&keys).await?;
                Ok(())
            }).await.expect("订阅菜单变化频道失败");

            if !sub_succeed {
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
            .and_eq("", Self::CLIENT_TYPE, &client_type)
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
        Self::select_page(&SysMenu::default(), PageInfo::new())
            .await
            .map(|v| v.list)
    }

    pub async fn select_top_level() -> Result<Vec<SysMenu>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", &Self::fields())
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
