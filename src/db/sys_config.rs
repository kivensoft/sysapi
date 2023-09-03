use anyhow::Result;
use gensql::table_define;
use localtime::LocalTime;
use mysql_async::prelude::Queryable;

use super::{PageData, PageInfo};


table_define!("t_sys_config", SysConfig,
    cfg_id:         u32         => CFG_ID,
    cfg_name:       String      => CFG_NAME,
    cfg_value:      String      => CFG_VALUE,
    updated_time:   LocalTime   => UPDATED_TIME,
    cfg_remark:     String      => CFG_REMARK,
);

impl SysConfig {
    /// 删除记录
    pub async fn delete_by_id(id: u32) -> Result<u32> {
        super::exec_sql(&Self::stmt_delete(&id)).await
    }

    /// 插入记录，返回(插入记录数量, 自增ID的值)
    pub async fn insert(value: &SysConfig) -> Result<(u32, u32)> {
        super::insert_sql(&Self::stmt_insert(value)).await
    }

    /// 更新记录
    pub async fn update_by_id(value: &SysConfig) -> Result<u32> {
        super::exec_sql(&Self::stmt_update(value)).await
    }

    /// 查询记录
    pub async fn select_by_id(id: u32) -> Result<Option<SysConfig>> {
        let rec = super::query_one_sql(&Self::stmt_select(&id)).await?.map(Self::row_map);
        Ok(rec)
    }

    /// 查询记录
    pub async fn select_page(value: &SysConfig, page: PageInfo) -> Result<PageData<SysConfig>> {
        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_like_opt("", Self::CFG_NAME, &value.cfg_name)
            .and_like_opt("", Self::CFG_VALUE, &value.cfg_value)
            .and_like_opt("", Self::CFG_REMARK, &value.cfg_remark)
            .end_where()
            .build_with_page()?;

        let mut conn = super::get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            conn.exec_first(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.exec_map(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

}
