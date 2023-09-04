use anyhow::Result;
use gensql::{table_define, get_conn, Queryable};
use localtime::LocalTime;

use super::{PageData, PageInfo};


table_define!("t_sys_config", SysConfig,
    cfg_id:         u32         => CFG_ID,
    cfg_name:       String      => CFG_NAME,
    cfg_value:      String      => CFG_VALUE,
    updated_time:   LocalTime   => UPDATED_TIME,
    cfg_remark:     String      => CFG_REMARK,
);

impl SysConfig {
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

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            conn.query_one_sql(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

}
