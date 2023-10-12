use anyhow::Result;
use gensql::{table_define, get_conn, Queryable, FastStr};
use localtime::LocalTime;

use super::{PageData, PageInfo};


table_define!{"t_sys_config", SysConfig,
    cfg_id:         u32,
    cfg_name:       FastStr,
    cfg_value:      FastStr,
    updated_time:   LocalTime,
    cfg_remark:     FastStr,
}

impl SysConfig {
    /// 查询记录
    pub async fn select_page(value: &SysConfig, page: PageInfo) -> Result<PageData<SysConfig>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
                .like_opt("", Self::CFG_NAME, &value.cfg_name)
                .like_opt("", Self::CFG_VALUE, &value.cfg_value)
                .like_opt("", Self::CFG_REMARK, &value.cfg_remark)
                .end_where()
            .build_with_page(page.index, page.size, page.total)?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            page.total.unwrap_or(0)
        } else {
            conn.query_one_sql(&tsql, &params).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

}
