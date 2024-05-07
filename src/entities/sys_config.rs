//! 系统配置表
use super::{PageData, PageInfo};
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;

/// 系统接口表
#[table("t_sys_config")]
pub struct SysConfig {
    /// 配置项id
    #[table(id)]
    cfg_id: u32,
    /// 配置项名称
    cfg_name: String,
    /// 配置项内容
    cfg_value: String,
    /// 更新时间
    updated_time: LocalTime,
    /// 配置项备注
    cfg_remark: String,
}

impl SysConfig {
    /// 查询记录
    pub async fn select_page(value: SysConfig, page: PageInfo) -> DbResult<PageData<SysConfig>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_columns(&Self::FIELDS)
            .from(Self::TABLE_NAME)
            .where_sql(|w|
                w.like_opt("", Self::CFG_NAME, value.cfg_name)
                    .like_opt("", Self::CFG_VALUE, value.cfg_value)
                    .like_opt("", Self::CFG_REMARK, value.cfg_remark)
            )
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(n) => n,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0)
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }
}
