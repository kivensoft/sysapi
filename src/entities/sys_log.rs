//! 系统审计日志表
//!

use super::{PageData, PageInfo};
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;

/// 系统接口表
#[table("t_sys_log")]
pub struct SysLog {
    /// 配置项id
    #[table(id)]
    log_id: u32,
    /// 配置项分类
    opera_type: String,
    /// 配置项名称
    user_id: u32,
    /// 创建时间
    created_time: LocalTime,
    /// 配置项内容
    opera_text: String,
}

impl SysLog {
    /// 查询记录
    pub async fn select_page(self, page: PageInfo) -> DbResult<PageData<SysLog>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| {
                w.eq_opt("", Self::OPERA_TYPE, self.opera_type)
                    .eq_opt("", Self::USER_ID, self.user_id)
                    .like_opt("", Self::OPERA_TEXT, self.opera_text)
            })
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 添加审计日志
    pub async fn append(category: String, user_id: u32, data: String) -> DbResult<()> {
        let log = Self {
            log_id: None,
            opera_type: Some(category),
            user_id: Some(user_id),
            created_time: Some(LocalTime::now()),
            opera_text: Some(data),
        };

        log.insert().await.map(|_| ())
    }
}
