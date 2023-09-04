use anyhow::Result;
use gensql::{table_define, get_conn, Queryable};
use localtime::LocalTime;

use super::{PageData, PageInfo};

table_define!("t_sys_role", SysRole,
    role_id:        u32       => ROLE_ID,
    client_type:    u8        => CLIENT_TYPE,
    role_name:      String    => ROLE_NAME,
    permissions:    String    => PERMISSIONS,
    updated_time:   LocalTime => UPDATED_TIME,
);

impl SysRole {
    /// 查询记录
    pub async fn select_page(value: &SysRole, page: PageInfo) -> Result<PageData<SysRole>> {
        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq_opt("", Self::CLIENT_TYPE, &value.client_type)
            .and_like_opt("", Self::ROLE_NAME, &value.role_name)
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

    /// 加载所有记录
    pub async fn select_all() -> Result<Vec<SysRole>> {
        Self::select_by_type(None).await
    }

    /// 加载指定类型的角色列表
    pub async fn select_by_type(client_type: Option<u8>) -> Result<Vec<SysRole>> {
        Self::select_page(&SysRole {
                client_type,
                ..Default::default()
            }, PageInfo::new())
            .await
            .map(|v| v.list)
    }

}
