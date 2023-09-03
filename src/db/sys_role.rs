use anyhow::Result;
use gensql::table_define;
use localtime::LocalTime;
use mysql_async::prelude::Queryable;

use super::{PageData, PageInfo};

table_define!("t_sys_role", SysRole,
    role_id:        u32       => ROLE_ID,
    client_type:    u8        => CLIENT_TYPE,
    role_name:      String    => ROLE_NAME,
    permissions:    String    => PERMISSIONS,
    updated_time:   LocalTime => UPDATED_TIME,
);

impl SysRole {
    /// 删除记录
    pub async fn delete_by_id(id: u32) -> Result<u32> {
        super::exec_sql(&Self::stmt_delete(&id)).await
    }

    /// 插入记录，返回(插入记录数量, 自增ID的值)
    pub async fn insert(value: &SysRole) -> Result<(u32, u32)> {
        super::insert_sql(&Self::stmt_insert(value)).await
    }

    /// 更新记录
    pub async fn update_by_id(value: &SysRole) -> Result<u32> {
        super::exec_sql(&Self::stmt_update(value)).await
    }

    /// 查询记录
    pub async fn select_by_id(id: u32) -> Result<Option<SysRole>> {
        Ok(super::query_one_sql(&Self::stmt_select(&id)).await?.map(Self::row_map))
    }

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

        let mut conn = super::get_conn().await?;
        let total = if tsql.is_empty() {
            0
        } else {
            conn.exec_first(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.exec_map(psql, params, Self::row_map).await?;

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
