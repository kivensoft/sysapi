use anyhow::Result;
use gensql::{table_define, get_conn, Queryable, table_flatten, query_all_sql};
use localtime::LocalTime;

use crate::{entities::sys_permission::SysPermission, utils::bits};

use super::{PageData, PageInfo};

table_define!{"t_sys_role", SysRole,
    role_id:      u32,
    role_type:    String,
    role_name:    String,
    permissions:  String,
    updated_time: LocalTime,
}

table_flatten!{SysRoleVo, SysRole,
    permission_names: Vec<String>,
}

impl SysRole {
    /// 查询记录
    pub async fn select_page(value: &SysRole, page: PageInfo) -> Result<PageData<SysRoleVo>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
                .like_opt("", Self::ROLE_TYPE, &value.role_type)
                .like_opt("", Self::ROLE_NAME, &value.role_name)
                .end_where()
            .build_with_page(page.index, page.size, page.total)?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            page.total.unwrap_or(0)
        } else {
            conn.query_one_sql(&tsql, &params).await?.map(|(total,)| total).unwrap_or(0)
        };

        let mut list = conn.query_all_sql(psql, params, |(
            role_id,
            role_type,
            role_name,
            permissions,
            updated_time,
        )| {
            SysRoleVo {
                inner: SysRole {
                    role_id,
                    role_type,
                    role_name,
                    permissions,
                    updated_time,
                },
                permission_names: None,
            }
        }).await?;

        // 设置每个角色中的权限名称列表 permission_names
        let p_list = SysPermission::select_all().await?;
        for role in list.iter_mut() {
            let bs = bits::string_to_bools(role.inner.permissions.as_ref().unwrap());
            let mut pns = Vec::with_capacity(bs.len());
            // 循环当前角色的权限标志位，如果设置了标志，则加入到权限名称数组中
            for (i, b) in bs.iter().enumerate() {
                if *b {
                    let p_idx = p_list.binary_search_by_key(&(i as i16), |p| p.permission_code.unwrap());
                    if let Ok(p_idx) = p_idx {
                        pns.push(p_list[p_idx].permission_name.as_ref().unwrap().clone());
                    }
                }
            }
            role.permission_names = Some(pns);
        }

        Ok(PageData { total, list, })
    }

    /// 加载所有记录
    pub async fn select_all() -> Result<Vec<SysRole>> {
        Self::select_by_role_type(None).await
    }

    /// 加载指定类型的角色列表
    pub async fn select_by_role_type(role_type: Option<String>) -> Result<Vec<SysRole>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
                .eq_opt("", Self::ROLE_TYPE, &role_type)
                .end_where()
            .build();

        query_all_sql(sql, params, Self::row_map).await
    }

}
