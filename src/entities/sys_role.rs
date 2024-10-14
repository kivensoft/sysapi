//! 角色表
use super::{PageData, PageInfo};
use crate::{entities::sys_permission::SysPermission, utils::{bits, consts}};
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;

#[table("t_sys_role")]
pub struct SysRole {
    /// 角色id
    #[table(id)]
    role_id: u32,
    /// 角色类型
    role_type: String,
    /// 角色名称
    role_name: String,
    /// 角色权限位集
    permissions: String,
    /// 更新时间
    updated_time: LocalTime,
}

#[table]
pub struct SysRoleVo {
    #[serde(flatten)]
    pub inner: SysRole,
    /// 角色权限位集对应的权限名称集合
    #[table(ignore)]
    pub permission_names: Vec<String>,
}

impl SysRole {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let role_id = self.role_id;
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(role_id).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let role_id = self.role_id;
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(role_id).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(record) => {
                let role_id = record.role_id;
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(role_id).await;
                }
                ret
            }
            None => Ok(false),
        }
    }

    pub async fn notify_changed(id: Option<u32>) {
        let id = match id {
            Some(n) => format!("{n}"),
            None => String::new(),
        };
        crate::services::gmc::get_cache().notify(consts::gmc::SYS_ROLE, &id).await
    }

    /// 查询记录
    pub async fn select_page(value: SysRole, page: PageInfo) -> DbResult<PageData<SysRoleVo>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| {
                w.like_opt("", Self::ROLE_TYPE, value.role_type).like_opt(
                    "",
                    Self::ROLE_NAME,
                    value.role_name,
                )
            })
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let mut list: Vec<SysRoleVo> = conn.query_fast(psql, params).await?;

        // 设置每个角色中的权限名称列表 permission_names
        let p_list = SysPermission::select_all().await?;
        for role in list.iter_mut() {
            let bs = bits::string_to_bools(role.inner.permissions.as_ref().unwrap());
            let mut pns = Vec::with_capacity(bs.len());

            // 循环当前角色的权限标志位，如果设置了标志，则加入到权限名称数组中
            for (i, b) in bs.iter().enumerate() {
                if *b {
                    let i = i as i16;
                    let p_idx = p_list.binary_search_by_key(&i, |p| p.permission_code.unwrap());
                    if let Ok(p_idx) = p_idx {
                        pns.push(p_list[p_idx].permission_name.as_ref().unwrap().clone());
                    }
                }
            }

            role.permission_names = Some(pns);
        }

        Ok(PageData { total, list })
    }

    /// 加载所有记录
    pub async fn select_all() -> DbResult<Vec<SysRole>> {
        Self::select_by_role_type(None).await
    }

    /// 加载指定类型的角色列表
    pub async fn select_by_role_type(role_type: Option<String>) -> DbResult<Vec<SysRole>> {
        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| w.eq_opt("", Self::ROLE_TYPE, role_type))
            .build();

        gensql::sql_query_fast(sql, params).await
    }

}
