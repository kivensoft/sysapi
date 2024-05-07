//! 权限定义表
use super::{PageData, PageInfo};
use crate::{
    entities::{
        sys_api::SysApi,
        sys_dict::{DictType, SysDict},
        sys_menu::SysMenu,
        sys_role::SysRole,
    },
    utils::bits,
};
use compact_str::format_compact;
use gensql::{table, DbError, DbResult, Queryable, Transaction};
use localtime::LocalTime;
use std::{collections::HashMap, hash::Hash};

#[table("t_sys_permission")]
pub struct SysPermission {
    /// 权限id
    #[table(id)]
    permission_id: u32,
    /// 权限组代码
    group_code: i16,
    /// 权限代码
    permission_code: i16,
    /// 权限名称
    permission_name: String,
    /// 更新时间
    updated_time: LocalTime,
}

#[table]
pub struct SysPermissionVo {
    #[serde(flatten)]
    inner: SysPermission,
    /// 权限组名称
    group_name: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SysPermissionRearrange {
    pub group_code: i16,
    pub permission_codes: Vec<i16>,
}

impl SysPermission {
    /// 查询记录
    pub async fn select_page( value: SysPermission, page: PageInfo) -> DbResult<PageData<SysPermissionVo>> {
        type T = SysPermission;
        type D = SysDict;
        const T: &str = "t";
        const D: &str = "d";

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_columns_with_table(T, &T::FIELDS)
            .select_as(D, D::DICT_NAME, SysPermissionVo::GROUP_NAME)
            .from_alias(T::TABLE_NAME, T)
            .left_join(D::TABLE_NAME, D, |j|
                j.on_eq(D::DICT_CODE, T, T::GROUP_CODE)
                    .on_eq_val(D::DICT_TYPE, DictType::PermissionGroup as u16)
            )
            .where_sql(|w|
                w.eq_opt(T, T::GROUP_CODE, value.group_code)
                    .like_opt(T, T::PERMISSION_NAME, value.permission_name)
            )
            .order_by_with_table(T, T::PERMISSION_CODE)
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(n) => n,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0)
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 查询permission_code最大值
    pub async fn select_max_code() -> DbResult<Option<i16>> {
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::PERMISSION_CODE))
            .from(Self::TABLE_NAME)
            .build();

        gensql::sql_query_one(sql, params).await
    }

    /// 加载所有记录
    pub async fn select_all() -> DbResult<Vec<SysPermission>> {
        let (psql, params) = gensql::SelectSql::new()
            .select_columns(&Self::FIELDS)
            .from(Self::TABLE_NAME)
            .order_by(Self::PERMISSION_CODE)
            .build();
        gensql::sql_query_fast(psql, params).await
    }

    pub async fn rearrange(value: &[SysPermissionRearrange]) -> DbResult<()> {
        // 从数据库中加载相关的权限组, 权限, 接口, 角色的记录集
        let dict_list = SysDict::select_by_type(DictType::PermissionGroup as u16).await?;
        let permission_list = SysPermission::select_all().await?;
        let api_list = SysApi::select_all().await?;
        let role_list = SysRole::select_all().await?;
        let menu_list = SysMenu::select_all().await?;

        // 生成各种辅助数据结构用于加快排序速度
        let dict_map = Self::slice_to_map(&dict_list, |v| v.dict_code.unwrap());
        let permission_map = Self::slice_to_map(&permission_list, |v| v.permission_code.unwrap());
        // 将所有api按权限code进行分组
        let api_map = Self::slice_group(&api_list, |v| v.permission_code.unwrap());
        // 将所有menu进行分组
        let menu_map = Self::slice_group(&menu_list, |v| v.permission_code.unwrap());

        // 保存权限变化后的角色所对应的权限字符串
        let mut role_permissions_list = vec![String::new(); role_list.len()];
        // 保存变动列表
        let mut new_dict_list = Vec::with_capacity(dict_list.len());
        let mut new_permission_list = Vec::with_capacity(permission_list.len());
        let mut new_api_list = Vec::with_capacity(api_list.len());
        let mut new_menu_list = Vec::with_capacity(menu_list.len());

        // 更新数据用到的临时变量
        let mut new_permission_code = 0;

        // 更新权限组, 权限, api
        for (new_group_code, item) in value.iter().enumerate() {
            let dict = *dict_map
                .get(&item.group_code)
                .ok_or_else(|| DbError::Other(format!("权限组[code = {}]丢失", item.group_code).into()))?;

            // 记录字典表权限组的权限组代码变化
            new_dict_list.push((dict.dict_id.unwrap(), new_group_code as i16));

            if item.permission_codes.is_empty() {
                continue;
            }

            // 更新权限组所属的所有权限
            for pcode in item.permission_codes.iter() {
                let permission = *permission_map
                    .get(pcode)
                    .ok_or_else(|| DbError::Other(format!("权限[code = {pcode}]丢失").into()))?;

                // 更新权限的权限组代码及权限代码
                new_permission_list.push((
                    permission.permission_id.unwrap(),
                    new_group_code as i16,
                    new_permission_code as i16,
                ));

                // 更新角色的权限(旧的权限位置移动到新的位置)
                for (role, role_p) in role_list.iter().zip(role_permissions_list.iter_mut()) {
                    let rp = role.permissions.as_ref().unwrap();
                    let bit = bits::get(rp, *pcode as usize);
                    bits::set(role_p, new_permission_code, bit);
                }

                // 更新权限关联的api
                if let Some(sub_api_list) = api_map.get(pcode) {
                    for item in sub_api_list.iter() {
                        new_api_list.push((item.api_id.unwrap(), new_permission_code as i16));
                    }
                }

                // 更新权限关联的菜单
                if let Some(sub_menu_list) = menu_map.get(pcode) {
                    for item in sub_menu_list.iter() {
                        new_menu_list.push((item.menu_id.unwrap(), new_permission_code as i16));
                    }
                }

                new_permission_code += 1;
            }
        }

        // 获取数据库连接
        let mut trans = gensql::start_transaction().await?;

        Self::update_dicts(&mut trans, &new_dict_list).await?;
        Self::update_permissions(&mut trans, &new_permission_list).await?;
        Self::update_apis(&mut trans, &new_api_list).await?;
        Self::update_menus(&mut trans, &new_menu_list).await?;
        Self::update_roles(&mut trans, &role_list, role_permissions_list).await?;

        // 提交事务
        trans.commit().await?;

        Ok(())
    }

    fn slice_to_map<K, V, F>(slice: &[V], f: F) -> HashMap<K, &V>
    where
        K: Eq + Hash,
        F: Fn(&V) -> K,
    {
        slice.iter().map(|v| (f(v), v)).collect()
    }

    fn slice_group<K, V, F>(slice: &[V], f: F) -> HashMap<K, Vec<&V>>
    where
        K: Eq + Hash,
        F: Fn(&V) -> K,
    {
        let mut map: HashMap<_, Vec<_>> = HashMap::new();
        for item in slice.iter() {
            map.entry(f(item))
                .and_modify(|v| v.push(item))
                .or_insert_with(|| vec![item]);
        }

        map
    }

    async fn update_dicts(trans: &mut Transaction<'_>, list: &[(u32, i16)]) -> DbResult<()> {
        type D = SysDict;
        let sql = format!(
            "update {} set {} = ?, {} = ? where {} = ?",
            D::TABLE_NAME,
            D::DICT_CODE,
            D::UPDATED_TIME,
            D::DICT_ID
        );
        let now = LocalTime::now();

        for item in list.iter() {
            let params = gensql::to_values![item.1, now, item.0];
            gensql::db_log_sql_params(&sql, &params);
            trans.exec(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_permissions( trans: &mut Transaction<'_>, list: &[(u32, i16, i16)]) -> DbResult<()> {
        type T = SysPermission;
        let sql = format!(
            "update {} set {} = ?, {} = ?, {} = ? where {} = ?",
            T::TABLE_NAME,
            T::GROUP_CODE,
            T::PERMISSION_CODE,
            T::UPDATED_TIME,
            T::PERMISSION_ID
        );
        let now = LocalTime::now();

        for item in list.iter() {
            let params = gensql::to_values![item.1, item.2, now, item.0];
            gensql::db_log_sql_params(&sql, &params);
            trans.exec(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_apis(trans: &mut Transaction<'_>, list: &[(u32, i16)]) -> DbResult<()> {
        type A = SysApi;
        let sql = format!(
            "update {} set {} = ?, {} = ? where {} = ?",
            A::TABLE_NAME,
            A::PERMISSION_CODE,
            A::UPDATED_TIME,
            A::API_ID
        );
        let now = LocalTime::now();

        for item in list.iter() {
            let params = gensql::to_values![item.1, now, item.0];
            gensql::db_log_sql_params(&sql, &params);
            trans.exec(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_menus(trans: &mut Transaction<'_>, list: &[(u32, i16)]) -> DbResult<()> {
        type T = SysMenu;
        let sql = format!(
            "update {} set {} = ?, {} = ? where {} = ?",
            T::TABLE_NAME,
            T::PERMISSION_CODE,
            T::UPDATED_TIME,
            T::MENU_ID
        );
        let now = LocalTime::now();

        for item in list.iter() {
            let params = gensql::to_values![item.1, now, item.0];
            gensql::db_log_sql_params(&sql, &params);
            trans.exec(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_roles(trans: &mut Transaction<'_>, roles: &[SysRole], ps: Vec<String>) -> DbResult<()> {
        type R = SysRole;
        let sql = format!(
            "update {} set {} = ?, {} = ? where {} = ?",
            R::TABLE_NAME,
            R::PERMISSIONS,
            R::UPDATED_TIME,
            R::ROLE_ID
        );

        let now = LocalTime::now();

        for (role, p) in roles.iter().zip(ps.into_iter()) {
            let params = gensql::to_values![p, now, role.role_id];
            gensql::db_log_sql_params(&sql, &params);
            trans.exec(&sql, params).await?;
        }

        Ok(())
    }
}
