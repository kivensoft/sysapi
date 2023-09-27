use std::{collections::HashMap, hash::Hash};

use anyhow::{Result, Context};
use compact_str::format_compact;
use gensql::{table_define, Transaction, get_conn, query_one_sql, vec_value, Queryable};
use localtime::LocalTime;

use crate::{
    db::{
        sys_dict::{SysDict, DictType},
        sys_api::SysApi,
        sys_menu::SysMenu,
        sys_role::SysRole,
    },
    utils::bits,
};

use super::{PageData, PageInfo};

table_define!("t_sys_permission", SysPermission,
    permission_id:      u32       => PERMISSION_ID,
    group_code:         u16       => GROUP_CODE,
    permission_code:    u16       => PERMISSION_CODE,
    permission_name:    String    => PERMISSION_NAME,
    updated_time:       LocalTime => UPDATED_TIME,
);

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SysPermissionRearrange {
    pub group_code: u16,
    pub permission_codes: Vec<u16>,
}

impl SysPermission {
    /// 查询记录
    pub async fn select_page(value: &SysPermission, page: PageInfo) -> Result<PageData<SysPermission>> {
        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq_opt("", Self::GROUP_CODE, &value.group_code)
            .and_like_opt("", Self::PERMISSION_NAME, &value.permission_name)
            .end_where()
            .order_by("", Self::PERMISSION_CODE)
            .build_with_page()?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            match page.total {
                Some(total) => total,
                None => conn.query_one_sql(tsql, params.clone())
                        .await?
                        .map(|(total,)| total)
                        .unwrap_or(0),
            }
        };

        let list = conn.query_all_sql(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

    /// 查询permission_code最大值
    pub async fn select_max_code() -> Result<Option<u16>> {
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::PERMISSION_CODE))
            .from(Self::TABLE)
            .build();

        query_one_sql(&sql, &params).await
    }

    /// 加载所有记录
    pub async fn select_all() -> Result<Vec<SysPermission>> {
        Self::select_page(&SysPermission::default(), PageInfo::new())
            .await
            .map(|v| v.list)
    }

    pub async fn rearrange(value: &[SysPermissionRearrange]) -> Result<()> {
        // 从数据库中加载相关的权限组, 权限, 接口, 角色的记录集
        let dict_list = SysDict::select_by_type(DictType::PermissionGroup as u16).await?;
        let permission_list = SysPermission::select_all().await?;
        let api_list = SysApi::select_all().await?;
        let role_list = SysRole::select_all().await?;
        let menu_list = SysMenu::select_all().await?;

        // 生成各种辅助数据结构用于加快排序速度
        let dict_map = Self::slice_to_map(&dict_list, |v| v.dict_code.unwrap());
        let permission_map = Self::slice_to_map(&permission_list,
                |v| v.permission_code.unwrap());
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
            let dict = *dict_map.get(&item.group_code).with_context(
                    || format!("权限组[code = {}]丢失", item.group_code))?;

            // 记录字典表权限组的权限组代码变化
            new_dict_list.push((dict.dict_id.unwrap(), new_group_code as u16));

            if item.permission_codes.is_empty() { continue }

            // 更新权限组所属的所有权限
            for pcode in item.permission_codes.iter() {
                let permission = *permission_map.get(pcode).with_context(
                        || format!("权限[code = {pcode}]丢失")
                )?;

                // 更新权限的权限组代码及权限代码
                new_permission_list.push((permission.permission_id.unwrap(),
                        new_group_code as u16, new_permission_code as u16));

                // 更新角色的权限(旧的权限位置移动到新的位置)
                for (role, role_p) in role_list.iter()
                        .zip(role_permissions_list.iter_mut()) {
                    let rp = role.permissions.as_ref().unwrap();
                    let bit = bits::get(rp, *pcode as usize);
                    bits::set(role_p, new_permission_code, bit);
                }

                // 更新权限关联的api
                if let Some(sub_api_list) = api_map.get(&(*pcode as i32)) {
                    for item in sub_api_list.iter() {
                        new_api_list.push((item.api_id.unwrap(), new_permission_code as i32));
                    }
                }

                // 更新权限关联的菜单
                if let Some(sub_menu_list) = menu_map.get(&(*pcode as i32)) {
                    for item in sub_menu_list.iter() {
                        new_menu_list.push((item.menu_id.unwrap(), new_permission_code as i32));
                    }
                }

                new_permission_code += 1;
            }

        }

        // 获取数据库连接
        let mut conn = get_conn().await?;
        let mut conn = conn.start_transaction().await?;
        let trans = &mut conn;

        Self::update_dicts(trans, &new_dict_list).await?;
        Self::update_permissions(trans, &new_permission_list).await?;
        Self::update_apis(trans, &new_api_list).await?;
        Self::update_menus(trans, &new_menu_list).await?;
        Self::update_roles(trans, &role_list, role_permissions_list).await?;

        // 提交事务
        conn.commit().await?;

        Ok(())
    }

    fn slice_to_map<K, V, F>(slice: &[V], f: F) -> HashMap<K, &V>
    where
        K: Eq + Hash,
        F: Fn(&V) -> K,
    {
        slice.iter().map(|v| (f(v), v)).collect()
    }

    fn slice_group< K, V, F>(slice: &[V], f: F) -> HashMap<K, Vec<&V>>
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

    async fn update_dicts(conn: &mut Transaction<'_>,
            list: &[(u32, u16)]) -> Result<()> {

        type D = SysDict;
        let sql = format!("update {} set {} = ?, {} = ? where {} = ?",
                D::TABLE, D::DICT_CODE, D::UPDATED_TIME, D::DICT_ID);
        let now = LocalTime::now();

        for item in list.iter() {
            let params = vec_value![item.1, now, item.0];
            gensql::log_sql_params(&sql, &params);
            conn.exec_sql(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_permissions(conn: &mut Transaction<'_>,
            list: &[(u32, u16, u16)]) -> Result<()> {

        type T = SysPermission;
        let sql = format!("update {} set {} = ?, {} = ?, {} = ? where {} = ?",
                T::TABLE, T::GROUP_CODE, T::PERMISSION_CODE, T::UPDATED_TIME,
                T::PERMISSION_ID);
        let now = LocalTime::now();

        for item in list.iter() {
            let params = vec_value![item.1, item.2, now, item.0];
            gensql::log_sql_params(&sql, &params);
            conn.exec_sql(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_apis(conn: &mut Transaction<'_>,
            list: &[(u32, i32)]) -> Result<()> {

        type A = SysApi;
        let sql = format!("update {} set {} = ?, {} = ? where {} = ?",
                A::TABLE, A::PERMISSION_CODE, A::UPDATED_TIME, A::API_ID);
        let now = LocalTime::now();

        for item in list.iter() {
            let params = vec_value![item.1, now, item.0];
            gensql::log_sql_params(&sql, &params);
            conn.exec_sql(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_menus(conn: &mut Transaction<'_>,
            list: &[(u32, i32)]) -> Result<()> {

        type T = SysMenu;
        let sql = format!("update {} set {} = ?, {} = ? where {} = ?",
                T::TABLE, T::PERMISSION_CODE, T::UPDATED_TIME, T::MENU_ID);
        let now = LocalTime::now();

        for item in list.iter() {
            let params = vec_value![item.1, now, item.0];
            gensql::log_sql_params(&sql, &params);
            conn.exec_sql(&sql, params).await?;
        }

        Ok(())
    }

    async fn update_roles(trans: &mut Transaction<'_>,
            roles: &[SysRole], ps: Vec<String>) -> Result<()> {

        type R = SysRole;
        let sql = format!("update {} set {} = ?, {} = ? where {} = ?",
                R::TABLE, R::PERMISSIONS, R::UPDATED_TIME, R::ROLE_ID);

        let now = LocalTime::now();

        for (role, p) in roles.iter().zip(ps.into_iter()) {
            let params = vec_value![p, now, role.role_id];
            trans.exec_sql(&sql, params).await?;
        }

        Ok(())
    }

}
