//! 权限定义表
use std::{collections::HashMap, hash::Hash};

use super::{PageData, PageInfo};
use crate::{
    entities::{
        sys_api::SysApi,
        sys_dict::{DictType, SysDict},
        sys_menu::SysMenu,
        sys_role::SysRole,
    },
    utils::{bits, consts},
};
use anyhow_ext::{bail, Result};
use gensql::{db_log_params, db_log_sql, table, to_values, DbResult, Queryable, Transaction};
use localtime::LocalTime;

pub const BUILTIN_ANONYMOUS_CODE: i16 = crate::auth::ANONYMOUS_CODE;
pub const BUILTIN_ANONYMOUS_NAME: &str = "匿名许可";
pub const BUILTIN_PUBLIC_CODE: i16 = crate::auth::PUBLIC_CODE;
pub const BUILTIN_PUBLIC_NAME: &str = "公共许可";

#[table("t_sys_permission")]
pub struct SysPermission {
    /// 权限id
    #[table(id)]
    permission_id: u32,
    /// 权限组代码
    group_code: i8,
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SysPermissionRearrange {
    pub group_code: i8,
    pub permission_codes: Vec<i16>,
}

impl SysPermission {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let pid = self.permission_id;
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(pid).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let pid = self.permission_id;
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(pid).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(record) => {
                let pid = record.permission_id;
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(pid).await;
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
        crate::services::gmc::get_cache().notify(consts::gmc::SYS_PERMISSION, &id).await
    }

    /// 查询记录
    pub async fn select_page(
        value: SysPermission,
        page: PageInfo,
    ) -> DbResult<PageData<SysPermissionVo>> {
        type T = SysPermission;
        type D = SysDict;

        let (t, d) = ("t", "d");

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_all_with_table(t)
            .select_as(d, D::DICT_NAME, SysPermissionVo::GROUP_NAME)
            .from_as(T::TABLE_NAME, t)
            .left_join(D::TABLE_NAME, d, |j| {
                j.on_eq(D::DICT_CODE, t, T::GROUP_CODE)
                    .on_eq_val(D::DICT_TYPE, DictType::PermissionGroup as u16)
            })
            .where_sql(|w| {
                w.eq_opt(t, T::GROUP_CODE, value.group_code).like_opt(
                    t,
                    T::PERMISSION_NAME,
                    value.permission_name,
                )
            })
            .order_by(t, T::PERMISSION_CODE)
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let list = conn.query_fast(psql, params).await?;
        Ok(PageData { total, list })
    }

    /// 查询permission_code最大值
    pub async fn select_max_code() -> DbResult<Option<i16>> {
        let (sql, params) = gensql::SelectSql::new()
            .select("", &format!("max({})", Self::PERMISSION_CODE))
            .from(Self::TABLE_NAME)
            .build();

        gensql::sql_query_one(sql, params).await
    }

    /// 加载所有记录
    pub async fn select_all() -> DbResult<Vec<SysPermission>> {
        let (psql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .order_by("", Self::PERMISSION_CODE)
            .build();
        gensql::sql_query_fast(psql, params).await
    }

    /// 对权限进行重新排序
    pub async fn rearrange(value: &[SysPermissionRearrange]) -> Result<()> {
        // 从数据库中加载相关的权限组, 权限, 接口, 角色的记录集
        let dict_list = SysDict::select_by_type(DictType::PermissionGroup as u8).await?;
        let permission_list = SysPermission::select_all().await?;
        let api_list = SysApi::select_all().await?;
        let menu_list = SysMenu::select_all().await?;
        let role_list = SysRole::select_all().await?;

        // 生成hashmap用于加快查找速度
        let dict_map = Self::slice_to_map(&dict_list, |v| {
            v.dict_code.as_ref().map(|v| v.parse::<i8>().unwrap_or(-9))
        })?;
        let permission_map = Self::slice_to_map(&permission_list, |v| v.permission_code)?;
        let api_map = Self::slice_group(&api_list, |v| v.permission_code)?;
        let menu_map = Self::slice_group(&menu_list, |v| v.permission_code)?;

        // 保存变动列表
        let mut upd_dicts = Vec::with_capacity(dict_list.len());
        let mut upd_permissions = Vec::with_capacity(permission_list.len());
        let mut upd_apis = Vec::with_capacity(api_list.len());
        let mut upd_menus = Vec::with_capacity(menu_list.len());
        // 保存权限变化后的角色所对应的权限字符串
        let mut upd_roles: Vec<_> = role_list
            .iter()
            .map(|v| (v.role_id.unwrap_or(0), String::with_capacity(64)))
            .collect();
        // 新的权限组、权限编码，在循环体中递增
        let (mut new_group_code, mut new_permission_code): (i8, i16) = (0, 0);

        // 生成待更新的权限组, 权限, api数据结构
        for item in value.iter() {
            // 找到权限组，并记录t_sys_dict待更新记录的内容
            let dict = match dict_map.get(&item.group_code) {
                Some(v) => *v,
                None => bail!("权限组[code = {}]丢失", item.group_code),
            };
            if new_group_code != dict.dict_code.as_ref().unwrap().parse().unwrap_or(-9) {
                upd_dicts.push((dict.dict_id.unwrap_or(0), new_group_code));
            }

            // 分组下没有子权限，退出本次循环
            if item.permission_codes.is_empty() {
                new_group_code += 1;
                continue;
            }

            // 更新权限组所属的所有权限
            for permission_code in item.permission_codes.iter() {
                let permission = match permission_map.get(permission_code) {
                    Some(v) => v,
                    None => bail!("权限[code = {}]丢失", permission_code),
                };

                // 更新权限的权限组代码及权限代码
                if new_permission_code != permission.permission_code.unwrap_or(-9)
                    || new_group_code != permission.group_code.unwrap_or(-9)
                {
                    upd_permissions.push((
                        permission.permission_id.unwrap_or(0),
                        new_group_code,
                        new_permission_code,
                    ));
                }

                // 更新角色的权限(旧的权限位置移动到新的位置)
                for (role, new_role) in role_list.iter().zip(upd_roles.iter_mut()) {
                    if let Some(rp) = &role.permissions {
                        if bits::get(rp, *permission_code as usize) {
                            bits::set(&mut new_role.1, new_permission_code as usize, true);
                        }
                    }
                }

                // 更新权限关联的api
                if let Some(apis) = api_map.get(permission_code) {
                    for item in apis.iter() {
                        if new_permission_code != item.permission_code.unwrap_or(-9) {
                            upd_apis.push((item.api_id.unwrap_or(0), new_permission_code));
                        }
                    }
                }

                // 更新权限关联的菜单
                if let Some(sub_menu_list) = menu_map.get(permission_code) {
                    for item in sub_menu_list.iter() {
                        if new_permission_code != item.permission_code.unwrap_or(-9) {
                            upd_menus.push((item.menu_id.unwrap_or(0), new_permission_code));
                        }
                    }
                }

                new_permission_code += 1;
            }
            new_group_code += 1;
        }

        // 判断角色权限，去除无需更新的角色记录
        Self::retain_upd_roles(&role_list, &mut upd_roles);

        // 使用事务模式进行多表更新
        let mut trans = gensql::start_transaction().await?;

        Self::update_dicts(&mut trans, upd_dicts).await?;
        Self::update_permissions(&mut trans, upd_permissions).await?;
        Self::update_apis(&mut trans, upd_apis).await?;
        Self::update_menus(&mut trans, upd_menus).await?;
        Self::update_roles(&mut trans, upd_roles).await?;
        // 提交事务
        trans.commit().await?;
        // 发送数据变更通知
        SysDict::notify_changed(None).await;
        Self::notify_changed(None).await;
        SysApi::notify_changed(None).await;
        SysMenu::notify_changed(None).await;
        SysRole::notify_changed(None).await;

        Ok(())
    }
}

impl SysPermission {
    /// 权限排序结果对字典项进行更新
    async fn update_dicts(trans: &mut Transaction<'_>, list: Vec<(u32, i8)>) -> DbResult<()> {
        if list.is_empty() {
            return Ok(());
        }

        type T = SysDict;
        let sql = format!(
            "update {} set {} = ?, {} = now() where {} = ?",
            T::TABLE_NAME,
            T::DICT_CODE,
            T::UPDATED_TIME,
            T::DICT_ID
        );
        db_log_sql(&sql);

        let iter = list
            .into_iter()
            .map(|v| to_values!(v.1, v.0))
            .inspect(|v| db_log_params(v));

        trans.exec_batch(sql, iter).await
    }

    /// 权限排序结果对权限进行更新
    async fn update_permissions(
        trans: &mut Transaction<'_>,
        list: Vec<(u32, i8, i16)>,
    ) -> DbResult<()> {
        if list.is_empty() {
            return Ok(());
        }

        let sql = format!(
            "update {} set {} = ?, {} = ?, {} = now() where {} = ?",
            SysPermission::TABLE_NAME,
            SysPermission::GROUP_CODE,
            SysPermission::PERMISSION_CODE,
            SysPermission::UPDATED_TIME,
            SysPermission::PERMISSION_ID
        );
        db_log_sql(&sql);

        let iter = list
            .into_iter()
            .map(|v| to_values!(v.1, v.2, v.0))
            .inspect(|v| db_log_params(v));

        trans.exec_batch(sql, iter).await
    }

    /// 权限排序结果对接口信息进行更新
    async fn update_apis(trans: &mut Transaction<'_>, list: Vec<(u32, i16)>) -> DbResult<()> {
        if list.is_empty() {
            return Ok(());
        }

        let sql = format!(
            "update {} set {} = ?, {} = now() where {} = ?",
            SysApi::TABLE_NAME,
            SysApi::PERMISSION_CODE,
            SysApi::UPDATED_TIME,
            SysApi::API_ID,
        );
        db_log_sql(&sql);

        let iter = list
            .into_iter()
            .map(|v| to_values!(v.1, v.0))
            .inspect(|v| db_log_params(v));

        trans.exec_batch(sql, iter).await
    }

    /// 权限排序结果对菜单进行更新
    async fn update_menus(trans: &mut Transaction<'_>, list: Vec<(u32, i16)>) -> DbResult<()> {
        if list.is_empty() {
            return Ok(());
        }

        let sql = format!(
            "update {} set {} = ?, {} = now() where {} = ?",
            SysMenu::TABLE_NAME,
            SysMenu::PERMISSION_CODE,
            SysMenu::UPDATED_TIME,
            SysMenu::MENU_ID,
        );
        db_log_sql(&sql);

        let iter = list
            .into_iter()
            .map(|v| to_values!(v.1, v.0))
            .inspect(|v| db_log_params(v));

        trans.exec_batch(sql, iter).await
    }

    /// 权限排序结果对角色进行更新
    async fn update_roles(trans: &mut Transaction<'_>, list: Vec<(u32, String)>) -> DbResult<()> {
        if list.is_empty() {
            return Ok(());
        }

        let sql = format!(
            "update {} set {} = ?, {} = now() where {} = ?",
            SysRole::TABLE_NAME,
            SysRole::PERMISSIONS,
            SysRole::UPDATED_TIME,
            SysRole::ROLE_ID,
        );
        db_log_sql(&sql);

        let iter = list
            .into_iter()
            .map(|v| to_values!(v.1, v.0))
            .inspect(|v| db_log_params(v));

        trans.exec_batch(sql, iter).await
    }

    // 判断角色权限，去除无需更新的角色记录
    fn retain_upd_roles(old_roles: &[SysRole], upd_roles: &mut Vec<(u32, String)>) {
        for (role, new_role) in old_roles.iter().zip(upd_roles.iter_mut()) {
            // 忽略更新前后权限相同的记录
            if let Some(permissions) = &role.permissions {
                if permissions == &new_role.1 {
                    new_role.1.clear()
                }
            } else {
                // 删除尾部多余的0，长度为偶数
                if new_role.1.len() % 2 != 0 {
                    new_role.1.push('0');
                }
                while new_role.1.ends_with("00") {
                    new_role.1.truncate(new_role.1.len() - 2);
                }
            }
        }
        upd_roles.retain(|s| !s.1.is_empty());
    }

    /// 数组转成hashmap，用于快速查找
    fn slice_to_map<K, V, F>(slice: &[V], f: F) -> Result<HashMap<K, &V>>
    where
        K: Eq + Hash,
        V: serde::Serialize,
        F: Fn(&V) -> Option<K>,
    {
        let mut map = HashMap::with_capacity(slice.len());
        for item in slice {
            match f(item) {
                Some(k) => {
                    map.insert(k, item);
                }
                None => return Self::none_error(item),
            }
        }
        Ok(map)
    }

    /// 数组转成hashmap，用于快速查找
    fn slice_group<K, V, F>(slice: &[V], f: F) -> Result<HashMap<K, Vec<&V>>>
    where
        K: Eq + Hash,
        V: serde::Serialize,
        F: Fn(&V) -> Option<K>,
    {
        let mut map: HashMap<_, Vec<_>> = HashMap::new();
        for item in slice.iter() {
            match f(item) {
                Some(k) => {
                    map.entry(k)
                        .and_modify(|v| v.push(item))
                        .or_insert_with(|| vec![item]);
                }
                None => return Self::none_error(item),
            }
        }

        Ok(map)
    }

    /// 空值错误
    fn none_error<T>(value: &impl serde::Serialize) -> Result<T> {
        let msg = serde_json::to_string(value).unwrap();
        bail!("错误，数据有空值: {}", msg)
    }
}
