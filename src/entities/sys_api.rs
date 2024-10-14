//! 接口定义表
use super::{sys_dict::DictType, sys_permission::SysPermission, PageData, PageInfo};
use crate::{
    entities::{
        sys_dict::{SysDict, BUILTIN_GROUP_CODE, BUILTIN_GROUP_NAME},
        sys_permission::{
            BUILTIN_ANONYMOUS_CODE, BUILTIN_ANONYMOUS_NAME, BUILTIN_PUBLIC_CODE, BUILTIN_PUBLIC_NAME,
        },
    }, services::gmc, utils::consts
};
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;
use std::collections::HashMap;

const CATEGORY: &str = consts::gmc::SYS_API;

/// 系统接口表
#[table("t_sys_api")]
pub struct SysApi {
    /// 接口id
    #[table(id)]
    api_id: u32,
    /// 权限代码
    permission_code: i16,
    /// 接口地址
    api_path: String,
    /// 接口描述
    api_remark: String,
    /// 更新时间
    updated_time: LocalTime,
}

#[table]
pub struct SysApiVo {
    #[serde(flatten)]
    inner: SysApi,
    /// 权限组代码
    group_code: i8,
    /// 权限组名称
    group_name: String,
    /// 权限名称
    permission_name: String,
}

impl SysApi {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let api_id = self.api_id;
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(api_id).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let api_id = self.api_id;
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(api_id).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(record) => {
                let api_id = record.api_id;
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(api_id).await;
                }
                ret
            }
            None => Ok(false),
        }
    }

    /// 数据变更通知
    pub async fn notify_changed(id: Option<u32>) {
        let id = match id {
            Some(n) => format!("{n}"),
            None => String::new(),
        };
        gmc::get_cache().notify(CATEGORY, &id).await
    }

    /// 查询记录
    pub async fn select_page(value: SysApiVo, page: PageInfo) -> DbResult<PageData<SysApiVo>> {
        type T = SysApi;
        type P = SysPermission;
        type D = SysDict;

        let (t, p, d) = ("t", "p", "d");

        let (group_code, pcode) = match value.group_code {
            Some(code) if code != BUILTIN_GROUP_CODE => (Some(code), None),
            Some(_) => (None, Some(0)),
            None => (None, None),
        };

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_all_with_table(t)
            .select_as(d, D::DICT_NAME, SysApiVo::GROUP_NAME)
            .select(p, P::GROUP_CODE)
            .select(p, P::PERMISSION_NAME)
            .from_as(T::TABLE_NAME, t)
            .left_join(P::TABLE_NAME, p, |j| {
                j.on_eq(P::PERMISSION_CODE, t, T::PERMISSION_CODE)
            })
            .left_join(D::TABLE_NAME, d, |j| {
                j.on_eq(D::DICT_CODE, p, P::GROUP_CODE)
                    .on_eq_val(D::DICT_TYPE, DictType::PermissionGroup as u16)
            })
            .where_sql(|w| {
                w.eq_opt(t, T::PERMISSION_CODE, value.inner.permission_code)
                    .like_opt(t, T::API_PATH, value.inner.api_path)
                    .like_opt(t, T::API_REMARK, value.inner.api_remark)
                    .eq_opt(p, P::GROUP_CODE, group_code)
                    .expr_opt(t, T::PERMISSION_CODE, "<", pcode)
            })
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let mut list: Vec<SysApiVo> = conn.query_fast(psql, params).await?;

        // 设置内置权限组/权限的编码和名称
        list.iter_mut().for_each(|v| {
            let mut finded = false;
            let pc = v.inner.permission_code.unwrap_or(0);
            if pc == BUILTIN_ANONYMOUS_CODE {
                v.permission_name = Some(BUILTIN_ANONYMOUS_NAME.to_string());
                finded = true;
            } else if pc == BUILTIN_PUBLIC_CODE {
                v.permission_name = Some(BUILTIN_PUBLIC_NAME.to_string());
                finded = true;
            }
            if finded {
                v.group_code = Some(BUILTIN_GROUP_CODE);
                v.group_name = Some(BUILTIN_GROUP_NAME.to_string());
            }
        });

        Ok(PageData { total, list })
    }

    /// 加载所有记录
    pub async fn select_all() -> DbResult<Vec<SysApi>> {
        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .order_by("", Self::API_ID)
            .build();
        gensql::sql_query_fast(&sql, params).await
    }

    /// 批量更新列表中的权限索引值, 更新失败则回滚
    pub async fn batch_update_permission_code(value: &[SysApi]) -> DbResult<()> {
        let mut trans = gensql::start_transaction().await?;

        let now = LocalTime::now();
        let sql = format!(
            "update {} set {} = ?, {} = ? where {} = ?",
            SysApi::TABLE_NAME,
            SysApi::PERMISSION_CODE,
            SysApi::UPDATED_TIME,
            SysApi::API_ID
        );
        gensql::db_log_sql(&sql);

        for val in value.iter() {
            let params = gensql::to_values![val.permission_code, now, val.api_id,];
            gensql::db_log_params(&params);
            trans.exec(&sql, params).await?;
        }

        trans.commit().await?;
        Self::notify_changed(None).await;

        Ok(())
    }

    /// 按id列表中的顺序重新排序
    pub async fn batch_update_id(ids: &[u32], all_records: Vec<SysApi>) -> DbResult<()> {
        type T = SysApi;
        let mut trans = gensql::start_transaction().await?;

        let ids_map: HashMap<u32, usize> =
            ids.iter().enumerate().map(|(i, v)| (*v, i + 1)).collect();
        let now = Some(LocalTime::now());

        let records: Vec<_> = all_records
            .into_iter()
            .map(|mut rec| {
                let api_id = rec.api_id.as_ref().unwrap();
                let idx = ids_map.get(api_id).unwrap();
                rec.api_id.replace(*idx as u32);
                rec.updated_time = now.clone();
                rec
            })
            .collect();

        // 清空原有的记录
        let (sql, params) = gensql::DeleteSql::new(T::TABLE_NAME, |w| w).build();
        trans.exec(sql, params).await?;

        let sql = format!(
            "insert into {} ({}, {}, {}, {}, {}) values(?, ?, ?, ?, ?)",
            T::TABLE_NAME,
            T::API_ID,
            T::PERMISSION_CODE,
            T::API_PATH,
            T::API_REMARK,
            T::UPDATED_TIME
        );
        // 重新添加变动后的记录
        for rec in records.into_iter() {
            let params = gensql::to_values![
                rec.api_id,
                rec.permission_code,
                rec.api_path,
                rec.api_remark,
                rec.updated_time,
            ];
            trans.exec(&sql, params).await?;
        }

        trans.commit().await?;
        Self::notify_changed(None).await;

        Ok(())
    }

}
