//! 接口定义表
use super::{sys_dict::DictType, sys_permission::SysPermission, PageData, PageInfo};
use crate::entities::sys_dict::SysDict;
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;
use std::collections::HashMap;

/// 系统接口表
#[table("t_sys_api")]
pub struct SysApi {
    /// 接口id
    #[table(id)]
    api_id:             u32,
    /// 权限代码
    permission_code:    i16,
    /// 接口地址
    api_path:           String,
    /// 接口描述
    api_remark:         String,
    /// 更新时间
    updated_time:       LocalTime,
}

#[table]
pub struct SysApiVo {
    #[serde(flatten)]
    inner:              SysApi,
    /// 权限组代码
    group_code:         i16,
    /// 权限组名称
    group_name:         String,
    /// 权限名称
    #[table(ignore)]
    permission_name:    String,
}

impl SysApi {
    /// 查询记录
    pub async fn select_page(value: SysApiVo, page: PageInfo) -> DbResult<PageData<SysApiVo>> {
        type T = SysApi;
        type P = SysPermission;
        type D = SysDict;

        const T: &str = "t";
        const P: &str = "p";
        const D: &str = "d";

        let (mut group_code, mut pcode) = (None, None);
        if value.inner.permission_code.is_none() {
            if let Some(v) = value.group_code {
                if v == -1 {
                    pcode = Some(0);
                } else {
                    group_code = Some(v);
                }
            }
        }

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_columns_with_table(T, &T::FIELDS)
            .select_with_table(P, P::GROUP_CODE)
            .select_as(D, D::DICT_NAME, SysApiVo::GROUP_NAME)
            .select_with_table(P, P::PERMISSION_NAME)
            .from_alias(T::TABLE_NAME, T)
            .left_join(P::TABLE_NAME, P, |j|
                j.on_eq(P::PERMISSION_CODE, T, T::PERMISSION_CODE)
            )
            .left_join(D::TABLE_NAME, D, |j|
                j.on_eq(D::DICT_CODE, P, P::GROUP_CODE)
                .on_eq_val(D::DICT_TYPE, DictType::PermissionGroup as u16)
            )
            .where_sql(|w|
                w.eq_opt(T, T::PERMISSION_CODE, value.inner.permission_code)
                .like_opt(T, T::API_PATH, value.inner.api_path)
                .like_opt(T, T::API_REMARK, value.inner.api_remark)
                .eq_opt(P, P::GROUP_CODE, group_code)
                .cmp(T, T::PERMISSION_CODE, "<", pcode)
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

    /// 加载所有记录
    pub async fn select_all() -> DbResult<Vec<SysApi>> {
        let fields = [Self::API_ID, Self::PERMISSION_CODE, Self::API_PATH, Self::API_REMARK];
        let (sql, params) = gensql::SelectSql::new()
            .select_columns(&fields)
            .from(Self::TABLE_NAME)
            .order_by(Self::API_ID)
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

        Ok(())
    }

}
