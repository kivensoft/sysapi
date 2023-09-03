use anyhow::Result;
use gensql::{table_define, vec_value};
use localtime::LocalTime;
use mysql_async::{prelude::Queryable, TxOpts, Params};
use serde::{Serialize, Deserialize};

use crate::{db::sys_dict::SysDict, services::rmq};

use super::{PageData, PageInfo, sys_dict::DictType, sys_permission::SysPermission};

table_define!("t_sys_api", SysApi,
    api_id:             u32       => API_ID,
    permission_code:    i32       => PERMISSION_CODE,
    api_path:           String    => API_PATH,
    api_remark:         String    => API_REMARK,
    updated_time:       LocalTime => UPDATED_TIME,
);

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SysApiExt {
    #[serde(flatten)]
    pub inner: SysApi,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_code: Option<i32>,
}

impl SysApi {
    /// 删除记录
    pub async fn delete_by_id(id: u32) -> Result<u32> {
        super::exec_sql(&Self::stmt_delete(&id)).await
    }

    /// 插入记录，返回(插入记录数量, 自增ID的值)
    pub async fn insert(value: &SysApi) -> Result<(u32, u32)> {
        super::insert_sql(&Self::stmt_insert(value)).await
    }

    /// 更新记录
    pub async fn update_by_id(value: &SysApi) -> Result<u32> {
        super::exec_sql(&Self::stmt_update(value)).await
    }

    /// 查询记录
    pub async fn select_by_id(id: u32) -> Result<Option<SysApi>> {
        Ok(super::query_one_sql(&Self::stmt_select(&id)).await?.map(Self::row_map))
    }

    /// 查询记录
    pub async fn select_page(value: &SysApiExt, page: PageInfo) -> Result<PageData<SysApiExt>> {
        use compact_str::format_compact as fmt;
        type P = SysPermission;
        type D = SysDict;

        let dict_type = DictType::PermissionGroup as u16;

        let (mut group_code, mut pcode) = (None, None);
        if value.inner.permission_code.is_none() {
            if let Some(v) = value.group_code {
                if v == -1 { pcode = Some(0); }
                else { group_code = Some(v); }
            }
        }

        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("t", &Self::fields())
            .select_ext("p", P::GROUP_CODE)
            .from_alias(Self::TABLE, "t")
            .left_join(P::TABLE, "p")
            .on_eq("p", P::PERMISSION_CODE, "t", Self::PERMISSION_CODE)
            .left_join(D::TABLE, "d")
            .on_eq("d", D::DICT_CODE, "p", P::GROUP_CODE)
            .on_eq_val("d", D::DICT_TYPE, &dict_type)
            .where_sql()
            .and_eq_opt("t", Self::PERMISSION_CODE, &value.inner.permission_code)
            .and_like_opt("t", Self::API_PATH, &value.inner.api_path)
            .and_like_opt("t", Self::API_REMARK, &value.inner.api_remark)
            .and_eq_opt("p", P::GROUP_CODE, &group_code)
            .if_opt(&fmt!("t.{} < ?", P::PERMISSION_CODE), &pcode)
            .end_where()
            .build_with_page()?;

        let mut conn = super::get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            conn.exec_first(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.exec_map(psql, params, |(
                api_id,
                permission_code,
                api_path,
                api_remark,
                updated_time,
                group_code,
            )| SysApiExt {
                inner: SysApi {
                    api_id,
                    permission_code,
                    api_path,
                    api_remark,
                    updated_time,
                },
                group_code,
            }).await?;

        Ok(PageData { total, list, })
    }

    /// 加载所有记录
    pub async fn select_all() -> Result<Vec<SysApi>> {
        let sql_params = gensql::SelectSql::new()
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .build();
        super::query_all_sql(&sql_params, Self::row_map).await
    }

    /// 批量更新列表中的权限索引值, 更新失败则回滚
    pub async fn batch_update_permission_id(value: &[SysApi]) -> Result<()> {
        let mut conn = super::get_conn().await?;
        let mut conn = conn.start_transaction(TxOpts::new()).await?;

        type T = SysApi;
        let sql = format!("update {} set {} = ?, {} = ? where {} = ?",
                T::TABLE, T::PERMISSION_CODE, T::UPDATED_TIME, T::API_ID);

        for item in value.iter() {
            let params = vec_value![
                item.permission_code,
                item.updated_time,
                item.api_id,
            ];

            gensql::log_sql_params(&sql, &params);
            conn.exec_drop(&sql, &Params::Positional(params)).await?;
        }

        conn.commit().await?;

        tokio::spawn(async move {
            let chan = rmq::make_channel(rmq::ChannelName::ModApi);
            let op = rmq::RecChanged::<SysApi>::publish_all(&chan).await;
            if let Err(e) = op {
                log::error!("redis发布消息失败: {e:?}");
            }
        });

        Ok(())
    }

}
