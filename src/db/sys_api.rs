use anyhow::Result;
use gensql::{table_define, get_conn, query_all_sql, vec_value, Queryable};
use localtime::LocalTime;
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

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            match page.total {
                Some(total) => total,
                None => conn.query_one_sql(&tsql, &params)
                        .await?
                        .map(|(total,)| total)
                        .unwrap_or(0),
            }
        };

        let list = conn.query_all_sql(psql, params, |(
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
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .build();
        query_all_sql(&sql, &params, Self::row_map).await
    }

    /// 批量更新列表中的权限索引值, 更新失败则回滚
    pub async fn batch_update_permission_id(value: &[SysApi]) -> Result<()> {
        let mut conn = get_conn().await?;
        let mut conn = conn.start_transaction().await?;

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
            conn.exec_sql(&sql, &params).await?;
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
