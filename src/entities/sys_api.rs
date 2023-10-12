use anyhow::Result;
use gensql::{table_define, get_conn, query_all_sql, vec_value, Queryable, FastStr, table_flatten};
use localtime::LocalTime;

use crate::{entities::sys_dict::SysDict, services::rmq, utils};

use super::{PageData, PageInfo, sys_dict::DictType, sys_permission::SysPermission};

table_define!{"t_sys_api", SysApi,
    api_id:             u32,
    permission_code:    i16,
    api_path:           FastStr,
    api_remark:         FastStr,
    updated_time:       LocalTime,
}

table_flatten!{SysApiVo, SysApi,
    group_code:      i16,
    group_name:      FastStr,
    permission_name: FastStr,
}

impl SysApi {
    /// 查询记录
    pub async fn select_page(value: &SysApiVo, page: PageInfo) -> Result<PageData<SysApiVo>> {
        use compact_str::format_compact as fmt;
        type T = SysApi;
        type P = SysPermission;
        type D = SysDict;

        const T: &str = "t";
        const P: &str = "p";
        const D: &str = "d";

        let (mut group_code, mut pcode) = (None, None);
        if value.inner.permission_code.is_none() {
            if let Some(v) = value.group_code {
                if v == -1 { pcode = Some(0); }
                else { group_code = Some(v); }
            }
        }

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_slice(T, T::FIELDS)
                .select_ext(P, P::GROUP_CODE)
                .select_as(D, D::DICT_NAME, SysApiVo::GROUP_NAME)
                .select_ext(P, P::PERMISSION_NAME)
            .from_alias(T::TABLE, T)
            .left_join(P::TABLE, P)
                .on_eq(P, P::PERMISSION_CODE, T, T::PERMISSION_CODE)
                .end_join()
            .left_join(D::TABLE, D)
                .on_eq(D, D::DICT_CODE, P, P::GROUP_CODE)
                .on_eq_val(D, D::DICT_TYPE, &(DictType::PermissionGroup as u16))
                .end_join()
            .where_sql()
                .eq_opt(T, T::PERMISSION_CODE, &value.inner.permission_code)
                .like_opt(T, T::API_PATH, &value.inner.api_path)
                .like_opt(T, T::API_REMARK, &value.inner.api_remark)
                .eq_opt(P, P::GROUP_CODE, &group_code)
                .if_opt(&fmt!("{}.{} < ?", T, T::PERMISSION_CODE), &pcode)
                .end_where()
            .build_with_page(page.index, page.size, page.total)?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            page.total.unwrap_or(0)
        } else {
            conn.query_one_sql(&tsql, &params).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, |(
                api_id,
                permission_code,
                api_path,
                api_remark,
                updated_time,
                group_code,
                group_name,
                permission_name,
            )| {
                let mut res = SysApiVo {
                    inner: SysApi {
                        api_id,
                        permission_code,
                        api_path,
                        api_remark,
                        updated_time,
                    },
                    group_code,
                    group_name,
                    permission_name,
                };
                if let Some(c) = res.inner.permission_code {
                    let p = match c {
                        utils::ANONYMOUS_PERMIT_CODE => utils::ANONYMOUS_PERMIT_NAME,
                        utils::PUBLIC_PERMIT_CODE => utils::PUBLIC_PERMIT_NAME,
                        _ => "",
                    };
                    if !p.is_empty() {
                        res.permission_name = Some(FastStr::new(p));
                        res.group_name = Some(FastStr::new(utils::INNER_GROUP_NAME));
                    }
                }

                res
            }).await?;

        Ok(PageData { total, list, })
    }

    /// 加载所有记录
    pub async fn select_all() -> Result<Vec<SysApi>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
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
