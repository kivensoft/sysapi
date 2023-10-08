use anyhow::Result;
use compact_str::format_compact;
use gensql::{table_define, Transaction, get_conn, query_one_sql, query_all_sql, Queryable, FastStr, table_flatten};
use localtime::LocalTime;

use super::{PageData, PageInfo};

pub enum DictType {
    DictClass,
    PermissionGroup,
    ClientType,
}

table_define!{"t_sys_dict", SysDict,
    dict_id:        u32,
    dict_type:      u16,
    dict_code:      i16,
    dict_name:      FastStr,
    updated_time:   LocalTime,
}

table_flatten!{SysDictVo, SysDict,
    dict_type_name: String,
}

impl SysDict {
    /// 查询记录
    pub async fn select_page(value: &SysDict, page: PageInfo) -> Result<PageData<SysDictVo>> {
        const T: &str = "t";
        const T1: &str = "t1";

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_slice(T, Self::FIELDS)
                .select_as(T1, Self::DICT_NAME, SysDictVo::DICT_TYPE_NAME)
            .from_alias(Self::TABLE, T)
            .left_join(Self::TABLE, T1)
                .on_eq(T1, Self::DICT_CODE, T, Self::DICT_TYPE)
                .on_eq_val(T1, Self::DICT_TYPE, &(DictType::DictClass as u16))
                .end_join()
            .where_sql()
                .eq_opt(T, Self::DICT_TYPE, &value.dict_type)
                .eq_opt(T, Self::DICT_CODE, &value.dict_code)
                .like_opt(T, Self::DICT_NAME, &value.dict_name)
                .end_where()
            .build_with_page(page.index, page.size, page.total)?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            page.total.unwrap_or(0)
        } else {
            conn.query_one_sql(&tsql, &params).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, |(
            dict_id,
            dict_type,
            dict_code,
            dict_name,
            updated_time,
            dict_type_name,
        )| SysDictVo {
            inner: SysDict {
                dict_id,
                dict_type,
                dict_code,
                dict_name,
                updated_time,
            },
            dict_type_name,
        }).await?;

        Ok(PageData { total, list, })
    }

    /// 返回指定类型的所有字典项
    pub async fn select_by_type(dict_type: u16) -> Result<Vec<SysDict>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
            .eq("", Self::DICT_TYPE, &dict_type)
            .end_where()
            .order_by("", Self::DICT_CODE)
            .build();
        query_all_sql(&sql, &params, Self::row_map).await
    }

    /// 查询指定类型的dict_code最大值
    pub async fn select_max_code(dict_type: u16) -> Result<Option<i16>> {
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::DICT_CODE))
            .from(Self::TABLE)
            .where_sql()
            .eq("", Self::DICT_TYPE, &dict_type)
            .end_where()
            .build();

        query_one_sql(&sql, &params).await
    }

    // 批量更新指定的类别(使用事务进行更新)
    pub async fn batch_update_by_type(dict_type: u16, dict_names: &[String]) -> Result<()> {
        let recs = Self::select_by_type(dict_type).await?;

        let mut new_code = 0;
        let update_count = std::cmp::min(recs.len(), dict_names.len());
        let mut new_dict = &mut SysDict {
            updated_time: Some(LocalTime::now()),
            ..Default::default()
        };

        let mut conn = get_conn().await?;
        let mut conn = conn.start_transaction().await?;
        let trans = &mut conn;

        // 更新已存在的记录
        for item in recs.iter().take(update_count) {
            let (id, name) = (item.dict_id, item.dict_name.clone());
            Self::my_update_dict(trans, id, new_code, name, new_dict).await?;
            new_code += 1;
        }

        // 增加新记录
        if dict_names.len() > update_count {
            new_dict.dict_id = None;
            new_dict.dict_type = Some(dict_type);

            for item in dict_names.iter().skip(update_count) {
                Self::my_insert_dict(trans, new_code, item, new_dict).await?;
                new_code += 1;
            }
        }

        // 删除多余的记录
        if recs.len() > update_count {
            for item in recs.iter().skip(update_count) {
                let id = item.dict_id.unwrap();
                Self::my_delete_dict(trans, id).await?;
            }
        }

        conn.commit().await?;

        Ok(())
    }

    async fn my_update_dict(conn: &mut Transaction<'_>, id: Option<u32>,
            code: i16, name: Option<FastStr>, dict: &mut SysDict) -> Result<()> {

        dict.dict_id = id;
        dict.dict_code = Some(code);
        dict.dict_name = name;

        let (sql, params) = Self::stmt_update_dynamic(dict);
        conn.exec_sql(sql, params).await?;
        Ok(())
    }

    async fn my_insert_dict(conn: &mut Transaction<'_>,
            code: i16, name: &str, dict: &mut SysDict) -> Result<()> {

        dict.dict_code = Some(code);
        dict.dict_name = Some(FastStr::new(name));

        let (sql, params) = Self::stmt_insert(dict);
        conn.exec_sql(sql, params).await?;
        Ok(())
    }

    async fn my_delete_dict(conn: &mut Transaction<'_>, id: u32) -> Result<()> {
        let (sql, params) = Self::stmt_delete(&id);
        conn.exec_sql(sql, params).await?;
        Ok(())
    }

}
