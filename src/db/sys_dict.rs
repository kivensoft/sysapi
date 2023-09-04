use anyhow::Result;
use compact_str::format_compact;
use gensql::{table_define, Transaction, get_conn, query_one_sql, query_all_sql, Queryable};
use localtime::LocalTime;

use super::{PageData, PageInfo};

pub enum DictType {
    _DictClass,
    PermissionGroup,
    _ClientType,
}

table_define!("t_sys_dict", SysDict,
    dict_id:        u32       => DICT_ID,
    dict_type:      u16       => DICT_TYPE,
    dict_code:      u16       => DICT_CODE,
    dict_name:      String    => DICT_NAME,
    updated_time:   LocalTime => UPDATED_TIME,
);

impl SysDict {
    /// 查询记录
    pub async fn select_page(value: &SysDict, page: PageInfo) -> Result<PageData<SysDict>> {
        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq_opt("", Self::DICT_TYPE, &value.dict_type)
            .and_eq_opt("", Self::DICT_CODE, &value.dict_code)
            .and_like_opt("", Self::DICT_NAME, &value.dict_name)
            .end_where()
            .build_with_page()?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            conn.query_one_sql(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

    /// 返回指定类型的所有字典项
    pub async fn select_by_type(dict_type: u16) -> Result<Vec<SysDict>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq("", Self::DICT_TYPE, &dict_type)
            .end_where()
            .build();
        query_all_sql(&sql, &params, Self::row_map).await
    }

    /// 查询指定类型的dict_code最大值
    pub async fn select_max_code(dict_type: u16) -> Result<Option<u16>> {
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::DICT_CODE))
            .from(Self::TABLE)
            .where_sql()
            .and_eq("", Self::DICT_TYPE, &dict_type)
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
            code: u16, name: Option<String>, dict: &mut SysDict) -> Result<()> {

        dict.dict_id = id;
        dict.dict_code = Some(code);
        dict.dict_name = name;

        let (sql, params) = Self::stmt_update_dynamic(dict);
        conn.exec_sql(sql, params).await?;
        Ok(())
    }

    async fn my_insert_dict(conn: &mut Transaction<'_>,
            code: u16, name: &str, dict: &mut SysDict) -> Result<()> {

        dict.dict_code = Some(code);
        dict.dict_name = Some(name.to_owned());

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
