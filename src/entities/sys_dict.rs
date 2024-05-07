//! 字典表
use super::{PageData, PageInfo};
use compact_str::format_compact;
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;

pub enum DictType {
    DictClass,
    PermissionGroup,
    ClientType,
}

/// 系统接口表
#[table("t_sys_dict")]
pub struct SysDict {
    /// 字典项id
    #[table(id)]
    dict_id:      u32,
    /// 字典项类型
    dict_type:    u16,
    /// 字典项代码
    dict_code:    i16,
    /// 字典项名称
    dict_name:    String,
    /// 更新时间
    updated_time: LocalTime,

    /// 字典项类型名称
    #[table(not_field)]
    dict_type_name: String,
}


impl SysDict {
    /// 查询记录
    pub async fn select_page(value: SysDict, page: PageInfo) -> DbResult<PageData<SysDict>> {
        const T: &str = "t";
        const T1: &str = "t1";

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_columns_with_table(T, &Self::FIELDS)
            .select_as(T1, Self::DICT_NAME, SysDict::DICT_TYPE_NAME)
            .from_alias(Self::TABLE_NAME, T)
            .left_join(Self::TABLE_NAME, T1, |j|
                j.on_eq(Self::DICT_CODE, T, Self::DICT_TYPE)
                    .on_eq_val(Self::DICT_TYPE, DictType::DictClass as u16)
            )
            .where_sql(|w|
                w.eq_opt(T, Self::DICT_TYPE, value.dict_type)
                    .eq_opt(T, Self::DICT_CODE, value.dict_code)
                    .like_opt(T, Self::DICT_NAME, value.dict_name)
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

    /// 返回指定类型的所有字典项
    pub async fn select_by_type(dict_type: u16) -> DbResult<Vec<SysDict>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_columns(&Self::FIELDS)
            .from(Self::TABLE_NAME)
            .where_sql(|w|
                w.eq("", Self::DICT_TYPE, dict_type)
            )
            .order_by(Self::DICT_CODE)
            .build();
        gensql::sql_query_fast(&sql, params).await
    }

    /// 查询指定类型的dict_code最大值
    pub async fn select_max_code(dict_type: u16) -> DbResult<Option<i16>> {
        let (sql, params) = gensql::SelectSql::new()
            .select(&format_compact!("max({})", Self::DICT_CODE))
            .from(Self::TABLE_NAME)
            .where_sql(|w|
                w.eq("", Self::DICT_TYPE, dict_type)
            )
            .build();

        gensql::sql_query_one(&sql, params).await
    }

    // 批量更新指定的类别(使用事务进行更新)
    pub async fn batch_update_by_type(dict_type: u16, dict_names: &[String]) -> DbResult<()> {
        let recs = Self::select_by_type(dict_type).await?;
        let now = LocalTime::now();
        let mut new_code = 0;
        let update_count = std::cmp::min(recs.len(), dict_names.len());
        let mut trans = gensql::start_transaction().await?;

        // 使用已存在的记录进行更新
        for item in recs.iter().take(update_count).zip(dict_names.iter()) {
            let dict = SysDict {
                dict_id: item.0.dict_id,
                dict_code: Some(new_code),
                dict_name: Some(item.1.to_string()),
                updated_time: Some(now.clone()),
                ..Default::default()
            };

            let (sql, params) = Self::prepare_update_by_id_selective(dict);
            trans.exec(sql, params).await?;

            new_code += 1;
        }

        // 增加新记录以保存多出来的数据
        if dict_names.len() > update_count {
            for item in dict_names.iter().skip(update_count) {
                let dict = SysDict {
                    dict_type: Some(dict_type),
                    dict_code: Some(new_code),
                    dict_name: Some(String::from(item)),
                    updated_time: Some(now.clone()),
                    ..Default::default()
                };

                let (sql, params) = Self::prepare_insert(dict);
                trans.exec(sql, params).await?;

                new_code += 1;
            }
        }

        // 删除多余的记录
        if recs.len() > update_count {
            for item in recs.iter().skip(update_count) {
                let id = item.dict_id.unwrap();
                let sql = Self::sql_delete_by_id();
                trans.exec(sql, gensql::to_values!(id)).await?;
            }
        }

        trans.commit().await?;

        Ok(())
    }

}
