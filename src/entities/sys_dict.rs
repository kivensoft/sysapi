//! 字典表
use std::sync::Arc;

use anyhow_ext::{Context, Result};
use gensql::{
    db_log_params, db_log_sql, table, to_values, BatchDeleteSql, BatchInsertSql, DbResult,
    InsertValue, Params, Queryable, ToValue,
};
use httpserver::log_debug;
use localtime::LocalTime;

use crate::{services::gmc, utils::{consts, IntStr}};

use super::{PageData, PageInfo};

pub const BUILTIN_GROUP_CODE: i8 = -1;
pub const BUILTIN_GROUP_NAME: &str = "内置权限";

const CATEGORY: &str = consts::gmc::SYS_DICT;

#[allow(dead_code)]
pub enum DictType {
    /// 字典类别
    DictCategory,
    /// 配置类别
    ConfigCategory,
    /// 权限组
    PermissionGroup,
    /// 客户端类别
    ClientCategory,
}

/// 系统接口表
#[table("t_sys_dict")]
pub struct SysDict {
    /// 字典项id
    #[table(id)]
    dict_id: u32,
    /// 字典项类型
    dict_type: u8,
    /// 字典项代码
    dict_code: String,
    /// 字典项名称
    dict_name: String,
    /// 更新时间
    updated_time: LocalTime,
}

#[table]
pub struct SysDictExt {
    #[serde(flatten)]
    inner: SysDict,

    /// 字典项类型名称
    #[table(not_field)]
    dict_type_name: String,
}

impl SysDict {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let dict_type = self.dict_type;
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(dict_type).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let dict_type = self.dict_type;
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(dict_type).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(record) => {
                let dict_type = record.dict_type;
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(dict_type).await;
                }
                ret
            }
            None => Ok(false),
        }
    }

    pub async fn notify_changed(ty: Option<u8>) {
        let ty = match ty {
            Some(n) => format!("{n}"),
            None => String::new(),
        };
        gmc::get_cache().notify(CATEGORY, &ty).await
    }

    /// 查询记录
    pub async fn select_page(query: SysDict, page: PageInfo) -> DbResult<PageData<SysDictExt>> {
        let (t, t1) = ("t", "t1");

        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_all_with_table(t)
            .select_as(t1, Self::DICT_NAME, SysDictExt::DICT_TYPE_NAME)
            .from_as(Self::TABLE_NAME, t)
            .left_join(Self::TABLE_NAME, t1, |j| {
                j.on_eq(Self::DICT_ID, t, Self::DICT_TYPE)
            })
            .where_sql(|w| {
                w.eq_opt(t, Self::DICT_TYPE, query.dict_type)
                    .expr(t, Self::DICT_TYPE, "!=", DictType::DictCategory as u8)
                    .like_opt(t, Self::DICT_NAME, query.dict_name)
            })
            .order_by_columns(t, &[Self::DICT_TYPE, Self::DICT_CODE])
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 返回指定类型的所有字典项
    pub async fn select_by_type(dict_type: u8) -> DbResult<Arc<Vec<SysDict>>> {
        // 优先从缓存中读取
        let mut buf = itoa::Buffer::new();
        let dt_str = buf.format(dict_type);
        if let Some(dicts) = gmc::get_cache().get_json(CATEGORY, dt_str).await {
            return Ok(dicts);
        }

        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| w.eq("", Self::DICT_TYPE, dict_type))
            .order_by("", Self::DICT_CODE)
            .build();
        let list = Arc::new(gensql::sql_query_fast(&sql, params).await?);
        if !list.is_empty() {
            gmc::get_cache().put_json(CATEGORY, dt_str, list.clone()).await;
        }
        Ok(list)
    }

    pub async fn get_permission_groups(use_builltin: bool) -> Result<Vec<IntStr>> {
        let list = SysDict::select_by_type(DictType::PermissionGroup as u8).await?;

        let mut res = Vec::with_capacity(list.len() + 1);
        if use_builltin {
            res.push(IntStr {
                key: BUILTIN_GROUP_CODE as i64,
                value: BUILTIN_GROUP_NAME.to_string(),
            });
        }

        let empty_str = String::with_capacity(0);
        for item in list.iter() {
            res.push(IntStr {
                key: item.dict_code.as_ref().unwrap_or(&empty_str).parse().dot()?,
                value: item.dict_name.as_ref().unwrap_or(&empty_str).to_string(),
            });
        }

        Ok(res)
    }

    /// 批量更新指定的类别(使用事务进行更新)
    pub async fn batch_by_type(dict_type: u8, dicts: Vec<SysDict>) -> DbResult<()> {
        let old_dicts = Self::select_by_type(dict_type).await?;
        let update_count = std::cmp::min(old_dicts.len(), dicts.len());
        let mut trans = gensql::start_transaction().await?;

        // 使用已存在的记录进行更新
        let sql = format!(
            "update {} set {} = ?, {} = ?, {} = now() where {} = ?",
            Self::TABLE_NAME,
            Self::DICT_CODE,
            Self::DICT_NAME,
            Self::UPDATED_TIME,
            Self::DICT_ID
        );
        db_log_sql(&sql);
        let mut update_dicts = Vec::with_capacity(old_dicts.len());
        for (old, new) in old_dicts.iter().take(update_count).zip(dicts.iter()) {
            let dict = to_values!(new.dict_code, new.dict_name, old.dict_id);
            db_log_params(&dict);
            update_dicts.push(dict);
        }
        trans.exec_batch(sql, update_dicts).await?;

        // 增加新记录以保存多出来的数据
        if dicts.len() > update_count {
            let (sql, params) = BatchInsertSql::new(
                Self::TABLE_NAME,
                &[
                    Self::DICT_TYPE,
                    Self::DICT_CODE,
                    Self::DICT_NAME,
                    Self::UPDATED_TIME,
                ],
            )
            .values(dicts.iter().skip(update_count).map(|v| {
                vec![
                    InsertValue::Value(dict_type.to_value()),
                    InsertValue::Value(v.dict_code.to_value()),
                    InsertValue::Value(v.dict_name.to_value()),
                    InsertValue::Sql("now()"),
                ]
            }))
            .build();

            trans.exec(sql, params).await?;
        }

        // 删除多余的记录
        if old_dicts.len() > update_count {
            let (sql, params) = BatchDeleteSql::new(
                Self::TABLE_NAME,
                Self::DICT_ID,
                old_dicts
                    .iter()
                    .skip(update_count)
                    .map(|v| v.dict_id.unwrap_or(0)),
            )
            .build();

            trans.exec(sql, params).await?;
        }

        trans.commit().await?;
        Self::notify_changed(None).await;

        Ok(())
    }

    /// 对字典项的id进行重新排序
    pub async fn resort(req_id: u32) -> DbResult<()> {
        // 加载所有记录
        let (sql, params) = gensql::SelectSql::new()
            .select_columns("", &[Self::DICT_ID, Self::DICT_TYPE])
            .from(Self::TABLE_NAME)
            .order_by_columns("", &[Self::DICT_TYPE, Self::DICT_CODE])
            .build();
        let dicts: Vec<SysDict> = gensql::sql_query_fast(sql, params).await?;
        if dicts.is_empty() {
            log_debug!(req_id, "未找到字典记录，忽略拍寻");
            return Ok(());
        }

        // 重新设置dict_id
        let (mut id1, mut id2) = (0, 100);
        let mut vec_id_map = Vec::with_capacity(dicts.len());
        for item in dicts.iter() {
            let id = if item.dict_type.unwrap() == 0 {
                id1 += 1;
                id1
            } else {
                id2 += 1;
                id2
            };

            let dict_id = item.dict_id.unwrap_or(0);
            if id != dict_id {
                vec_id_map.push((id, dict_id));
            }
        }
        if vec_id_map.is_empty() {
            log_debug!(req_id, "dict_id已经是排序好的，无需再次排序");
            return Ok(());
        }
        // 按旧id的升序排序，避免批量更新时id冲突
        vec_id_map.sort_by(|a, b| a.1.cmp(&b.1));

        // 更新sql
        let upd_sql = format!(
            "update {} set {} = ? where {} = ?",
            Self::TABLE_NAME,
            Self::DICT_ID,
            Self::DICT_ID
        );
        db_log_sql(&upd_sql);
        // 生成更新参数
        let vec_params: Vec<_> = vec_id_map
            .iter()
            .map(|(id, dict_id)| to_values!(id, dict_id))
            .inspect(|v| db_log_params(v))
            .collect();

        let mut trans = gensql::start_transaction().await?;
        // 批量更新
        trans.exec_batch(upd_sql, vec_params).await?;

        let sql = format!("alter table {} auto_increment = 1", Self::TABLE_NAME);
        db_log_sql(&sql);
        // 重置dict_id的自增值
        trans.exec(sql, Params::Empty).await?;

        trans.commit().await?;
        Self::notify_changed(None).await;

        Ok(())
    }

}
