//! 系统配置表
//!

use std::sync::Arc;

use super::{PageData, PageInfo};
use crate::{services::gmc, utils::consts};
use gensql::{table, DbResult, Queryable};
use localtime::LocalTime;

type CacheValueType = Option<Arc<String>>;
const CATEGORY: &str = consts::gmc::SYS_CONFIG;

/// 系统接口表
#[table("t_sys_config")]
pub struct SysConfig {
    /// 配置项id
    #[table(id)]
    cfg_id: u32,
    /// 配置项分类
    category: u8,
    /// 配置项名称
    cfg_name: String,
    /// 配置项内容
    cfg_value: String,
    /// 更新时间
    updated_time: LocalTime,
    /// 配置项备注
    cfg_remark: String,
}

impl SysConfig {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let name = String::from(self.cfg_name.as_ref().map_or("", |s| s.as_str()));
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(&name).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let name = String::from(self.cfg_name.as_ref().map_or("", |s| s.as_str()));
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(&name).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(cfg) => {
                let name = String::from(cfg.cfg_name.as_ref().map_or("", |s| s.as_str()));
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(&name).await;
                }
                ret
            }
            None => Ok(false),
        }
    }

    /// 数据变更通知
    pub async fn notify_changed(name: &str) {
        gmc::get_cache().notify(CATEGORY, name).await
    }

    /// 查询记录
    pub async fn select_page(value: SysConfig, page: PageInfo) -> DbResult<PageData<SysConfig>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| {
                w.like_opt("", Self::CFG_NAME, value.cfg_name)
                    .like_opt("", Self::CFG_VALUE, value.cfg_value)
                    .like_opt("", Self::CFG_REMARK, value.cfg_remark)
            })
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 获取配置项(优先从缓存中读取，缓存读取不到则从数据库中读取)
    pub async fn get_value(name: &str) -> DbResult<CacheValueType> {
        // 优先从缓存加载
        let cache = gmc::get_cache();
        let value = cache.get(CATEGORY, name).await;
        if value.is_some() {
            return Ok(value);
        }

        // 从数据库中加载
        let (sql, params) = gensql::SelectSql::new()
            .select("", Self::CFG_VALUE)
            .from(Self::TABLE_NAME)
            .where_sql(|w| w.eq("", Self::CFG_NAME, name))
            .build();

        let result: Option<String> = gensql::sql_query_one(sql, params).await?;

        // 写入缓存
        match result {
            Some(value) => {
                let value = Arc::new(value);
                cache.put(CATEGORY, name, value.clone()).await;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// 获取配置项(优先从缓存中读取，缓存读取不到则从数据库中读取)
    pub async fn get_or_init<F: Fn() -> SysConfig>(name: &str, init_fn: F) -> DbResult<CacheValueType> {
        // 如果成功加载数据则直接返回
        let value = Self::get_value(name).await?;
        if value.is_some() {
            return Ok(value);
        }

        // 写入缺省值到数据库
        let cfg = init_fn();
        let value = cfg.cfg_value.as_ref().map(|s| Arc::new(s.clone()));
        cfg.insert().await?;

        Ok(value)
    }

}
