pub mod sys_api;
pub mod sys_config;
pub mod sys_dict;
pub mod sys_menu;
pub mod sys_permission;
pub mod sys_role;
pub mod sys_user;
pub mod sys_user_state;

use crate::AppConf;

use anyhow::Result;
use mysql_async::{prelude::{Queryable, FromRow}, Conn, Pool, Params};
use serde::{Deserialize, Serialize};

static mut DB_POOL: DbPool = None;

type DbPool = Option<Box<Pool>>;

#[derive(Serialize, Deserialize)]
pub struct PageData<T> {
    pub total: u32,
    pub list: Vec<T>,
}

#[derive(Clone, Default)]
pub struct PageInfo {
    pub index: u32,
    pub size: u32,
}

#[derive(Deserialize)]
pub struct PageQuery<T> {
    #[serde(flatten)]
    pub inner: T,
    pub i: u32,
    pub p: u32,
}

impl PageInfo {
    pub fn new() -> Self {
        Self { index: 0, size: 0 }
    }

    #[allow(dead_code)]
    pub fn with(index: u32, size: u32) -> Self {
        Self { index, size }
    }
}

impl <T> PageQuery<T> {
    pub fn data(&self) -> &T {
        &self.inner
    }

    pub fn page(&self) -> PageInfo {
        PageInfo { index: self.i, size: self.p }
    }
}

#[macro_export]
macro_rules! sql_eq {
    ($t:literal) => {
        concat($t, " = :", $t)
    };
}

#[macro_export]
macro_rules! opt_params_map {
    ($obj:ident, $($e:tt,)*) => {{
        let mut params = std::collections::HashMap::<std::vec::Vec<u8>, mysql_async::Value>::new();
        let mut sql = String::new();

        $(
            if $obj.$e.is_some() {
                sql.push_str(" and ");
                sql.push_str(stringify!($e));
                sql.push_str(" = :");
                sql.push_str(stringify!($e));
                params.insert(stringify!($e).as_bytes().to_owned(), mysql_async::Value::from($obj.$e.as_ref().unwrap()));
            }
        )*

        if params.is_empty() {
            (sql, mysql_async::Params::Empty)
        } else {
            (sql, mysql_async::Params::Named(params))
        }
    }};
}

async fn get_conn() -> Result<Conn> {
    unsafe {
        debug_assert!(DB_POOL.is_some());
        match &DB_POOL {
            Some(pool) => Ok(pool.get_conn().await?),
            _ => std::hint::unreachable_unchecked(),
        }
    }
}

// redis连接测试
pub async fn try_connect() -> Result<()> {
    let mut conn = get_conn().await?;
    conn.ping().await?;
    Ok(())
}

pub fn init_pool(ac: &AppConf) -> Result<()> {
    debug_assert!(unsafe { DB_POOL.is_none() });

    let url = format!(
        "mysql://{}:{}@{}:{}/{}?{}",
        ac.db_user, ac.db_pass, ac.db_host, ac.db_port, ac.db_name, ac.db_extra
    );

    let pool = Box::new(Pool::from_url(url)?);
    unsafe {
        DB_POOL = Some(pool);
    }

    Ok(())
}

#[allow(dead_code)]
async fn exec_sql(val: &(String, Params)) -> Result<u32> {
    let mut conn = get_conn().await?;
    conn.exec_drop(&val.0, &val.1).await?;
    Ok(conn.affected_rows() as u32)
}

#[allow(dead_code)]
async fn insert_sql(val: &(String, Params)) -> Result<(u32, u32)> {
    let mut conn = get_conn().await?;
    conn.exec_drop(&val.0, &val.1).await?;
    let count = conn.affected_rows() as u32;
    let new_id = conn.last_insert_id().unwrap_or(0) as u32;
    Ok((count, new_id))
}

#[allow(dead_code)]
async fn query_all_sql<T, F, U>(val: &(String, Params), f: F) -> Result<Vec<U>>
where
    T: FromRow + Send + 'static,
    F: FnMut(T) -> U + Send,
    U: Send,
{
    let mut conn = get_conn().await?;
    Ok(conn.exec_map(&val.0, &val.1, f).await?)
}

#[allow(dead_code)]
async fn query_one_sql<T: FromRow + Send + 'static>(val: &(String, Params)) -> Result<Option<T>> {
    let mut conn = get_conn().await?;
    Ok(conn.exec_first(&val.0, &val.1).await?)
}
