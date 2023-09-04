//! mysql implement

use anyhow::Result;
use mysql_async::{Pool, prelude::{self, FromRow, StatementLike}, Params, TxOpts};

pub use mysql_common::value::{convert::ToValue, Value};

#[async_trait::async_trait]
pub trait Queryable {
    async fn exec_sql<S, P>(&mut self, stmt: S, params: P) -> Result<u32>
    where
        S: StatementLike,
        P: Into<Params> + Send;

    async fn insert_sql<S, P>(&mut self, stmt: S, params: P) -> Result<(u32, u32)>
    where
        S: StatementLike,
        P: Into<Params> + Send;

    async fn query_one_sql<S, P, T>(&mut self, stmt: S, params: P) -> Result<Option<T>>
    where
        S: StatementLike,
        P: Into<Params> + Send,
        T: FromRow + Send + 'static;

    async fn query_all_sql<S, P, F, T, U>(&mut self, stmt: S, params: P, f: F) -> Result<Vec<U>>
    where
        S: StatementLike,
        P: Into<Params> + Send,
        F: FnMut(T) -> U + Send,
        T: FromRow + Send + 'static,
        U: Send;

}

pub struct Conn(mysql_async::Conn);
pub struct Transaction<'a>(mysql_async::Transaction<'a>);

type DbPool = Option<Pool>;

static DB_POOL: DbPool = None;

/// 从连接池中获取连接
pub async fn get_conn() -> Result<Conn> {
    debug_assert!(DB_POOL.is_some());
    match &DB_POOL {
        Some(pool) => Ok(Conn(pool.get_conn().await?)),
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

// mysql连接测试
pub async fn try_connect() -> Result<()> {
    let mut conn = inner_get_conn().await?;
    prelude::Queryable::ping(&mut conn).await?;
    Ok(())
}

/// 初始化连接池(必须在程序开始时调用)
pub fn init_pool(user: &str, pass: &str, host: &str, port: &str, db: &str) -> Result<()> {
    debug_assert!(DB_POOL.is_none());

    let url = format!("mysql://{}:{}@{}:{}/{}", user, pass, host, port, db);

    unsafe {
        let db_pool = &DB_POOL as *const DbPool as *mut DbPool;
        *db_pool = Some(Pool::from_url(url)?);
    }

    Ok(())
}

/// 执行sql, 返回受影响的记录数
pub async fn exec_sql<S, P>(stmt: S, params: P) -> Result<u32>
where
    S: StatementLike,
    P: Into<Params> + Send,
{
    // inner_exec_sql(&mut inner_get_conn().await?, stmt, params).await
    let mut conn = inner_get_conn().await?;
    prelude::Queryable::exec_drop(&mut conn, stmt, params).await?;
    Ok(conn.affected_rows() as u32)
}

/// 插入记录专用sql, 返回受影响的记录数和新记录的id(如果没有新纪录id则返回0)
pub async fn insert_sql<S, P>(stmt: S, params: P) -> Result<(u32, u32)>
where
    S: StatementLike,
    P: Into<Params> + Send,
{
    let mut conn = inner_get_conn().await?;
    prelude::Queryable::exec_drop(&mut conn, stmt, params).await?;
    let count = conn.affected_rows() as u32;
    let new_id = conn.last_insert_id().unwrap_or(0) as u32;
    Ok((count, new_id))
}

/// 执行单条记录查询sql, 返回单条记录, 找不到记录返回None
pub async fn query_one_sql<S, P, T>(stmt: S, params: P) -> Result<Option<T>>
where
    S: StatementLike,
    P: Into<Params> + Send,
    T: FromRow + Send + 'static,
{
    Ok(prelude::Queryable::exec_first(&mut inner_get_conn().await?, stmt, params).await?)
}

/// 执行查询多条记录的sql, 返回记录列表
pub async fn query_all_sql<S, P, F, T, U>(stmt: S, params: P, f: F) -> Result<Vec<U>>
where
    S: StatementLike,
    P: Into<Params> + Send,
    F: FnMut(T) -> U + Send,
    T: FromRow + Send + 'static,
    U: Send,
{
    Ok(prelude::Queryable::exec_map(&mut inner_get_conn().await?, stmt, params, f).await?)
}

#[async_trait::async_trait]
impl Queryable for Conn {
    async fn exec_sql<S, P>(&mut self, stmt: S, params: P) -> Result<u32>
    where
        S: StatementLike,
        P: Into<Params> + Send,
    {
        prelude::Queryable::exec_drop(&mut self.0, stmt, params).await?;
        Ok(self.0.affected_rows() as u32)
    }

    async fn insert_sql<S, P>(&mut self, stmt: S, params: P) -> Result<(u32, u32)>
    where
        S: StatementLike,
        P: Into<Params> + Send,
    {
        prelude::Queryable::exec_drop(&mut self.0, stmt, params).await?;
        let count = self.0.affected_rows() as u32;
        let new_id = self.0.last_insert_id().unwrap_or(0) as u32;
        Ok((count, new_id))
    }

    async fn query_one_sql<S, P, T>(&mut self, stmt: S, params: P) -> Result<Option<T>>
    where
        S: StatementLike,
        P: Into<Params> + Send,
        T: FromRow + Send + 'static
    {
        Ok(prelude::Queryable::exec_first(&mut self.0, stmt, params).await?)
    }

    async fn query_all_sql<S, P, F, T, U>(&mut self, stmt: S, params: P, f: F) -> Result<Vec<U>>
    where
        S: StatementLike,
        P: Into<Params> + Send,
        F: FnMut(T) -> U + Send,
        T: FromRow + Send + 'static,
        U: Send,
    {
        Ok(prelude::Queryable::exec_map(&mut self.0, stmt, params, f).await?)
    }
}

impl Conn {
    pub async fn start_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(Transaction((&mut self.0).start_transaction(TxOpts::new()).await?))
    }
}

#[async_trait::async_trait]
impl Queryable for Transaction<'_> {
    async fn exec_sql<S, P>(&mut self, stmt: S, params: P) -> Result<u32>
    where
        S: StatementLike,
        P: Into<Params> + Send,
    {
        prelude::Queryable::exec_drop(&mut self.0, stmt, params).await?;
        Ok(self.0.affected_rows() as u32)
    }

    async fn insert_sql<S, P>(&mut self, stmt: S, params: P) -> Result<(u32, u32)>
    where
        S: StatementLike,
        P: Into<Params> + Send,
    {
        prelude::Queryable::exec_drop(&mut self.0, stmt, params).await?;
        let count = self.0.affected_rows() as u32;
        let new_id = self.0.last_insert_id().unwrap_or(0) as u32;
        Ok((count, new_id))
    }

    async fn query_one_sql<S, P, T>(&mut self, stmt: S, params: P) -> Result<Option<T>>
    where
        S: StatementLike,
        P: Into<Params> + Send,
        T: FromRow + Send + 'static
    {
        Ok(prelude::Queryable::exec_first(&mut self.0, stmt, params).await?)
    }

    async fn query_all_sql<S, P, F, T, U>(&mut self, stmt: S, params: P, f: F) -> Result<Vec<U>>
    where
        S: StatementLike,
        P: Into<Params> + Send,
        F: FnMut(T) -> U + Send,
        T: FromRow + Send + 'static,
        U: Send,
    {
        Ok(prelude::Queryable::exec_map(&mut self.0, stmt, params, f).await?)
    }
}

impl Transaction<'_> {
    pub async fn commit(self) -> Result<()> {
        Ok(self.0.commit().await?)
    }

    pub async fn rollback(self) -> Result<()> {
        Ok(self.0.rollback().await?)
    }
}

async fn inner_get_conn() -> Result<mysql_async::Conn> {
    debug_assert!(DB_POOL.is_some());
    match &DB_POOL {
        Some(pool) => Ok(pool.get_conn().await?),
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}
