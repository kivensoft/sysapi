//! mysql implement
// author: kiven
// slince 2023-08-24

use futures::future::BoxFuture;
use futures::FutureExt;
use mysql_async::prelude::Queryable as QA;
pub use mysql_async::Error as DbError;
pub use mysql_async::{
    params::Params,
    prelude::{FromRow, FromValue, ToValue, StatementLike},
    Column, FromRowError, FromValueError, Row, Value,
};
use mysql_async::{Pool, TxOpts};

pub type DbResult<T> = Result<T, DbError>;

pub trait FastFromRow {
    fn fast_from_row(index_vec: &[i32], row: Row) -> Self;
    fn fast_map_index(columns: &[&[u8]]) -> Vec<i32>;
}

pub trait Queryable {
    fn exec<'a, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'a, DbResult<u32>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a;

    fn insert<'a, S, P>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<(u32, u32)>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a;

    fn query_one<'a, S, P, T>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<Option<T>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        T: FromRow + Send + 'static;

    fn query<'a, S, P, F, U>(
        &'a mut self,
        stmt: S,
        params: P,
        f: F,
    ) -> BoxFuture<'a, DbResult<Vec<U>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        F: FnMut(Row) -> U + Send + 'a,
        U: Send + 'static;

    fn query_fold<'a, S, P, F, U>(
        &'a mut self,
        stmt: S,
        params: P,
        init: U,
        f: F,
    ) -> BoxFuture<'a, DbResult<U>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        F: FnMut(U, Row) -> U + Send + 'a,
        U: Send + 'static;

    fn query_fast<'a, S, P, U>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<Vec<U>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        U: FastFromRow + Send;

    fn exec_batch<'a, S, P, I>(&'a mut self, stmt: S, params_iter: I) -> BoxFuture<'a, DbResult<()>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        I: IntoIterator<Item = P> + Send + 'a,
        I::IntoIter: Send;
}

pub struct Conn(mysql_async::Conn);
pub struct Transaction<'a>(mysql_async::Transaction<'a>);

static mut DB_POOL: std::mem::MaybeUninit<Pool> = std::mem::MaybeUninit::uninit();
#[cfg(debug_assertions)]
static mut INITED: bool = false;

/// 初始化连接池(必须在程序开始时调用)
pub fn init_pool(user: &str, pass: &str, host: &str, port: &str, db: &str) -> DbResult<()> {
    let url = format!("mysql://{}:{}@{}:{}/{}", user, pass, host, port, db);
    let p = Pool::from_url(url)?;
    unsafe {
        #[cfg(debug_assertions)]
        {
            if INITED {
                panic!("init_pool already run");
            }
            INITED = true;
        }
        DB_POOL.write(p);
    }
    Ok(())
}

/// 从连接池中获取连接
pub async fn get_conn() -> DbResult<Conn> {
    let c = get_db_pool().get_conn().await?;
    Ok(Conn(c))
}

/// 从连接池中开启事务并返回事务对象
pub async fn start_transaction() -> DbResult<Transaction<'static>> {
    let trans = get_db_pool().start_transaction(TxOpts::new()).await?;
    Ok(Transaction(trans))
}

// mysql连接测试
pub async fn try_connect() -> DbResult<()> {
    let mut conn = get_db_pool().get_conn().await?;
    QA::ping(&mut conn).await
}

/// 执行sql, 返回受影响的记录数
pub async fn sql_exec<S, P>(stmt: S, params: P) -> DbResult<u32>
where
    S: StatementLike,
    P: Into<Params> + Send,
{
    // inner_exec_sql(&mut inner_get_conn().await?, stmt, params).await
    let mut conn = get_db_pool().get_conn().await?;
    QA::exec_drop(&mut conn, stmt, params).await?;
    Ok(conn.affected_rows() as u32)
}

/// 插入记录专用sql, 返回受影响的记录数和新记录的id(如果没有新纪录id则返回0)
pub async fn sql_insert<S, P>(stmt: S, params: P) -> DbResult<(u32, u32)>
where
    S: StatementLike,
    P: Into<Params> + Send,
{
    let mut conn = get_db_pool().get_conn().await?;
    QA::exec_drop(&mut conn, stmt, params).await?;
    let count = conn.affected_rows() as u32;
    let new_id = conn.last_insert_id().unwrap_or(0) as u32;
    Ok((count, new_id))
}

/// 执行单条记录查询sql, 返回单条记录, 找不到记录返回None
pub async fn sql_query_one<S, P, T>(stmt: S, params: P) -> DbResult<Option<T>>
where
    S: StatementLike,
    P: Into<Params> + Send,
    T: FromRow + Send + 'static,
{
    let mut conn = get_db_pool().get_conn().await?;
    QA::exec_first(&mut conn, stmt, params).await
}

/// 执行查询多条记录的sql, 返回记录列表
pub async fn sql_query<S, P, F, U>(stmt: S, params: P, f: F) -> DbResult<Vec<U>>
where
    S: StatementLike,
    P: Into<Params> + Send,
    F: FnMut(Row) -> U + Send,
    U: Send,
{
    let mut conn = get_db_pool().get_conn().await?;
    QA::exec_map(&mut conn, stmt, params, f).await
}

/// 执行给定查询, 将返回的结果合并到给定的变量
#[allow(dead_code)]
pub async fn sql_query_fold<S, P, F, U>(stmt: S, params: P, init: U, f: F) -> DbResult<U>
where
    S: StatementLike,
    P: Into<Params> + Send,
    F: FnMut(U, Row) -> U + Send,
    U: Send,
{
    let mut conn = get_db_pool().get_conn().await?;
    QA::exec_fold(&mut conn, stmt, params, init, f).await
}

/// 执行查询多条记录的sql, 返回记录列表(首次转换结果时记录列对应的序号，加快处理速度)
#[allow(dead_code)]
pub async fn sql_query_fast<S, P, U>(stmt: S, params: P) -> DbResult<Vec<U>>
where
    S: StatementLike,
    P: Into<Params> + Send,
    U: FastFromRow + Send,
{
    let mut conn = get_db_pool().get_conn().await?;
    _sql_query_fast(&mut conn, stmt, params).await
}

/// 使用参数流批量执行给定查询
#[allow(dead_code)]
pub async fn sql_exec_batch<S, P, I>(stmt: S, params_iter: I) -> DbResult<()>
where
    S: StatementLike,
    P: Into<Params> + Send,
    I: IntoIterator<Item = P> + Send,
    I::IntoIter: Send,
{
    let mut conn = get_db_pool().get_conn().await?;
    QA::exec_batch(&mut conn, stmt, params_iter).await
}

impl Queryable for Conn {
    fn exec<'a, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'a, DbResult<u32>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
    {
        async move {
            QA::exec_drop(&mut self.0, stmt, params).await?;
            Ok(self.0.affected_rows() as u32)
        }
        .boxed()
    }

    fn insert<'a, S, P>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<(u32, u32)>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
    {
        async move {
            QA::exec_drop(&mut self.0, stmt, params).await?;
            let count = self.0.affected_rows() as u32;
            let new_id = self.0.last_insert_id().unwrap_or(0) as u32;
            Ok((count, new_id))
        }
        .boxed()
    }

    fn query_one<'a, S, P, T>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<Option<T>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        T: FromRow + Send + 'static,
    {
        QA::exec_first(&mut self.0, stmt, params)
    }

    fn query<'a, S, P, F, U>(
        &'a mut self,
        stmt: S,
        params: P,
        f: F,
    ) -> BoxFuture<'a, DbResult<Vec<U>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        F: FnMut(Row) -> U + Send + 'a,
        U: Send + 'a,
    {
        QA::exec_map(&mut self.0, stmt, params, f)
    }

    fn query_fold<'a, S, P, F, U>(
        &'a mut self,
        stmt: S,
        params: P,
        init: U,
        f: F,
    ) -> BoxFuture<'a, DbResult<U>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        F: FnMut(U, Row) -> U + Send + 'a,
        U: Send + 'static,
    {
        QA::exec_fold(&mut self.0, stmt, params, init, f)
    }

    fn query_fast<'a, S, P, U>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<Vec<U>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        U: FastFromRow + Send,
    {
        _sql_query_fast(&mut self.0, stmt, params)
    }

    fn exec_batch<'a, S, P, I>(&'a mut self, stmt: S, params_iter: I) -> BoxFuture<'a, DbResult<()>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        I: IntoIterator<Item = P> + Send + 'a,
        I::IntoIter: Send,
    {
        QA::exec_batch(&mut self.0, stmt, params_iter)
    }
}

impl Conn {
    pub async fn start_transaction(&mut self) -> DbResult<Transaction<'_>> {
        Ok(Transaction(
            (&mut self.0).start_transaction(TxOpts::new()).await?,
        ))
    }
}

impl Queryable for Transaction<'_> {
    fn exec<'a, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'a, DbResult<u32>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
    {
        async move {
            QA::exec_drop(&mut self.0, stmt, params).await?;
            Ok(self.0.affected_rows() as u32)
        }
        .boxed()
    }

    fn insert<'a, S, P>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<(u32, u32)>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
    {
        async move {
            QA::exec_drop(&mut self.0, stmt, params).await?;
            let count = self.0.affected_rows() as u32;
            let new_id = self.0.last_insert_id().unwrap_or(0) as u32;
            Ok((count, new_id))
        }
        .boxed()
    }

    fn query_one<'a, S, P, T>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<Option<T>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        T: FromRow + Send + 'static,
    {
        QA::exec_first(&mut self.0, stmt, params)
    }

    fn query<'a, S, P, F, U>(
        &'a mut self,
        stmt: S,
        params: P,
        f: F,
    ) -> BoxFuture<'a, DbResult<Vec<U>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        F: FnMut(Row) -> U + Send + 'a,
        U: Send + 'static,
    {
        QA::exec_map(&mut self.0, stmt, params, f)
    }

    fn query_fold<'a, S, P, F, U>(
        &'a mut self,
        stmt: S,
        params: P,
        init: U,
        f: F,
    ) -> BoxFuture<'a, DbResult<U>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        F: FnMut(U, Row) -> U + Send + 'a,
        U: Send + 'static,
    {
        QA::exec_fold(&mut self.0, stmt, params, init, f)
    }

    fn query_fast<'a, S, P, U>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'a, DbResult<Vec<U>>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        U: FastFromRow + Send,
    {
        _sql_query_fast(&mut self.0, stmt, params)
    }

    fn exec_batch<'a, S, P, I>(&'a mut self, stmt: S, params_iter: I) -> BoxFuture<'a, DbResult<()>>
    where
        S: StatementLike + 'a,
        P: Into<Params> + Send + 'a,
        I: IntoIterator<Item = P> + Send + 'a,
        I::IntoIter: Send,
    {
        QA::exec_batch(&mut self.0, stmt, params_iter)
    }
}

impl Transaction<'_> {
    pub async fn commit(self) -> DbResult<()> {
        Ok(self.0.commit().await?)
    }

    pub async fn rollback(self) -> DbResult<()> {
        Ok(self.0.rollback().await?)
    }
}

fn get_db_pool() -> &'static Pool {
    unsafe {
        #[cfg(debug_assertions)]
        if !INITED {
            panic!("DB_POOL has not been initialized yet");
        }
        &*DB_POOL.as_ptr()
    }
}

pub fn _sql_query_fast<'a, C, S, P, U>(
    conn: &'a mut C,
    stmt: S,
    params: P,
) -> BoxFuture<'a, DbResult<Vec<U>>>
where
    C: QA,
    S: StatementLike + 'a,
    P: Into<Params> + Send + 'a,
    U: FastFromRow + Send,
{
    async move {
        let mut qr = QA::exec_iter(conn, stmt, params).await?;
        let mut vals = Vec::with_capacity(10);
        let columns: Vec<_> = qr.columns_ref().iter().map(|v| v.name_ref()).collect();
        let idxes = U::fast_map_index(&columns);
        while let Some(row) = qr.next().await? {
            vals.push(U::fast_from_row(&idxes, row));
        }
        Ok(vals)
    }
    .boxed()
}
