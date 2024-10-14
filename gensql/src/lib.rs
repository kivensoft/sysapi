//! generator sql module
// author: kiven
// slince 2023-06-15

mod sql_builder;
cfg_if::cfg_if! {
    if #[cfg(feature = "mysql")] {
        mod db_mysql;
        use db_mysql as db;
    // } else if #[cfg(feature = "postgresql")] {
    }
}

pub use db::{
    get_conn, init_pool, sql_exec, sql_insert, sql_query, sql_query_fast,
    sql_query_map, sql_query_one, start_transaction, try_connect, Column,
    Conn, DbConfig, DbError, DbResult, FastFromRow, FromRow, FromRowError,
    FromValue, FromValueError, Params, Queryable, Row, StatementLike,
    ToValue, Transaction, Value,
};
pub use gensql_derive::table;
pub use serde;
pub use sql_builder::{
    db_log_params, db_log_sql, db_log_sql_params, trans_to_select_count,
    BatchDeleteSql, BatchInsertSql, DeleteSql, GenSqlError,
    InsertSql, InsertValue, SelectSql, TrimSql, UpdateSql, WhereSql,
};

#[macro_export]
macro_rules! to_values {
    ($($x:expr),+ $(,)?) => { vec![ $($crate::ToValue::to_value(&$x),)* ] };
}

#[macro_export]
macro_rules! option_struct {
    (
        $(#[$smeta:meta])*
        $struct_name:ident,
        $(#[$pmeta:meta])*
        $($field:tt : $f_type:tt,)+
    ) => {
        #[derive($crate::serde::Serialize, $crate::serde::Deserialize, Default, Debug, Clone)]
        #[serde(rename_all = "camelCase")]
        $(#[$smeta])*
        pub struct $struct_name {
            $(
                $(#[$fmeta])*
                #[serde(skip_serializing_if = "Option::is_none")]
                pub $field: Option<$f_type>,
            )*
        }
    };
}
