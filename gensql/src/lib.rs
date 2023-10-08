//! generator sql module
// author: kiven
// slince 2023-06-15

mod sql_builder;
mod faststr;
cfg_if::cfg_if! {
    if #[cfg(feature = "mysql")] {
        mod db_mysql;
        use db_mysql as db;
    // } else if #[cfg(feature = "postgresql")] {
    }
}

pub use serde;
pub use db::{
    Conn, Row, Value, FromRow, FromValue, ToValue, Queryable, Transaction,
    ConvIr, FromValueError,
    get_conn, try_connect, init_pool,
    exec_sql, insert_sql, query_all_sql, query_one_sql,
};
pub use sql_builder::{
    GenSqlError, GeneratorSql, SelectSql, InsertSql, UpdateSql, DeleteSql,
    TrimSql, WhereSql, log_sql, log_sql_params, trans_to_select_count
};
pub use faststr::FastStr;
pub use gensql_derive::Table;


#[macro_export]
macro_rules! vec_value {
    ($($x:expr),+ $(,)?) => {
        vec![ $($crate::ToValue::to_value(&$x),)* ]
    };
}

#[macro_export]
macro_rules! row_map {
    ($data_type:tt, $($e:tt,)*) => {
        |( $( $e, )*)| $data_type {
            $( $e,)*
            ..Default::default()
        }
    };
}

#[macro_export]
macro_rules! option_struct {
    ( $struct_name:ident, $($field:ident : $f_type:tt,)+ ) => {
        #[derive($crate::serde::Serialize, $crate::serde::Deserialize, Default, Debug, Clone)]
        #[serde(rename_all = "camelCase")]
        pub struct $struct_name {
                #[serde(skip_serializing_if = "Option::is_none")]
                pub $id_name: Option<$id_type>,
            $(
                #[serde(skip_serializing_if = "Option::is_none")]
                pub $field: Option<$f_type>,
            )*
        }
    }
}

#[macro_export]
macro_rules! table_flatten {
    ( $struct_name:ident, $parent_struct:ident, $($field:ident : $f_type:ty,)+ ) => {
        #[derive($crate::serde::Serialize, $crate::serde::Deserialize, Default, Debug, Clone, $crate::Table)]
        #[serde(rename_all = "camelCase")]
        pub struct $struct_name {
            #[serde(flatten)]
            pub inner: $parent_struct,
            $(
                #[serde(skip_serializing_if = "Option::is_none")]
                pub $field: Option<$f_type>,
            )*
        }
    }
}

/// 创建数据库表的结构宏
///
/// ## Examples
///
/// ```rust
/// use gensql;
///
/// gensql::table_define!(t_sys_config, SysConfig,
///     cfg_id: u32 => CFG_ID,
///     cfg_name: String => CFG_NAME,
///     cfg_value: String => CFG_VALUE,
///     updated_time: LocalTime => UPDATED_TIME,
///     cfg_remark: String => CFG_REMARK,
/// );
///
/// ```
#[macro_export]
macro_rules! table_define {

    (@unit $($x:tt)*) => (());
    (@count $($x:expr),*) => (<[()]>::len(&[$($crate::table_define!(@unit $x)),*]));

    ( $table_name:literal, $struct_name:ident, $id_name:ident: $id_type:ty,
            $($field:ident : $f_type:ty,)+ ) => {

        #[derive($crate::serde::Serialize, $crate::serde::Deserialize, Default, Debug, Clone, $crate::Table)]
        #[serde(rename_all = "camelCase")]
        #[table(name = $table_name)]
        pub struct $struct_name {
                #[serde(skip_serializing_if = "Option::is_none")]
                #[table(id)]
                pub $id_name: Option<$id_type>,
            $(
                #[serde(skip_serializing_if = "Option::is_none")]
                pub $field: Option<$f_type>,
            )*
        }

        impl $struct_name {
            /// 删除记录
            pub async fn delete_by_id(id: &$id_type) -> Result<u32> {
                let (sql, params) = Self::stmt_delete(id);
                $crate::exec_sql(&sql, &params).await
            }

            /// 插入记录，返回(插入记录数量, 自增ID的值)
            pub async fn insert(value: &Self) -> Result<(u32, u32)> {
                let (sql, params) = Self::stmt_insert(value);
                $crate::insert_sql(&sql, &params).await
            }

            /// 更新记录
            pub async fn update_by_id(value: &Self) -> Result<u32> {
                let (sql, params) = Self::stmt_update(value);
                $crate::exec_sql(&sql, &params).await
            }

            /// 动态字段更新, 只更新有值的字段
            pub async fn update_dyn_by_id(value: &Self) -> Result<u32> {
                let (sql, params) = Self::stmt_update_dynamic(value);
                $crate::exec_sql(&sql, &params).await
            }

            /// 查询记录
            pub async fn select_by_id(id: &$id_type) -> Result<Option<Self>> {
                let (sql, params) = Self::stmt_select(id);
                Ok($crate::query_one_sql(&sql, &params).await?.map(Self::row_map))
            }

            pub fn stmt_delete(id: &$id_type) -> (String, Vec<$crate::Value>) {
                use $crate::ToValue;

                let mut sql = String::new();
                sql.push_str("delete from ");
                sql.push_str($table_name);
                sql.push_str(" where ");
                sql.push_str(stringify!($id_name));
                sql.push_str(" = ?");

                let params = vec![id.to_value()];

                $crate::log_sql_params(&sql, &params);

                (sql, params)
            }

            pub fn stmt_insert(val: &$struct_name) -> (String, Vec<$crate::Value>) {
                use $crate::ToValue;

                let params = vec![
                    val.$id_name.to_value(),
                    $(
                        val.$field.to_value(),
                    )*
                ];

                let mut sql = String::new();
                sql.push_str("insert into ");
                sql.push_str($table_name);
                sql.push_str(" (");
                sql.push_str(stringify!($id_name));
                $(
                    sql.push_str(", ");
                    sql.push_str(stringify!($field));
                )*
                sql.push_str(") values (?");
                for i in 1..params.len() { sql.push_str(", ?"); }
                sql.push_str(")");

                $crate::log_sql_params(&sql, &params);

                (sql, params)
            }

            pub fn stmt_select(id: &$id_type) -> (String, Vec<$crate::Value>) {
                use $crate::ToValue;

                let mut sql = String::new();
                sql.push_str("select ");
                sql.push_str(stringify!($id_name));
                $(
                    sql.push_str(", ");
                    sql.push_str(stringify!($field));
                )*
                sql.push_str(" from ");
                sql.push_str($table_name);
                sql.push_str(" where ");
                sql.push_str(stringify!($id_name));
                sql.push_str(" = ?");

                let params = vec![id.to_value()];

                $crate::log_sql_params(&sql, &params);

                (sql, params)
            }

            pub fn stmt_update(val: &$struct_name) -> (String, Vec<$crate::Value>) {
                use $crate::ToValue;

                let mut sql = String::new();
                sql.push_str("update ");
                sql.push_str($table_name);
                sql.push_str(" set ");
                $(
                    sql.push_str(stringify!($field));
                    sql.push_str(" = ?, ");
                )*
                sql.truncate(sql.len() - 2);
                sql.push_str(" where ");
                sql.push_str(stringify!($id_name));
                sql.push_str(" = ?");

                let params = vec![
                    $( val.$field.to_value(), )*
                    val.$id_name.to_value(),
                ];

                $crate::log_sql_params(&sql, &params);

                (sql, params)
            }

            pub fn stmt_update_dynamic(val: &$struct_name) -> (String, Vec<$crate::Value>) {
                use $crate::ToValue;

                let mut params = Vec::new();
                let mut sql = String::new();
                sql.push_str("update ");
                sql.push_str($table_name);
                sql.push_str(" set ");
                $(
                    if val.$field.is_some() {
                        sql.push_str(stringify!($field));
                        sql.push_str(" = ?, ");
                        params.push(val.$field.to_value());
                    }
                )*
                sql.truncate(sql.len() - 2);
                sql.push_str(" where ");
                sql.push_str(stringify!($id_name));
                sql.push_str(" = ?");
                params.push(val.$id_name.to_value());

                $crate::log_sql_params(&sql, &params);

                (sql, params)
            }

            pub fn row_map(val: $crate::Row) -> $struct_name {
                let ($id_name, $( $field, )*) = $crate::FromRow::from_row(val);
                $struct_name {
                    $id_name,
                    $( $field, )*
                }
            }

        }

    }
}
