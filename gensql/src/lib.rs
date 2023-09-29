//! generator sql module

cfg_if::cfg_if! {
    if #[cfg(feature = "mysql")] {
        mod db_mysql;
        use db_mysql as db;
    // } else if #[cfg(feature = "postgresql")] {
    }
}

pub use serde;
pub use db::{
    Conn, Row, Value, FromValue, ToValue, Queryable, Transaction,
    get_conn, try_connect, init_pool,
    exec_sql, insert_sql, query_all_sql, query_one_sql,
};

use compact_str::{CompactString, format_compact};
use itoa::Buffer;
use thiserror::Error;

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
macro_rules! struct_flatten {

    ( $struct_name:ident, $parent_struct:ident,
            $($field:ident : $f_type:ty => $field_name:ident,)+ ) => {

        #[derive($crate::serde::Serialize, $crate::serde::Deserialize, Default, Debug, Clone)]
        #[serde(rename_all = "camelCase")]
        pub struct $struct_name {
            #[serde(flatten)]
            pub inner: $parent_struct,
            $(
                #[serde(skip_serializing_if = "Option::is_none")]
                pub $field: Option<$f_type>,
            )*
        }

        impl $struct_name {
            $(
                pub const $field_name: &str = stringify!($field);
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

    ( $table_name:literal, $struct_name:ident, $id_name:ident: $id_type:ty => $id_alias:ident,
            $($field:ident : $f_type:ty => $field_name:ident,)+ ) => {

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

        impl $struct_name {
            pub const TABLE: &str = $table_name;
            pub const ID: &str = stringify!($id_name);
            pub const $id_alias: &str = stringify!($id_name);
            $(
                pub const $field_name: &str = stringify!($field);
            )*
            pub const FIELDS: &[&'static str; $crate::table_define!(@count $id_name, $($field),*)] = &[
                stringify!($id_alias), $(stringify!($field_name)),*
            ];

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

            pub fn row_map(val: (Option<$id_type>, $( Option<$f_type>, )*)) -> $struct_name {
                let ($id_name, $( $field, )*) = val;
                $struct_name {
                    $id_name,
                    $( $field, )*
                }
            }

        }

    }
}

const AND: &str = "and ";
const OR: &str = "or ";
const ON: &str = "on ";

#[derive(Error, Debug)]
pub enum GenSqlError {
    #[error("unsearch ` from ` in sql")]
    UnsearchFrom,
}

pub trait GeneratorSql {
    fn sql(&mut self) -> &mut Vec<u8>;
    fn params(&mut self) -> &mut Vec<Value>;
}

pub struct SelectSql {
    sql: Vec<u8>,
    params: Vec<Value>,
}

pub struct InsertSql {
    sql: Vec<u8>,
    params: Vec<Value>,
}

pub struct UpdateSql {
    sql: Vec<u8>,
    params: Vec<Value>,
}

pub struct DeleteSql {
    sql: Vec<u8>,
    params: Vec<Value>,
}

pub struct TrimSql<T: GeneratorSql> {
    parent: T,
    prefix: CompactString,
    suffix: CompactString,
    prefix_overrides: Vec<CompactString>,
    suffix_overrides: Vec<CompactString>,
}

pub struct WhereSql<T: GeneratorSql>(TrimSql<T>);


#[derive(PartialEq)]
enum LikeType {
    Full,
    Left,
    Right,
}

#[inline]
pub fn log_sql(sql: &str) {
    log::debug!("[SQL]: {sql}");
}

#[inline]
pub fn log_sql_params(sql: &str, params: &[mysql_common::value::Value]) {
    log::debug!("[SQL]: {sql}");
    log::debug!("[PARAMS]: {params:?}");
}

/// 将查询详细字段的语句转换为查询记录数量的语句
pub fn trans_to_select_count(select_sql: &str) -> Result<String, GenSqlError> {
    let from_pos = match select_sql.find(" from ") {
        Some(pos) => pos,
        None => return Err(GenSqlError::UnsearchFrom),
    };

    let limit_pos = match select_sql.rfind(" limit ") {
        Some(pos) => pos,
        None => select_sql.len(),
    };

    Ok(format!("select count(*){}", &select_sql[from_pos..limit_pos]))
}

impl GeneratorSql for SelectSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.params
    }
}

impl SelectSql {
    #[inline]
    pub fn new() -> Self {
        let mut sql = Vec::new();
        sql.extend_from_slice(b"select ");

        SelectSql {
            sql,
            params: Vec::new(),
        }
    }

    #[inline]
    pub fn select(mut self, col: &str) -> Self {
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b", ");
        self
    }

    pub fn select_ext(mut self, table: &str, col: &str) -> Self {
        if !table.is_empty() {
            self.sql.extend_from_slice(table.as_bytes());
            self.sql.push(b'.');
        }
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b", ");
        self
    }

    pub fn select_as(mut self, table: &str, col: &str, col_alias: &str) -> Self {
        if !table.is_empty() {
            self.sql.extend_from_slice(table.as_bytes());
            self.sql.push(b'.');
        }
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.push(b' ');
        self.sql.extend_from_slice(col_alias.as_bytes());
        self.sql.extend_from_slice(b", ");
        self
    }

    pub fn select_slice(mut self, table: &str, cols: &[&str]) -> Self {
        cols.iter().for_each(|col| {
            if !table.is_empty() {
                self.sql.extend_from_slice(table.as_bytes());
                self.sql.push(b'.');
            }
            self.sql.extend_from_slice(col.as_bytes());
            self.sql.extend_from_slice(b", ");
        });
        self
    }

    #[inline]
    pub fn from(self, table: &str) -> Self {
        self.from_alias(table, "")
    }

    pub fn from_alias(mut self, table: &str, alias: &str) -> Self {
        let sql = &mut self.sql;
        if sql.ends_with(b", ") {
            sql.truncate(sql.len() - 2);
        } else {
            sql.push(b'*');
        }
        sql.extend_from_slice(b" from ");
        sql.extend_from_slice(table.as_bytes());
        if !alias.is_empty() {
            sql.push(b' ');
            sql.extend_from_slice(alias.as_bytes());
        }
        self
    }

    #[inline]
    pub fn join(self, table: &str, alias: &str) -> Self {
        self.pri_join(" join ", table, alias)
    }

    #[inline]
    pub fn left_join(self, table: &str, alias: &str) -> Self {
        self.pri_join(" left join ", table, alias)
    }

    #[inline]
    pub fn right_join(self, table: &str, alias: &str) -> Self {
        self.pri_join(" right join ", table, alias)
    }

    #[inline]
    pub fn full_join(self, table: &str, alias: &str) -> Self {
        self.pri_join(" full join ", table, alias)
    }

    pub fn on(mut self, expr: &str) -> Self {
        let sql = &mut self.sql;
        if !sql.ends_with(ON.as_bytes()) {
            sql.push(b' ');
            sql.extend_from_slice(AND.as_bytes());
        }
        sql.extend_from_slice(expr.as_bytes());
        self
    }

    pub fn on_eq(self, table1: &str, col1: &str, table2: &str, col2: &str) -> Self {
        self.on(&format_compact!("{}{} = {}{}", _ta(table1), col1,
            _ta(table2), col2))
    }

    pub fn on_val<V: ToValue>(mut self, table: &str, col: &str, expr: &str, val: &V) -> Self {
        self.params.push(val.to_value());
        self.on(&format_compact!("{}{} {} ?", _ta(table), col, expr))
    }

    pub fn on_val_opt<V: ToValue>(self, table: &str, col: &str, expr: &str, val: &Option<V>) -> Self {
        match val {
            Some(val) => self.on_val(table, col, expr, val),
            None => self,
        }
    }

    pub fn on_eq_val<V: ToValue>(self, table: &str, col: &str, val: &V) -> Self {
        self.on_val(table, col, "=", val)
    }

    pub fn on_eq_val_opt<V: ToValue>(self, table: &str, col: &str, val: &Option<V>) -> Self {
        self.on_val_opt(table, col, "=", val)
    }

    #[inline]
    pub fn where_sql(self) -> WhereSql<Self> {
        WhereSql::new(self)
    }

    #[inline]
    pub fn group_by(self, table: &str, col: &str) -> Self {
        self.group_by_slice(table, std::slice::from_ref(&col))
    }

    pub fn group_by_slice(mut self, table: &str, cols: &[&str]) -> Self {
        let sql = &mut self.sql;
        sql.extend_from_slice(b" group by ");
        for item in cols.iter() {
            if !table.is_empty() {
                sql.extend_from_slice(table.as_bytes());
                sql.push(b'.');
            }
            sql.extend_from_slice(item.as_bytes());
        }
        self
    }

    #[inline]
    pub fn order_by(self, table: &str, col: &str) -> Self {
        self.order_by_slice(table, std::slice::from_ref(&col))
    }

    pub fn order_by_slice(mut self, table: &str, cols: &[&str]) -> Self {
        let sql = &mut self.sql;
        sql.extend_from_slice(b" order by ");
        for item in cols.iter() {
            if !table.is_empty() {
                sql.extend_from_slice(table.as_bytes());
                sql.push(b'.');
            }
            sql.extend_from_slice(item.as_bytes());
        }
        self
    }

    pub fn limits(mut self, offset: u32, count: u32) -> Self {
        if count > 0 {
            Self::set_limits(&mut self.sql, offset, count);
        }
        self
    }

    pub fn build(self) -> (String, Vec<Value>) {
        let sql = unsafe { String::from_utf8_unchecked(self.sql) };
        if self.params.is_empty() {
            log_sql(&sql);
        } else {
            log_sql_params(&sql, &self.params);
        }
        (sql, self.params)
    }

    pub fn build_with_page(self, pgae_index: u32, page_size: u32, total: Option<u32>) ->
            Result<(String, String, Vec<Value>), GenSqlError> {

        let mut sql = unsafe { String::from_utf8_unchecked(self.sql) };
        let mut total_sql = String::new();

        if pgae_index > 0 && page_size > 0 {
            let from_pos = match sql.find(" from ") {
                Some(pos) => pos,
                None => return Err(GenSqlError::UnsearchFrom),
            };

            if total.is_none() {
                total_sql.push_str("select count(*)");
                total_sql.push_str(&sql[from_pos..]);
            }

            unsafe {
                Self::set_limits(sql.as_mut_vec(),
                    (pgae_index - 1) * page_size, page_size);
            }
        }


        if self.params.is_empty() {
            if !total_sql.is_empty() {
                log_sql(&total_sql);
            }
            log_sql(&sql);
        } else {
            if !total_sql.is_empty() {
                log_sql(&total_sql);
            }
            log_sql_params(&sql, &self.params);
        }

        Ok((total_sql, sql, self.params))
    }

    fn pri_join(mut self, join_type: &str, table: &str, alias: &str) -> Self {
        let sql = &mut self.sql;
        sql.extend_from_slice(join_type.as_bytes());
        sql.extend_from_slice(table.as_bytes());
        if !alias.is_empty() {
            sql.push(b' ');
            sql.extend_from_slice(alias.as_bytes());
        }
        sql.push(b' ');
        sql.extend_from_slice(ON.as_bytes());
        self
    }

    fn set_limits(sql: &mut Vec<u8>, offset: u32, count: u32) {
        debug_assert!(count > 0);
        let mut num_buf = Buffer::new();
        sql.extend_from_slice(b" limit ");
        sql.extend_from_slice(num_buf.format(offset).as_bytes());
        sql.extend_from_slice(b", ");
        sql.extend_from_slice(num_buf.format(count).as_bytes());
    }
}

impl InsertSql {
    pub fn new(table_name: &str) -> Self {
        let mut sql = Vec::new();
        sql.extend_from_slice(b"insert into ");
        sql.extend_from_slice(table_name.as_bytes());
        sql.extend_from_slice(b" (");
        Self {
            sql,
            params: Vec::new(),
        }
    }

    pub fn value<T: ToValue>(mut self, col: &str, val: &T) -> Self {
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b", ");
        self.params.push(val.to_value());
        self
    }

    #[inline]
    pub fn value_opt<T: ToValue>(self, col: &str, val: &Option<T>) -> Self {
        match val {
            Some(val) => self.value(col, val),
            None => self,
        }
    }

    #[inline]
    pub fn value_str<T: ToValue + AsRef<str>>(self, col: &str, val: &Option<T>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.value(col, val)
            }
        };
        self
    }

    pub fn build(mut self) -> (String, Vec<Value>) {
        debug_assert!(!self.params.is_empty());
        debug_assert!(self.sql.ends_with(b", "));

        let sql = &mut self.sql;
        sql.truncate(sql.len() - 2);

        sql.extend_from_slice(b") values (?");
        for _ in 1..self.params.len() {
            sql.extend_from_slice(b", ?");
        }
        sql.push(b')');

        let sql = unsafe { String::from_utf8_unchecked(self.sql) };
        log_sql_params(&sql, &self.params);

        (sql, self.params)
    }
}

impl GeneratorSql for UpdateSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.params
    }
}

impl UpdateSql {
    #[inline]
    pub fn new(table_name: &str) -> Self {
        let mut sql = Vec::new();
        sql.extend_from_slice(b"update ");
        sql.extend_from_slice(table_name.as_bytes());
        sql.extend_from_slice(b" set ");

        Self {
            sql,
            params: Vec::new(),
        }
    }

    #[inline]
    pub fn set<T: ToValue>(mut self, col: &str, val: &T) -> Self {
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b" = ?, ");
        self.params.push(val.to_value());
        self
    }

    #[inline]
    pub fn set_opt<T: ToValue>(self, col: &str, val: &Option<T>) -> Self {
        match val {
            Some(val) => self.set(col, val),
            None => self,
        }
    }

    #[inline]
    pub fn set_str<T: ToValue + AsRef<str>>(self, col: &str, val: &Option<T>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.set(col, val);
            }
        }

        self
    }

    #[inline]
    pub fn set_sql(mut self, sql: &str) -> Self {
        self.sql.extend_from_slice(sql.as_bytes());
        self.sql.extend_from_slice(b", ");
        self
    }

    #[inline]
    pub fn set_sql_if(self, cond: bool, sql: &str) -> Self {
        if cond { self.set_sql(sql) } else { self }
    }

    #[inline]
    pub fn where_sql(mut self) -> WhereSql<Self> {
        debug_assert!(self.sql.ends_with(b", "));
        self.sql.truncate(self.sql.len() - 2);
        WhereSql::new(self)
    }

    pub fn build(mut self) -> (String, Vec<Value>) {
        debug_assert!(!self.params.is_empty());

        if self.sql.ends_with(b", ") {
            self.sql.truncate(self.sql.len() - 2);
        }

        let sql = unsafe { String::from_utf8_unchecked(self.sql) };
        log_sql_params(&sql, &self.params);

        (sql, self.params)
    }
}

impl GeneratorSql for DeleteSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.params
    }
}

impl DeleteSql {
    pub fn new(table_name: &str) -> WhereSql<Self> {
        let mut sql = Vec::new();
        sql.extend_from_slice(b"delete from ");
        sql.extend_from_slice(table_name.as_bytes());

        WhereSql::new(Self {
            sql,
            params: Vec::new(),
        })
    }

    pub fn build(self) -> (String, Vec<Value>) {
        let sql = unsafe { String::from_utf8_unchecked(self.sql) };
        log_sql_params(&sql, &self.params);

        (sql, self.params)
    }
}

impl <T: GeneratorSql>GeneratorSql for TrimSql<T> {
    fn sql(&mut self) -> &mut Vec<u8> {
        self.parent.sql()
    }

    fn params(&mut self) -> &mut Vec<Value> {
        self.parent.params()
    }
}

impl <T: GeneratorSql> TrimSql<T> {
    pub fn new(mut parent: T, prefix: &str, suffix: &str,
            prefix_overrides: &[&str], suffix_overrides: &[&str]) -> Self {

        let psql = parent.sql();

        if !prefix.is_empty() {
            psql.push(b' ');
            psql.extend_from_slice(prefix.as_bytes());
        }

        let prefix_overrides = prefix_overrides.iter()
            .map(|s| CompactString::new(s))
            .collect();
        let suffix_overrides = suffix_overrides.iter()
            .map(|s| CompactString::new(s))
            .collect();

        Self {
            parent,
            prefix: CompactString::new(prefix),
            suffix: CompactString::new(suffix),
            prefix_overrides,
            suffix_overrides,
        }
    }

    pub fn end_trim(mut self) -> T {
        let psql = self.parent.sql();

        // 语句为空
        if !self.prefix.is_empty() && psql.ends_with(self.prefix.as_bytes()) {
            psql.truncate(psql.len() - self.prefix.len() - 1);
        } else {
            // 去除后缀多余的字符
            for item in self.suffix_overrides {
                if !item.is_empty() && psql.ends_with(item.as_bytes()) {
                    psql.truncate(psql.len() - item.len());
                    break;
                }
            }
            // 添加后缀
            psql.extend_from_slice(self.suffix.as_bytes());
        }

        self.parent
    }

    pub fn add_sql(mut self, sql: &str) -> Self {
        self.inner_add_sql(sql);
        self
    }

    pub fn add_value<V: ToValue>(mut self, sql: &str, val: &V) -> Self {
        self.inner_add_sql(sql);
        self.parent.params().push(val.to_value());
        self
    }

    pub fn add_slice<V: ToValue>(mut self, sql: &str, vals: &[V]) -> Self {
        if !vals.is_empty() {
            self.inner_add_sql(sql);

            let params = self.parent.params();
            for item in vals.iter() {
                params.push(item.to_value());
            }
        }
        self
    }

    pub fn add_values(mut self, sql: &str, vals: Vec<Value>) -> Self {
        if !vals.is_empty() {
            self.inner_add_sql(sql);

            let params = self.parent.params();
            for item in vals.into_iter() {
                params.push(item);
            }
        }
        self
    }

    pub fn if_one<V: ToValue>(mut self, cond: bool, sql: &str, val: &V) -> Self {
        if cond {
            self.inner_add_sql(sql);
            self.parent.params().push(val.to_value());
        }
        self
    }

    pub fn if_slice<V: ToValue>(self, cond: bool, sql: &str, vals: &[V]) -> Self {
        if cond {
            self.add_slice(sql, vals)
        } else {
            self
        }
    }

    pub fn if_values(self, cond: bool, sql: &str, vals: Vec<Value>) -> Self {
        if cond {
            self.add_values(sql, vals)
        } else {
            self
        }
    }

    pub fn if_opt<V: ToValue>(mut self, sql: &str, val: &Option<V>) -> Self {
        if let Some(val) = val {
            self.inner_add_sql(sql);
            self.parent.params().push(val.to_value());
        }
        self
    }

    pub fn for_each<U, F: Fn(&U) -> Value>(mut self, open: &str, close: &str,
        sep: &str, list: &[U], f: F) -> Self
    {
        if !list.is_empty() {
            let psql = self.parent.sql();

            if !open.is_empty() {
                psql.push(b' ');
                psql.extend_from_slice(open.as_bytes());
            }

            for _ in 0..list.len() {
                psql.push(b'?');
                if !sep.is_empty() {
                    psql.extend_from_slice(sep.as_bytes())
                }
            }
            if !sep.is_empty() {
                psql.truncate(psql.len() - sep.len());
            }

            if !close.is_empty() {
                psql.extend_from_slice(close.as_bytes());
            }

            let params = self.parent.params();

            for item in list.iter() {
                params.push(f(item));
            }
        }
        self
    }

    pub fn trim(self, prefix: &str, suffix: &str, prefix_overrides: &[&str],
            suffix_overrides: &[&str]) -> TrimSql<Self> {
        TrimSql::new(self, prefix, suffix, prefix_overrides, suffix_overrides)
    }

    fn inner_add_sql(&mut self, sql: &str) {
        let mut sql = sql.as_bytes();
        let psql = self.parent.sql();

        if !self.prefix.is_empty() && psql.ends_with(self.prefix.as_bytes()) {
            for item in self.prefix_overrides.iter() {
                if sql.starts_with(item.as_bytes()) {
                    sql = &sql[item.len()..];
                    break;
                }
            }
        } else {
            psql.push(b' ');
        }

        psql.extend_from_slice(sql);
    }

}

impl <T: GeneratorSql>GeneratorSql for WhereSql<T> {
    fn sql(&mut self) -> &mut Vec<u8> {
        self.0.sql()
    }

    fn params(&mut self) -> &mut Vec<Value> {
        self.0.params()
    }
}

impl <T: GeneratorSql> WhereSql<T> {
    #[inline]
    pub fn new(parent: T) -> Self {
        Self(TrimSql::new(parent, "where ", "", &[AND, OR], &[]))
    }

    #[inline]
    pub fn end_where(self) -> T {
        self.0.end_trim()
    }

    pub fn trim(self, prefix: &str, suffix: &str, prefix_overrides: &[&str],
            suffix_overrides: &[&str]) -> TrimSql<Self> {
        TrimSql::new(self, prefix, suffix, prefix_overrides, suffix_overrides)
    }

    #[inline]
    pub fn add_sql(mut self, sql: &str) -> Self {
        self.0 = self.0.add_sql(sql);
        self
    }

    #[inline]
    pub fn add_value<V: ToValue>(mut self, sql: &str, val: &V) -> Self {
        self.0 = self.0.add_value(sql, val);
        self
    }

    #[inline]
    pub fn add_slice<V: ToValue>(mut self, sql: &str, vals: &[V]) -> Self {
        self.0 = self.0.add_slice(sql, vals);
        self
    }

    #[inline]
    pub fn add_values(mut self, sql: &str, vals: Vec<Value>) -> Self {
        self.0 = self.0.add_values(sql, vals);
        self
    }

    #[inline]
    pub fn if_one<V: ToValue>(mut self, cond: bool, sql: &str, val: &V) -> Self {
        self.0 = self.0.if_one(cond, sql, val);
        self
    }

    #[inline]
    pub fn if_slice<V: ToValue>(mut self, cond: bool, sql: &str, vals: &[V]) -> Self {
        self.0 = self.0.if_slice(cond, sql, vals);
        self
    }

    #[inline]
    pub fn if_values(mut self, cond: bool, sql: &str, vals: Vec<Value>) -> Self {
        self.0 = self.0.if_values(cond, sql, vals);
        self
    }

    #[inline]
    pub fn if_opt<V: ToValue>(mut self, sql: &str, val: &Option<V>) -> Self {
        self.0 = self.0.if_opt(sql, val);
        self
    }

    #[inline]
    pub fn for_each<U, F: Fn(&U) -> Value>(mut self, open: &str, close: &str,
        sep: &str, list: &[U], f: F) -> Self
    {
        self.0 = self.0.for_each(open, close, sep, list, f);
        self
    }

    #[inline]
    pub fn eq<V: ToValue>(self, table: &str, col: &str, val: &V) -> Self {
        self.expr(AND, table, col, "=", val)
    }

    #[inline]
    pub fn eq_if<V: ToValue>(self, pred: bool, table: &str, col: &str, val: &V) -> Self {
        if pred {
            self.eq(table, col, val)
        } else {
            self
        }
    }

    #[inline]
    pub fn eq_opt<V: ToValue>(self, table: &str, col: &str, val: &Option<V>) -> Self {
        match val {
            Some(val) => self.eq(table, col, val),
            None => self,
        }
    }

    #[inline]
    pub fn eq_str<V: ToValue + AsRef<str>>(self, table: &str, col: &str, val: &Option<V>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.eq(table, col, val);
            }
        }
        self
    }

    #[inline]
    pub fn like(self, table: &str, col: &str, val: &str) -> Self {
        self.inner_like(AND, table, col, LikeType::Full, val)
    }

    pub fn like_opt<V: AsRef<str>>(self, table: &str, col: &str, val: &Option<V>) -> Self {
        match val {
            Some(val) => self.like(table, col, val.as_ref()),
            None => self,
        }
    }

    #[inline]
    pub fn like_right(self, table: &str, col: &str, val: &str) -> Self {
        self.inner_like(AND, table, col, LikeType::Right, val)
    }

    pub fn like_right_opt<V: AsRef<str>>(self, table: &str, col: &str, val: &Option<V>) -> Self {
        match val {
            Some(val) => self.like_right(table, col, val.as_ref()),
            None => self,
        }
    }

    pub fn between<V: ToValue>(self, table: &str, col: &str, v1: &V, v2: &V) -> Self {
        let sql = format_compact!("and {}{} between ? and ?", _ta(table), col);
        self.add_slice(&sql, &vec![v1, v2])
    }

    pub fn between_opt<V: ToValue>(self, table: &str, col: &str, v1: &Option<V>, v2: &Option<V>) -> Self {
        if let (Some(v1), Some(v2)) = (v1, v2) {
            self.between(table, col, v1, v2)
        } else {
            self
        }
    }

    #[inline]
    pub fn in_<V: ToValue>(self, table: &str, col: &str, vals: &[V]) -> Self {
        if !vals.is_empty() {
            let sql = format_compact!("and {}{} in", _ta(table), col);
            self.add_sql(&sql)
                .for_each("(", ")", ", ", vals, V::to_value)
        } else {
            self
        }
    }

    #[inline]
    pub fn in_opt<V: ToValue>(self, table: &str, col: &str, vals: &Option<Vec<V>>) -> Self {
        match vals {
            Some(vals) => self.in_(table, col, vals),
            None => self,
        }
    }

    fn expr<V: ToValue>(self, opera: &str, table: &str, col: &str, expr: &str, val: &V) -> Self {
        let sql = format_compact!("{}{}{} {} ?", opera, _ta(table), col, expr);
        self.add_value(&sql, val)
    }

    fn inner_like(self, opera: &str, table: &str, col: &str, like_type: LikeType, val: &str) -> Self {
        if val.is_empty() { return self; }

        let mut s = String::with_capacity(val.len() + 2);
        if like_type != LikeType::Right {
            s.push('%');
        }
        s.push_str(val);
        if like_type != LikeType::Left {
            s.push('%');
        }

        self.expr(opera, table, col, "like", &s)
    }

}

#[inline]
fn _ta(table: &str) -> CompactString {
    let mut s = CompactString::new(table);
    if !table.is_empty() {
        s.push('.');
    }
    s
}
