//! FastStr implement
// author: kiven
// slince 2023-09-20

use std::borrow::Cow;

use super::{ToValue, Value};
use itoa::Buffer;
use thiserror::Error;
use utils::make_col_expr;

const AND: &str = " and ";
const OR: &str = " or ";
const ON: &str = " on ";

#[derive(Error, Debug)]
pub enum GenSqlError {
    #[error("unsearch ` from ` in sql")]
    UnsearchFrom,
}

pub enum InsertValue {
    Sql(&'static str),
    SqlString(String),
    Value(Value),
}

pub trait GeneratorSql {
    fn sql(&mut self) -> &mut Vec<u8>;
    fn params(&mut self) -> &mut Vec<Value>;
    fn sql_params(&mut self) -> (&mut Vec<u8>, &mut Vec<Value>);
}

#[derive(Default)]
struct BaseSql {
    pub(crate) sql: Vec<u8>,
    pub(crate) params: Vec<Value>,
}

pub struct FieldInfo<'a> {
    table: &'a str,
    column: &'a str,
    alias: &'a str,
}

pub struct SelectSql(BaseSql);

pub struct InsertSql {
    sql: Vec<u8>,
    params: Vec<InsertValue>,
}

pub struct UpdateSql(BaseSql);

pub struct DeleteSql(BaseSql);

pub struct BatchInsertSql {
    sql: Vec<u8>,
    params: Vec<Vec<InsertValue>>,
    #[cfg(debug_assertions)]
    count: usize,
}

pub struct BatchDeleteSql(BaseSql);

pub struct JoinSql {
    parent: SelectSql,
    table: String,
}

pub struct TrimSql<T: GeneratorSql> {
    parent: T,
    prefix: String,
    suffix: String,
    prefix_overrides: Vec<String>,
    suffix_overrides: Vec<String>,
}

pub struct WhereSql<T: GeneratorSql>(TrimSql<T>);

#[derive(PartialEq)]
enum LikeType {
    Full,
    Left,
    Right,
}

#[inline]
pub fn db_log_sql(sql: &str) {
    log::debug!("[SQL]: {}", sql);
}

#[inline]
pub fn db_log_params(params: &[Value]) {
    if !params.is_empty() {
        log::debug!("[SQL-PARAMS]: {:?}", params);
    }
}

#[inline]
pub fn db_log_sql_params(sql: &str, params: &[Value]) {
    db_log_sql(sql);
    db_log_params(params);
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

    Ok(format!(
        "select count(*){}",
        &select_sql[from_pos..limit_pos]
    ))
}

impl GeneratorSql for BaseSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.params
    }

    fn sql_params(&mut self) -> (&mut Vec<u8>, &mut Vec<Value>) {
        (&mut self.sql, &mut self.params)
    }

}

impl GeneratorSql for SelectSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.0.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.0.params
    }

    fn sql_params(&mut self) -> (&mut Vec<u8>, &mut Vec<Value>) {
        (&mut self.0.sql, &mut self.0.params)
    }

}

impl GeneratorSql for DeleteSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.0.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.0.params
    }

    fn sql_params(&mut self) -> (&mut Vec<u8>, &mut Vec<Value>) {
        (&mut self.0.sql, &mut self.0.params)
    }

}

impl GeneratorSql for UpdateSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.0.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.0.params
    }

    fn sql_params(&mut self) -> (&mut Vec<u8>, &mut Vec<Value>) {
        (&mut self.0.sql, &mut self.0.params)
    }

}

impl InsertSql {
    pub fn new(table: &str) -> Self {
        debug_assert!(!table.is_empty());
        let mut result = Self {
            sql: Vec::new(),
            params: Vec::new(),
        };
        let sql = &mut result.sql;
        sql.extend_from_slice(b"insert into ");
        sql.extend_from_slice(table.as_bytes());
        sql.extend_from_slice(b" (");
        result
    }

    pub fn value<T: ToValue>(mut self, col: &str, val: T) -> Self {
        debug_assert!(!col.is_empty());
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b", ");
        self.params.push(InsertValue::Value(val.to_value()));
        self
    }

    #[inline]
    pub fn value_opt<T: ToValue>(self, col: &str, val: Option<T>) -> Self {
        match val {
            Some(val) => self.value(col, val),
            None => self,
        }
    }

    #[inline]
    pub fn value_str<T: AsRef<str>>(self, col: &str, val: Option<T>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.value(col, val.as_ref());
            }
        };
        self
    }

    pub fn value_sql(mut self, col: &str, raw: Cow<'static, str>) -> Self {
        debug_assert!(!col.is_empty() && !raw.is_empty());
        let v = match raw {
            Cow::Borrowed(v) => InsertValue::Sql(v),
            Cow::Owned(v) => InsertValue::SqlString(v),
        };
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b", ");
        self.params.push(v);
        self
    }

    pub fn build(self) -> (String, Vec<Value>) {
        debug_assert!(!self.params.is_empty());
        debug_assert!(self.sql.ends_with(b", "));

        let mut sql = self.sql;
        let mut params = Vec::new();

        sql.truncate(sql.len() - 2);
        sql.extend_from_slice(b") values (");
        for param in self.params.into_iter() {
            match param {
                InsertValue::Sql(s) =>
                    sql.extend_from_slice(s.as_bytes()),
                InsertValue::SqlString(s) =>
                    sql.extend_from_slice(s.as_bytes()),
                InsertValue::Value(v) => {
                    sql.push(b'?');
                    params.push(v);
                }
            }
            sql.extend_from_slice(b", ")
        }
        sql.truncate(sql.len() - 2);
        sql.push(b')');

        let sql = unsafe { String::from_utf8_unchecked(sql) };
        db_log_sql_params(&sql, &params);
        (sql, params)
    }
}

impl SelectSql {
    #[inline]
    pub fn new() -> Self {
        let mut result = SelectSql(BaseSql::default());
        result.0.sql.extend_from_slice(b"select ");
        result
    }

    #[inline]
    pub fn select_all_with_table(self, table: &str) -> Self {
        self.select_as(table, "*", "")
    }

    #[inline]
    pub fn select(self, table: &str, col: &str) -> Self {
        self.select_as(table, col, "")
    }

    pub fn select_as(mut self, table: &str, col: &str, alias: &str) -> Self {
        debug_assert!(!col.is_empty());
        utils::push_col_alias(&mut self.0.sql, table, col, alias);
        self
    }

    pub fn select_columns(mut self, table: &str, cols: &[&str]) -> Self {
        debug_assert!(!cols.is_empty() && cols.iter().find(|v| v.is_empty()).is_none());
        let sql = &mut self.0.sql;
        cols.iter().for_each(|col| {
            utils::push_col_alias(sql, table, col, "");
        });
        self
    }

    pub fn select_with_iter<'a, I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = FieldInfo<'a>>,
    {
        let sql = &mut self.0.sql;
        for item in iter.into_iter() {
            debug_assert!(!item.column.is_empty());
            utils::push_col_alias(sql, item.table, item.column, item.alias);
        }
        self
    }

    #[inline]
    pub fn from(self, table: &str) -> Self {
        self.from_as(table, "")
    }

    pub fn from_as(mut self, table: &str, alias: &str) -> Self {
        debug_assert!(!table.is_empty());
        let sql = &mut self.0.sql;

        // 删除select留下的多余分隔符
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

    pub fn join<F>(self, table: &str, alias: &str, f: F) -> Self
    where
        F: FnOnce(JoinSql) -> JoinSql,
    {
        f(JoinSql::new(self, " join ", table, alias)).end_join()
    }

    pub fn left_join<F>(self, table: &str, alias: &str, f: F) -> Self
    where
        F: FnOnce(JoinSql) -> JoinSql,
    {
        f(JoinSql::new(self, " left join ", table, alias)).end_join()
    }

    pub fn right_join<F>(self, table: &str, alias: &str, f: F) -> Self
    where
        F: FnOnce(JoinSql) -> JoinSql,
    {
        f(JoinSql::new(self, " right join ", table, alias)).end_join()
    }

    pub fn full_join<F>(self, table: &str, alias: &str, f: F) -> Self
    where
        F: FnOnce(JoinSql) -> JoinSql,
    {
        f(JoinSql::new(self, " full join ", table, alias)).end_join()
    }

    #[inline]
    pub fn where_sql<F>(self, f: F) -> Self
    where
        F: FnOnce(WhereSql<Self>) -> WhereSql<Self>,
    {
        f(WhereSql::with_parent(self)).end_where()
    }

    #[inline]
    pub fn group_by_(self, table: &str, col: &str) -> Self {
        self.group_by_columns(table, std::slice::from_ref(&col))
    }

    pub fn group_by_columns(mut self, table: &str, cols: &[&str]) -> Self {
        debug_assert!(!cols.is_empty() && cols.iter().find(|v| v.is_empty()).is_none());
        let sql = &mut self.0.sql;
        sql.extend_from_slice(b" group by ");
        for item in cols.iter() {
            utils::push_col_alias(sql, table, item, "");
        }
        sql.truncate(sql.len() - 2);
        self
    }

    pub fn having(mut self, expr: &str) -> Self {
        let sql = &mut self.0.sql;
        sql.extend_from_slice(b" having ");
        sql.extend_from_slice(expr.as_bytes());
        self
    }

    #[inline]
    pub fn order_by(self, table: &str, col: &str) -> Self {
        self.order_by_columns(table, std::slice::from_ref(&col))
    }

    pub fn order_by_columns(mut self, table: &str, cols: &[&str]) -> Self {
        utils::order_by_iter(&mut self.0.sql, &mut cols.iter().map(|v| (table, *v, false)));
        self
    }

    pub fn order_by_with_iter<'a, I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a str, bool)>,
    {
        utils::order_by_iter(&mut self.0.sql, &mut iter.into_iter());
        self
    }

    pub fn limits(mut self, offset: u32, count: u32) -> Self {
        if count > 0 {
            Self::set_limits(&mut self.0.sql, offset, count);
        }
        self
    }

    #[inline]
    pub fn add_sql(mut self, sql: &str) -> Self {
        self.0.sql.extend_from_slice(sql.as_bytes());
        self
    }

    #[inline]
    pub fn add_sql_if(self, cond: bool, sql: &str) -> Self {
        if cond { self.add_sql(sql) } else { self }
    }

    #[inline]
    pub fn add_sql_val_if<T: ToValue>(self, cond: bool, sql: &str, val: T) -> Self {
        if cond { self.add_sql_val(sql, val) } else { self }
    }

    #[inline]
    pub fn add_sql_val<T: ToValue>(mut self, sql: &str, val: T) -> Self {
        self.0.params.push(val.to_value());
        self.add_sql(sql)
    }

    #[inline]
    pub fn add_sql_vals_if<I: IntoIterator<Item = V>, V: ToValue>(
        self, cond: bool, sql: &str, iter: I) -> Self
    {
        if cond { self.add_sql_vals(sql, iter) } else { self }
    }

    #[inline]
    pub fn add_sql_vals<I: IntoIterator<Item = V>, V: ToValue>(mut self, sql: &str, iter: I) -> Self {
        self.0.params.extend(iter.into_iter().map(|v| v.to_value()));
        self.add_sql(sql)
    }

    pub fn build(self) -> (String, Vec<Value>) {
        let sql = unsafe { String::from_utf8_unchecked(self.0.sql) };
        db_log_sql_params(&sql, &self.0.params);
        (sql, self.0.params)
    }

    pub fn build_with_page(
        self,
        pgae_index: u32,
        page_size: u32,
        total: Option<u32>,
    ) -> (String, String, Vec<Value>) {
        let mut sql = unsafe { String::from_utf8_unchecked(self.0.sql) };
        let mut total_sql = String::new();

        if pgae_index > 0 && page_size > 0 {
            if total.is_none() {
                if let Some(from_pos) = sql.find(" from ") {
                    total_sql.push_str("select count(*)");
                    total_sql.push_str(&sql[from_pos..]);
                }
                db_log_sql(&total_sql);
            };

            unsafe {
                Self::set_limits(sql.as_mut_vec(), (pgae_index - 1) * page_size, page_size);
            }
        }
        db_log_sql_params(&sql, &self.0.params);

        (total_sql, sql, self.0.params)
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

impl UpdateSql {
    pub fn new(table: &str) -> Self {
        let mut result = Self(BaseSql::default());
        let sql = &mut result.0.sql;
        sql.extend_from_slice(b"update ");
        sql.extend_from_slice(table.as_bytes());
        sql.extend_from_slice(b" set ");
        result
    }

    pub fn set<T: ToValue>(mut self, col: &str, val: T) -> Self {
        self.0.sql.extend_from_slice(col.as_bytes());
        self.0.sql.extend_from_slice(b" = ?, ");
        self.0.params.push(val.to_value());
        self
    }

    #[inline]
    pub fn set_opt<T: ToValue>(self, col: &str, val: Option<T>) -> Self {
        match val {
            Some(val) => self.set(col, val),
            None => self,
        }
    }

    #[inline]
    pub fn set_str<T: AsRef<str>>(self, col: &str, val: Option<T>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.set(col, val.as_ref());
            }
        }
        self
    }

    pub fn set_sql(mut self, col: &str, raw: &str) -> Self {
        let sql = &mut self.0.sql;
        sql.extend_from_slice(col.as_bytes());
        sql.extend_from_slice(b" = ");
        sql.extend_from_slice(raw.as_bytes());
        sql.extend_from_slice(b", ");
        self
    }

    #[inline]
    pub fn where_sql<F>(mut self, f: F) -> Self
    where
        F: FnOnce(WhereSql<Self>) -> WhereSql<Self>,
    {
        debug_assert!(self.0.sql.ends_with(b", "));
        self.0.sql.truncate(self.0.sql.len() - 2);
        f(WhereSql::with_parent(self)).end_where()
    }

    pub fn build(self) -> (String, Vec<Value>) {
        debug_assert!(!self.0.params.is_empty());

        let mut sql = self.0.sql;
        if sql.ends_with(b", ") {
            sql.truncate(sql.len() - 2);
        }

        let sql = unsafe { String::from_utf8_unchecked(sql) };

        db_log_sql_params(&sql, &self.0.params);
        (sql, self.0.params)
    }
}

impl DeleteSql {
    pub fn new<F>(table: &str, f: F) -> Self
    where
        F: FnOnce(WhereSql<Self>) -> WhereSql<Self>,
    {
        debug_assert!(!table.is_empty());
        let mut sql = Vec::new();
        sql.extend_from_slice(b"delete from ");
        sql.extend_from_slice(table.as_bytes());
        let w = WhereSql::with_parent(Self(BaseSql {
            sql,
            params: Vec::new(),
        }));
        f(w).end_where()
    }

    pub fn build(self) -> (String, Vec<Value>) {
        let sql = unsafe { String::from_utf8_unchecked(self.0.sql) };
        db_log_sql_params(&sql, &self.0.params);
        (sql, self.0.params)
    }
}

impl BatchInsertSql {
    pub fn new(table: &str, cols: &[&str]) -> Self {
        #[cfg(debug_assertions)]
        debug_assert!(!table.is_empty());
        #[cfg(debug_assertions)]
        let mut result = Self {
            sql: Vec::new(),
            params: Vec::new(),
            count: cols.len(),
        };
        #[cfg(not(debug_assertions))]
        let mut result = Self {
            sql: Vec::new(),
            params: Vec::new(),
        };
        let sql = &mut result.sql;
        sql.extend_from_slice(b"insert into ");
        sql.extend_from_slice(table.as_bytes());
        sql.push(b' ');
        sql.push(b'(');
        for col in cols {
            debug_assert!(!col.is_empty());
            sql.extend_from_slice(col.as_bytes());
            sql.push(b',');
            sql.push(b' ');
        }
        sql.truncate(sql.len() - 2);
        sql.extend_from_slice(b") values ");

        result
    }

    pub fn value(mut self, val: Vec<InsertValue>) -> Self {
        #[cfg(debug_assertions)]
        debug_assert!(self.count == val.len());
        self.params.push(val);
        self
    }

    pub fn values<I>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = Vec<InsertValue>>,
    {
        for item in i.into_iter() {
            #[cfg(debug_assertions)]
            debug_assert!(self.count == item.len());
            self.params.push(item);
        }
        self
    }

    pub fn build(self) -> (String, Vec<Value>) {
        debug_assert!(!self.params.is_empty());

        let mut sql = self.sql;
        let mut params = Vec::new();

        for ps in self.params.into_iter() {
            sql.push(b'(');
            for param in ps.into_iter() {
                match param {
                    InsertValue::Sql(s) =>
                        sql.extend_from_slice(s.as_bytes()),
                    InsertValue::SqlString(s) =>
                        sql.extend_from_slice(s.as_bytes()),
                    InsertValue::Value(v) => {
                        sql.push(b'?');
                        params.push(v);
                    }
                }
                sql.extend_from_slice(b", ")
            }
            sql.truncate(sql.len() - 2);
            sql.extend_from_slice(b"), ");
        }
        sql.truncate(sql.len() - 2);

        let sql = unsafe { String::from_utf8_unchecked(sql) };
        db_log_sql_params(&sql, &params);
        (sql, params)
    }
}

impl BatchDeleteSql {
    pub fn new<V, I>(table: &str, col: &str, i: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        debug_assert!(!table.is_empty());
        debug_assert!(!col.is_empty());

        let (mut sql, mut params) = (Vec::new(), Vec::new());

        sql.extend_from_slice(b"delete from ");
        sql.extend_from_slice(table.as_bytes());
        sql.extend_from_slice(b" where ");
        sql.extend_from_slice(col.as_bytes());
        sql.extend_from_slice(b" in (");

        for val in i.into_iter() {
            sql.extend_from_slice(b"?, ");
            params.push(val.to_value());
        }
        sql.truncate(sql.len() - 2);
        sql.push(b')');

        Self(BaseSql{sql, params})
    }

    pub fn build(self) -> (String, Vec<Value>) {
        let sql = unsafe { String::from_utf8_unchecked(self.0.sql) };
        db_log_sql_params(&sql, &self.0.params);
        (sql, self.0.params)
    }
}

impl JoinSql {
    pub(crate) fn new(mut parent: SelectSql, join_type: &str, table: &str, alias: &str) -> Self {
        debug_assert!(!table.is_empty());
        let sql = &mut parent.0.sql;

        sql.extend_from_slice(join_type.as_bytes());
        sql.extend_from_slice(table.as_bytes());

        if !alias.is_empty() {
            sql.push(b' ');
            sql.extend_from_slice(alias.as_bytes());
        }

        sql.extend_from_slice(ON.as_bytes());

        let table = String::from(if !alias.is_empty() { alias } else { table });
        Self { parent, table }
    }

    #[inline]
    pub(crate) fn end_join(self) -> SelectSql {
        self.parent
    }

    #[inline]
    pub fn on(mut self, expr: &str) -> Self {
        Self::on_raw(&mut self.parent.0.sql, expr);
        self
    }

    pub fn on_eq(mut self, self_col: &str, other_table: &str, other_col: &str) -> Self {
        debug_assert!(!self_col.is_empty() && !other_table.is_empty() && !other_col.is_empty());
        utils::join_on(&mut self.parent.0.sql, &self.table, self_col, " = ", other_table, other_col);
        self
    }

    pub fn on_val<T: ToValue>(mut self, self_col: &str, expr: &str, val: T) -> Self {
        debug_assert!(!self_col.is_empty() && !expr.is_empty());
        self.parent.0.params.push(val.to_value());
        utils::join_on(&mut self.parent.0.sql, &self.table, self_col, expr, "", "?");
        self
    }

    #[inline]
    pub fn on_val_opt<T: ToValue>(self, col: &str, expr: &str, val: Option<T>) -> Self {
        match val {
            Some(val) => self.on_val(col, expr, val),
            None => self,
        }
    }

    #[inline]
    pub fn on_eq_val<T: ToValue>(self, col: &str, val: T) -> Self {
        self.on_val(col, "=", val)
    }

    #[inline]
    pub fn on_eq_val_opt<T: ToValue>(self, col: &str, val: Option<T>) -> Self {
        match val {
            Some(val) => self.on_val(col, "=", val),
            None => self,
        }
    }

    #[inline]
    pub fn on_eq_str<T: AsRef<str>>(self, col: &str, val: Option<T>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.on_val(col, "=", val.as_ref());
            }
        }
        self
    }

    fn on_raw(sql: &mut Vec<u8>, raw: &str) {
        debug_assert!(!raw.is_empty());
        utils::replace_prefix(sql, ON, AND);
        sql.extend_from_slice(raw.as_bytes());
    }

}

impl TrimSql<BaseSql> {
    #[inline]
    pub fn new(prefix: &str, suffix: &str, prefix_overrides: &[&str], suffix_overrides: &[&str]) -> Self {
        Self::with_parent(BaseSql::default(), prefix, suffix, prefix_overrides, suffix_overrides)
    }

    #[inline]
    pub fn to_sql_params(self) -> (String, Vec<Value>) {
        let b = self.end_trim();
        let sql = unsafe { String::from_utf8_unchecked(b.sql) };
        (sql, b.params)
    }
}

impl<T: GeneratorSql> TrimSql<T> {
    fn with_parent(
        mut parent: T,
        prefix: &str,
        suffix: &str,
        prefix_overrides: &[&str],
        suffix_overrides: &[&str],
    ) -> Self {
        debug_assert!(prefix_overrides.iter().find(|s| s.is_empty()).is_none());
        debug_assert!( suffix_overrides.iter().find(|s| s.is_empty()).is_none());

        let psql = parent.sql();

        if !prefix.is_empty() {
            psql.extend_from_slice(prefix.as_bytes());
        }

        let prefix_overrides = prefix_overrides.iter().map(|s| String::from(*s)).collect();
        let suffix_overrides = suffix_overrides.iter().map(|s| String::from(*s)).collect();

        Self {
            parent,
            prefix: String::from(prefix),
            suffix: String::from(suffix),
            prefix_overrides,
            suffix_overrides,
        }
    }

    /// 在语句结尾的时候，进行截断
    fn end_trim(mut self) -> T {
        let psql = self.parent.sql();

        // 语句为空
        if !self.prefix.is_empty() && psql.ends_with(self.prefix.as_bytes()) {
            psql.truncate(psql.len() - self.prefix.len());
        } else {
            // 去除后缀多余的字符
            for item in &self.suffix_overrides {
                if psql.ends_with(item.as_bytes()) {
                    psql.truncate(psql.len() - item.len());
                    break;
                }
            }
            // 添加后缀
            if !self.suffix.is_empty() {
                psql.extend_from_slice(self.suffix.as_bytes());
            }
        }

        self.parent
    }

    #[inline]
    pub fn add_sql(mut self, sql: &str) -> Self {
        self.inner_add_sql(sql);
        self
    }

    #[inline]
    pub fn add_sql_if(mut self, cond: bool, sql: &str) -> Self {
        if cond { self.inner_add_sql(sql); }
        self
    }

    #[inline]
    pub fn add_sql_if_cb<'a, F: Fn() -> Cow<'a, str>>(mut self, cond: bool, f: F) -> Self {
        if cond { self.inner_add_sql(&f()); }
        self
    }

    #[inline]
    pub fn add_value<V: ToValue>(mut self, sql: &str, val: V) -> Self {
        self.inner_add_sql(sql);
        self.parent.params().push(val.to_value());
        self
    }

    #[inline]
    pub fn add_value_if<V: ToValue>(self, cond: bool, sql: &str, val: V) -> Self {
        if cond {
            self.add_value(sql, val)
        } else {
            self
        }
    }

    #[inline]
    pub fn add_value_opt<V: ToValue>(self, sql: &str, val: Option<V>) -> Self {
        match val {
            Some(val) => self.add_value(sql, val),
            None => self,
        }
    }

    #[inline]
    pub fn add_value_str<V: AsRef<str>>(self, sql: &str, val: Option<V>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.add_value(sql, val.as_ref());
            }
        }
        self
    }

    #[inline]
    pub fn add_values<I, V>(mut self, sql: &str, iter: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        self.inner_add_values(sql, iter);
        self
    }

    #[inline]
    pub fn add_values_if<I, V>(self, cond: bool, sql: &str, iter: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        if cond {
            self.add_values(sql, iter)
        } else {
            self
        }
    }

    #[inline]
    pub fn for_each<I, V>(mut self, prefix: &str, open: &str, close: &str, sep: &str, iter: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        self.inner_for_each(prefix, open, close, sep, iter);
        self
    }

    #[inline]
    fn inner_add_sql(&mut self, sql: &str) {
        utils::trim_add_sql(self.parent.sql(), self.prefix.as_str(), &self.prefix_overrides, sql)
    }

    #[inline]
    fn inner_add_values<I, V>(&mut self, sql: &str, iter: I)
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        if utils::trim_add_values(self.parent.params(), iter) > 0 {
            self.inner_add_sql(sql);
        }
    }

    #[inline]
    fn inner_for_each<I, V>(&mut self, prefix: &str, open: &str, close: &str, sep: &str, iter: I)
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        utils::make_for_each(self.parent.sql_params(), &self.prefix,
            &self.prefix_overrides, prefix, open, close, sep,
            &mut iter.into_iter().map(|v| v.to_value()));
    }

}

impl WhereSql<BaseSql> {
    #[inline]
    pub fn new() -> Self {
        Self::with_parent(BaseSql::default())
    }

    #[inline]
    pub fn to_sql_params(self) -> (String, Vec<Value>) {
        self.0.to_sql_params()
    }
}

impl<T: GeneratorSql> WhereSql<T> {
    #[inline]
    fn with_parent(parent: T) -> Self {
        Self(TrimSql::with_parent(parent, " where ", "", &[AND, OR], &[]))
    }

    #[inline]
    fn end_where(self) -> T {
        self.0.end_trim()
    }

    #[inline]
    pub fn add_sql(mut self, sql: &str) -> Self {
        self.0.inner_add_sql(sql);
        self
    }

    #[inline]
    pub fn add_sql_if(mut self, cond: bool, sql: &str) -> Self {
        if cond { self.0.inner_add_sql(sql); }
        self
    }

    #[inline]
    pub fn add_sql_if_cb<'a, F: Fn() -> Cow<'a, str>>(mut self, cond: bool, f: F) -> Self {
        if cond { self.0.inner_add_sql(&f()); }
        self
    }

    /// 添加查询条件，使用原始的sql语句
    ///
    /// Arguments:
    ///
    /// * `sql`: 原始的sql，例如：` and name = ?`
    /// * `val`: 动态参数
    ///
    #[inline]
    pub fn add_value<V: ToValue>(mut self, sql: &str, val: V) -> Self {
        self.0.inner_add_sql(sql);
        self.0.parent.params().push(val.to_value());
        self
    }

    /// 当`pred`为`true`时添加查询条件，使用原始的sql语句
    ///
    /// Arguments:
    ///
    /// * `sql`: 原始的sql，例如：` and name = ?`
    /// * `val`: 动态参数
    ///
    #[inline]
    pub fn add_value_if<V: ToValue>(self, cond: bool, sql: &str, val: V) -> Self {
        if cond {
            self.add_value(sql, val)
        } else {
            self
        }
    }

    #[inline]
    pub fn add_value_if_cb<'a, V: ToValue, F: Fn() -> Cow<'a, str>>(
        self, cond: bool, val: V, f: F) -> Self
    {
        if cond { self.add_value(&f(), val) } else { self }
    }

    /// 当`val`不为`None`时添加查询条件，使用原始的sql语句
    ///
    /// Arguments:
    ///
    /// * `sql`: 原始的sql，例如：` and name = ?`
    /// * `val`: 动态参数
    ///
    #[inline]
    pub fn add_value_opt<V: ToValue>(self, sql: &str, val: Option<V>) -> Self {
        match val {
            Some(val) => self.add_value(sql, val),
            None => self,
        }
    }

    /// 当`val`不为`None`且不为空字符串时添加查询条件，使用原始的sql语句
    ///
    /// Arguments:
    ///
    /// * `sql`: 原始的sql，例如：` and name = ?`
    /// * `val`: 动态参数
    ///
    #[inline]
    pub fn add_value_str<V: AsRef<str>>(self, sql: &str, val: Option<V>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.add_value(sql, val.as_ref());
            }
        }
        self
    }

    /// 添加查询条件，使用原始的sql语句
    ///
    /// Arguments:
    ///
    /// * `sql`: 原始的sql，例如：` and name = ? and age = ?`
    /// * `vals`: 动态参数列表
    ///
    #[inline]
    pub fn add_values<I, V>(mut self, sql: &str, iter: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        self.0.inner_add_values(sql, iter);
        self
    }

    /// 当`pred`为`true`时添加查询条件，使用原始的sql语句
    ///
    /// Arguments:
    ///
    /// * `sql`: 原始的sql，例如：` and name = ? and age = ?`
    /// * `val`: 动态参数
    ///
    #[inline]
    pub fn add_values_if<I, V>(mut self, cond: bool, sql: &str, iter: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        if cond { self.0.inner_add_values(sql, iter); }
        self
    }

    /// 循环`vals`，生成查询条件，例如 `for_each(" and age in (", ")", ", ", vals)`，
    ///     将生成类似 ` and age in (?, ?, ?)``
    ///
    /// Arguments:
    ///
    /// * `open`: 开头插入的sql语句
    /// * `close`: 结尾追加的sql语句
    /// * `step`: 参数之间的分隔符
    /// * `val`: 列表参数
    ///
    #[inline]
    pub fn for_each<I, V>(mut self, prefix: &str, open: &str, close: &str, sep: &str, iter: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: ToValue,
    {
        self.0.inner_for_each(prefix, open, close, sep, iter);
        self
    }

    /// 添加比较条件，将字段值与传入参数`val`使用指定表达式`expr`进行比较。
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `expr`: 比较表达式，类似于“=”、“!=”、“">”、“<”、“">=”、“<=”等
    /// * `val`: 要比较的值，实现`ToValue`特征的泛型类型
    ///
    #[inline]
    pub fn expr<V: ToValue>(mut self, table: &str, col: &str, expr: &str, val: V) -> Self {
        utils::where_expr(self.0.parent.sql_params(), &self.0.prefix, AND, table, col, expr, val);
        self
    }

    /// 添加比较条件，将字段值与传入参数`val`使用指定表达式`expr`进行比较。
    ///
    /// Arguments:
    ///
    /// * `pred`: 判断是否添加条件的值
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `expr`: 比较表达式，类似于“=”、“!=”、“">”、“<”、“">=”、“<=”等
    /// * `val`: 要比较的值，实现`ToValue`特征的泛型类型
    ///
    #[inline]
    pub fn expr_if<V: ToValue>(self, pred: bool, table: &str, col: &str, expr: &str, val: V) -> Self {
        if pred {
            self.expr(table, col, expr, val)
        } else {
            self
        }
    }

    /// 添加比较条件，将字段值与传入参数`val`使用指定表达式`expr`进行比较。
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `expr`: 比较表达式，类似于“=”、“!=”、“">”、“<”、“">=”、“<=”等
    /// * `val`: 要比较的值，实现`ToValue`特征的泛型类型
    ///
    #[inline]
    pub fn expr_opt<V: ToValue>(self, table: &str, col: &str, expr: &str, val: Option<V>) -> Self {
        match val {
            Some(val) => self.expr(table, col, expr, val),
            None => self,
        }
    }

    /// 添加相等比较条件，将字段值与传入参数`val`进行比较。
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值，实现`ToValue`特征的泛型类型
    ///
    #[inline]
    pub fn eq<V: ToValue>(mut self, table: &str, col: &str, val: V) -> Self {
        utils::where_expr(self.0.parent.sql_params(), &self.0.prefix, AND, table, col, "=", val);
        self
    }

    /// 当`pred`为`true`时添加相等比较条件，否则忽略。
    ///
    /// Arguments:
    ///
    /// * `pred`: 判断是否添加条件的值
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值，实现`ToValue`特征的泛型类型
    ///
    #[inline]
    pub fn eq_if<V: ToValue>(self, pred: bool, table: &str, col: &str, val: V) -> Self {
        if pred {
            self.eq(table, col, val)
        } else {
            self
        }
    }

    /// 当`val`不为`None`时添加相等比较条件，否则忽略。
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值，实现`ToValue`特征的泛型类型
    ///
    #[inline]
    pub fn eq_opt<V: ToValue>(self, table: &str, col: &str, val: Option<V>) -> Self {
        match val {
            Some(val) => self.eq(table, col, val),
            None => self,
        }
    }

    /// 当`val`不为`None`且不为空字符串时添加相等比较条件，否则忽略。
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值
    ///
    #[inline]
    pub fn eq_str<V: AsRef<str>>(self, table: &str, col: &str, val: Option<V>) -> Self {
        if let Some(val) = val {
            if !val.as_ref().is_empty() {
                return self.eq(table, col, val.as_ref());
            }
        }
        self
    }

    /// 添加like比较条件，在`val`的两端添加`%`
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值
    ///
    #[inline]
    pub fn like(mut self, table: &str, col: &str, val: &str) -> Self {
        utils::where_like(self.0.parent.sql_params(), &self.0.prefix,
            AND, table, col, LikeType::Full, val);
        self
    }

    /// 当`val`不为`None`且不为空字符串时添加like比较条件，在`val`的两端添加`%`
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值
    ///
    #[inline]
    pub fn like_opt<V: AsRef<str>>(self, table: &str, col: &str, val: Option<V>) -> Self {
        match val {
            Some(val) => self.like(table, col, val.as_ref()),
            None => self,
        }
    }

    /// 添加like比较条件，在`val`的右边添加`%`
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值
    ///
    #[inline]
    pub fn like_right(mut self, table: &str, col: &str, val: &str) -> Self {
        utils::where_like(self.0.parent.sql_params(), &self.0.prefix,
            AND, table, col, LikeType::Right, val);
        self
    }

    /// 当`val`不为`None`且不为空字符串时添加like比较条件，在`val`的右边添加`%`
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `val`: 要比较的值
    ///
    #[inline]
    pub fn like_right_opt<V: AsRef<str>>(self, table: &str, col: &str, val: Option<V>) -> Self {
        match val {
            Some(val) => self.like_right(table, col, val.as_ref()),
            None => self,
        }
    }

    /// 当`v1`和`v2`不为`None`且不为空字符串时添加between比较条件，between是闭区间条件，即`v1 <= col <= v2`
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `v1`: 起始值，必须满足 v1 <= v2
    /// * `v2`: 结束值
    ///
    #[inline]
    pub fn between_opt<V: ToValue>(self, table: &str, col: &str, v1: Option<V>, v2: Option<V>) -> Self {
        if let (Some(v1), Some(v2)) = (v1, v2) {
            self.between(table, col, v1, v2)
        } else {
            self
        }
    }

    /// 添加between比较条件，between是闭区间条件，即`v1 <= col <= v2`
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `v1`: 起始值，必须满足 v1 <= v2
    /// * `v2`: 结束值
    ///
    pub fn between<V: ToValue>(mut self, table: &str, col: &str, v1: V, v2: V) -> Self {
        let psql = self.0.parent.sql();

        utils::replace_prefix(psql, &self.0.prefix, AND);
        utils::push_col(psql, table, col);

        psql.extend_from_slice(b" between ? and ?");

        let params = self.0.parent.params();
        params.push(v1.to_value());
        params.push(v2.to_value());

        self
    }

    /// 添加in条件，类似生成
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `v1`: 起始值，必须满足 v1 <= v2
    /// * `v2`: 结束值
    ///
    #[inline]
    pub fn in_opt<V: ToValue>(self, table: &str, col: &str, vals: Option<Vec<V>>) -> Self {
        match vals {
            Some(vals) => self.in_(table, col, vals),
            None => self,
        }
    }

    /// 添加in条件，类似生成
    ///
    /// Arguments:
    ///
    /// * `table`: 表名或表别名
    /// * `col`: 字段名
    /// * `v1`: 起始值，必须满足 v1 <= v2
    /// * `v2`: 结束值
    ///
    #[inline]
    pub fn in_<I: IntoIterator<Item = V>, V: ToValue>(mut self, table: &str, col: &str, iter: I) -> Self {
        let open = make_col_expr(table, col, "in (");
        utils::make_for_each(self.0.parent.sql_params(), &self.0.prefix, &self.0.prefix_overrides,
            AND, &open, ")", ", ", &mut iter.into_iter().map(|v| v.to_value()));
        self
    }

}

impl<T: ToValue> From<T> for InsertValue {
    fn from(value: T) -> Self {
        Self::Value(value.to_value())
    }
}

mod utils {
    use mysql_async::{prelude::ToValue, Value};

    use super::{LikeType, AND, ON};

    pub(crate) fn push_col(sql: &mut Vec<u8>, table: &str, col: &str) {
        if !table.is_empty() {
            sql.extend_from_slice(table.as_bytes());
            sql.push(b'.');
        }

        sql.extend_from_slice(col.as_bytes());
    }

    pub(crate) fn push_col_alias(sql: &mut Vec<u8>, table: &str, col: &str, alias: &str) {
        push_col(sql, table, col);

        if !alias.is_empty() {
            sql.extend_from_slice(b" as ");
            sql.extend_from_slice(alias.as_bytes());
        }

        sql.push(b',');
        sql.push(b' ');
    }

    pub(crate) fn make_col_expr(table: &str, col: &str, expr: &str) -> String {
        debug_assert!(!col.is_empty());
        let mut sql = String::new();
        if !table.is_empty() {
            sql.push_str(table);
            sql.push('.');
        }
        sql.push_str(col);
        sql.push(' ');
        sql.push_str(expr);
        sql
    }

    pub(crate) fn trim_add_values<I: IntoIterator<Item = V>, V: ToValue>(
        params: &mut Vec<Value>, iter: I) -> usize
    {
        let len = params.len();
        params.extend(iter.into_iter().map(|v| v.to_value()));
        params.len() - len
    }

    pub(crate) fn trim_add_sql(buf: &mut Vec<u8>, prefix: &str,
        prefix_overrides: &[String], sql: &str)
    {
        debug_assert!(!sql.is_empty());

        let mut sql = sql.as_bytes();
        let start_space = sql[0] == b' ';

        if !prefix.is_empty() && buf.ends_with(prefix.as_bytes()) {
            for item in prefix_overrides.iter() {
                let mut item = item.as_bytes();
                if !start_space {
                    item = &item[1..];
                }

                if sql.starts_with(item) {
                    sql = &sql[item.len()..];
                    break;
                }
            }
        }

        if !start_space { buf.push(b' '); }
        if !sql.is_empty() {
            buf.extend_from_slice(sql);
        }
    }

    pub(crate) fn make_for_each(
        (sql, params): (&mut Vec<u8>, &mut Vec<Value>), prefix: &str,
        prefix_overrides: &[String], sql_prefix: &str, open: &str,
        close: &str, sep: &str, iter: &mut dyn Iterator<Item = Value>)
    {
        debug_assert!(!sep.is_empty());

        let mut total = 0;

        // 计算参数数量并记录转换参数
        for item in iter {
            total += 1;
            params.push(item);
        }

        if total > 0 {
            trim_add_sql(sql, prefix, prefix_overrides, sql_prefix);

            if !open.is_empty() {
                sql.extend_from_slice(open.as_bytes());
            }

            sql.push(b'?');
            for _ in 1..total {
                sql.extend_from_slice(sep.as_bytes());
                sql.push(b'?');
            }

            if !close.is_empty() {
                sql.extend_from_slice(close.as_bytes());
            }
        }
    }

    pub(crate) fn where_expr<V: ToValue>(base: (&mut Vec<u8>, &mut Vec<Value>), prefix: &str,
        and_or: &str, table: &str, col: &str, opera: &str, val: V)
    {
        debug_assert!(!col.is_empty() && !opera.is_empty());

        replace_prefix(base.0, prefix, and_or);
        push_col(base.0, table, col);
        base.0.push(b' ');
        base.0.extend_from_slice(opera.as_bytes());
        base.0.extend_from_slice(b" ?");

        base.1.push(val.to_value());
    }

    pub(crate) fn where_like(base: (&mut Vec<u8>, &mut Vec<Value>), prefix: &str, and_or: &str,
        table: &str, col: &str, like_type: LikeType, val: &str)
    {
        if !val.is_empty() {
            let mut s = String::new();
            if like_type != LikeType::Right {
                s.push('%');
            }
            s.push_str(val);
            if like_type != LikeType::Left {
                s.push('%');
            }

            where_expr(base, prefix, and_or, table, col, "like", s.as_str());
        }
    }

    pub(crate) fn replace_prefix(buf: &mut Vec<u8>, prefix: &str, sql: &str) {
        if !buf.ends_with(prefix.as_bytes()) {
            buf.extend_from_slice(sql.as_bytes());
        }
    }

    pub(crate) fn join_on(buf: &mut Vec<u8>, table1: &str, col1: &str, expr: &str, table2: &str, col2: &str) {
        replace_prefix(buf, ON, AND);
        buf.extend_from_slice(table1.as_bytes());
        buf.extend_from_slice(b".");
        buf.extend_from_slice(col1.as_bytes());
        buf.push(b' ');
        buf.extend_from_slice(expr.as_bytes());
        buf.push(b' ');
        if !table2.is_empty() {
            buf.extend_from_slice(table2.as_bytes());
            buf.extend_from_slice(b".");
        }
        buf.extend_from_slice(col2.as_bytes());
    }

    pub(crate) fn order_by_iter<'a>(sql: &mut Vec<u8>, iter: &mut dyn Iterator<Item = (&'a str, &'a str, bool)>) {
        sql.extend_from_slice(b" order by ");
        for (table, column, desc) in iter {
            debug_assert!(!column.is_empty());
            push_col(sql, table, column);
            if desc {
                sql.extend_from_slice(b" desc");
            }
            sql.push(b',');
            sql.push(b' ');
        }
        sql.truncate(sql.len() - 2);
    }
}
