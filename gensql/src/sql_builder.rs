//! FastStr implement
// author: kiven
// slince 2023-09-20

use super::{ToValue, Value};
use compact_str::CompactString;
use itoa::Buffer;
use rclite::Arc;
use thiserror::Error;

const AND: &str = " and ";
const OR: &str = " or ";
const ON: &str = " on ";

#[derive(Error, Debug)]
pub enum GenSqlError {
    #[error("unsearch ` from ` in sql")]
    UnsearchFrom,
}

enum InsertValue {
    Sql(CompactString),
    Value(Value),
}

pub trait GeneratorSql {
    fn sql(&mut self) -> &mut Vec<u8>;
    fn params(&mut self) -> &mut Vec<Value>;
}

#[derive(Default)]
struct BaseSql {
    sql: Vec<u8>,
    params: Vec<Value>,
}

pub struct SelectSql(BaseSql);

pub struct InsertSql {
    sql: Vec<u8>,
    params: Vec<InsertValue>,
}

pub struct UpdateSql(BaseSql);

pub struct DeleteSql(BaseSql);

pub struct JoinSql {
    parent: SelectSql,
    table: CompactString,
}

pub struct TrimSql<T: GeneratorSql> {
    pub(crate) parent: T,
    pub(crate) prefix: Arc<CompactString>,
    pub(crate) suffix: CompactString,
    pub(crate) prefix_overrides: Vec<CompactString>,
    pub(crate) suffix_overrides: Vec<CompactString>,
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
    log::trace!("[SQL]: {}", sql);
}

#[inline]
pub fn db_log_params(params: &[Value]) {
    log::trace!("[SQL-PARAMS]: {:?}", params);
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
}

impl GeneratorSql for SelectSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.0.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.0.params
    }
}

impl GeneratorSql for DeleteSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.0.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.0.params
    }
}

impl GeneratorSql for UpdateSql {
    fn sql(&mut self) -> &mut Vec<u8> {
        &mut self.0.sql
    }

    fn params(&mut self) -> &mut Vec<Value> {
        &mut self.0.params
    }
}

impl<T: GeneratorSql> GeneratorSql for TrimSql<T> {
    fn sql(&mut self) -> &mut Vec<u8> {
        self.parent.sql()
    }

    fn params(&mut self) -> &mut Vec<Value> {
        self.parent.params()
    }
}

impl<T: GeneratorSql> GeneratorSql for WhereSql<T> {
    fn sql(&mut self) -> &mut Vec<u8> {
        self.0.sql()
    }

    fn params(&mut self) -> &mut Vec<Value> {
        self.0.params()
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

    pub fn value_raw(mut self, col: &str, raw: &str) -> Self {
        debug_assert!(!col.is_empty() && !raw.is_empty());
        self.sql.extend_from_slice(col.as_bytes());
        self.sql.extend_from_slice(b", ");
        self.params.push(InsertValue::Sql(CompactString::new(raw)));
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
                InsertValue::Sql(s) => {
                    sql.extend_from_slice(s.as_bytes());
                }
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
    pub fn select(self, col: &str) -> Self {
        self.select_as("", col, "")
    }

    #[inline]
    pub fn select_with_table(self, table: &str, col: &str) -> Self {
        self.select_as(table, col, "")
    }

    pub fn select_as(mut self, table: &str, col: &str, alias: &str) -> Self {
        debug_assert!(!col.is_empty());
        let sql = &mut self.0.sql;
        push_col(sql, table, col);
        if !alias.is_empty() {
            sql.extend_from_slice(b" as ");
            sql.extend_from_slice(alias.as_bytes());
        }
        sql.extend_from_slice(b", ");
        self
    }

    #[inline]
    pub fn select_columns(self, cols: &[&str]) -> Self {
        self.select_columns_with_table("", cols)
    }

    pub fn select_columns_with_table(mut self, table: &str, cols: &[&str]) -> Self {
        debug_assert!(!cols.is_empty() && cols.iter().find(|v| v.is_empty()).is_none());
        let sql = &mut self.0.sql;
        cols.iter().for_each(|col| {
            push_col(sql, table, col);
            sql.extend_from_slice(b", ");
        });
        self
    }

    #[inline]
    pub fn from(self, table: &str) -> Self {
        self.from_alias(table, "")
    }

    pub fn from_alias(mut self, table: &str, alias: &str) -> Self {
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
    pub fn group_by(self, col: &str) -> Self {
        self.group_by_columns_with_table("", std::slice::from_ref(&col))
    }

    #[inline]
    pub fn group_by_with_table(self, table: &str, col: &str) -> Self {
        self.group_by_columns_with_table(table, std::slice::from_ref(&col))
    }

    #[inline]
    pub fn group_by_columns(self, cols: &[&str]) -> Self {
        self.group_by_columns_with_table("", cols)
    }

    pub fn group_by_columns_with_table(mut self, table: &str, cols: &[&str]) -> Self {
        debug_assert!(!cols.is_empty() && cols.iter().find(|v| v.is_empty()).is_none());
        let sql = &mut self.0.sql;
        sql.extend_from_slice(b" group by ");
        for item in cols.iter() {
            push_col(sql, table, item);
        }
        self
    }

    #[inline]
    pub fn order_by(self, col: &str) -> Self {
        self.order_by_columns_with_table("", std::slice::from_ref(&col))
    }

    #[inline]
    pub fn order_by_with_table(self, table: &str, col: &str) -> Self {
        self.order_by_columns_with_table(table, std::slice::from_ref(&col))
    }

    #[inline]
    pub fn order_by_columns(self, cols: &[&str]) -> Self {
        self.order_by_columns_with_table("", cols)
    }

    pub fn order_by_columns_with_table(mut self, table: &str, cols: &[&str]) -> Self {
        debug_assert!(!cols.is_empty() && cols.iter().find(|v| v.is_empty()).is_none());
        let sql = &mut self.0.sql;
        sql.extend_from_slice(b" order by ");
        for item in cols.iter() {
            push_col(sql, table, item);
        }
        self
    }

    #[inline]
    pub fn order_by_desc(self, col: &str) -> Self {
        self.order_by_with_table_desc("", col)
    }

    pub fn order_by_with_table_desc(mut self, table: &str, col: &str) -> Self {
        debug_assert!(!col.is_empty());
        let sql = &mut self.0.sql;
        sql.extend_from_slice(b" order by ");
        push_col(sql, table, col);
        sql.extend_from_slice(b" desc");
        self
    }

    pub fn limits(mut self, offset: u32, count: u32) -> Self {
        if count > 0 {
            Self::set_limits(&mut self.0.sql, offset, count);
        }
        self
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

    pub fn set_raw(mut self, col: &str, raw: &str) -> Self {
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

        let table = CompactString::new(if !alias.is_empty() { alias } else { table });

        Self { parent, table }
    }

    #[inline]
    pub(crate) fn end_join(self) -> SelectSql {
        self.parent
    }

    #[inline]
    pub fn on(mut self, expr: &str) -> Self {
        debug_assert!(!expr.is_empty());
        let sql = &mut self.parent.0.sql;
        Self::on_raw(sql, expr);
        self
    }

    pub fn on_eq(mut self, self_col: &str, other_table: &str, other_col: &str) -> Self {
        debug_assert!(!self_col.is_empty() && !other_table.is_empty() && !other_col.is_empty());
        let sql = &mut self.parent.0.sql;
        Self::on_raw(sql, &self.table);
        sql.push(b'.');
        sql.extend_from_slice(self_col.as_bytes());
        sql.extend_from_slice(b" = ");
        sql.extend_from_slice(other_table.as_bytes());
        sql.push(b'.');
        sql.extend_from_slice(other_col.as_bytes());

        self
    }

    pub fn on_val<T: ToValue>(mut self, self_col: &str, expr: &str, val: T) -> Self {
        debug_assert!(!self_col.is_empty() && !expr.is_empty());
        self.parent.0.params.push(val.to_value());

        let sql = &mut self.parent.0.sql;
        Self::on_raw(sql, &self.table);
        sql.push(b'.');
        sql.extend_from_slice(self_col.as_bytes());
        sql.push(b' ');
        sql.extend_from_slice(expr.as_bytes());
        sql.extend_from_slice(b" ?");

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
        self.on_val_opt(col, "=", val)
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
        if !sql.ends_with(ON.as_bytes()) {
            sql.extend_from_slice(AND.as_bytes());
        }
        sql.extend_from_slice(raw.as_bytes());
    }

}

impl TrimSql<BaseSql> {
    pub fn new(
        prefix: &str,
        suffix: &str,
        prefix_overrides: &[&str],
        suffix_overrides: &[&str],
    ) -> Self {
        Self::with_parent(
            BaseSql::default(),
            prefix,
            suffix,
            prefix_overrides,
            suffix_overrides,
        )
    }
}

impl<T: GeneratorSql> TrimSql<T> {
    pub(crate) fn with_parent(
        mut parent: T,
        prefix: &str,
        suffix: &str,
        prefix_overrides: &[&str],
        suffix_overrides: &[&str],
    ) -> Self {
        debug_assert!(
            prefix.is_empty()
                || prefix.as_bytes()[0] == b' ' && prefix.as_bytes()[prefix.len() - 1] == b' '
        );
        debug_assert!(
            prefix_overrides.is_empty() || prefix_overrides.iter().find(|s| s.is_empty()).is_none()
        );
        debug_assert!(
            suffix_overrides.is_empty() || suffix_overrides.iter().find(|s| s.is_empty()).is_none()
        );

        let psql = parent.sql();

        if !prefix.is_empty() {
            psql.extend_from_slice(prefix.as_bytes());
        }

        let prefix_overrides = prefix_overrides
            .iter()
            .map(|s| CompactString::new(s))
            .collect();
        let suffix_overrides = suffix_overrides
            .iter()
            .map(|s| CompactString::new(s))
            .collect();

        Self {
            parent,
            prefix: Arc::new(CompactString::new(prefix)),
            suffix: CompactString::new(suffix),
            prefix_overrides,
            suffix_overrides,
        }
    }

    /// 在语句结尾的时候，进行截断
    pub(crate) fn end_trim(mut self) -> T {
        let psql = self.parent.sql();

        // 语句为空
        if !self.prefix.is_empty() && psql.ends_with(self.prefix.as_bytes()) {
            psql.truncate(psql.len() - self.prefix.len());
        } else {
            // 去除后缀多余的字符
            for item in self.suffix_overrides {
                if psql.ends_with(item.as_bytes()) {
                    psql.truncate(psql.len() - item.len());
                    break;
                }
            }
            // 添加后缀
            psql.extend_from_slice(self.suffix.as_bytes());
        }

        self.parent
    }

    #[inline]
    pub fn add_sql(mut self, sql: &str) -> Self {
        self.inner_add_sql(sql);
        self
    }

    #[inline]
    pub fn add_value<V: ToValue>(mut self, sql: &str, val: V) -> Self {
        self.inner_add_sql(sql);
        self.parent.params().push(val.to_value());
        self
    }

    #[inline]
    pub fn add_values<V: ToValue>(mut self, sql: &str, vals: Vec<V>) -> Self {
        self.inner_add_values(sql, vals);
        self
    }

    #[inline]
    pub fn if_add_value<V: ToValue>(mut self, cond: bool, sql: &str, val: V) -> Self {
        if cond {
            self.inner_add_sql(sql);
            self.parent.params().push(val.to_value());
        }
        self
    }

    #[inline]
    pub fn if_add_values<V: ToValue>(self, cond: bool, sql: &str, vals: Vec<V>) -> Self {
        if cond {
            self.add_values(sql, vals)
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
    pub fn for_each<V: ToValue>(
        mut self,
        open: &str,
        close: &str,
        sep: &str,
        list: Vec<V>,
    ) -> Self {
        self.inner_for_each(open, close, sep, list);
        self
    }

    pub(crate) fn inner_add_sql(&mut self, sql: &str) {
        let psql = self.parent.sql();
        let mut sql = sql.as_bytes();
        let start_space = sql[0] == b' ';

        if !self.prefix.is_empty() && psql.ends_with(self.prefix.as_bytes()) {
            for mut item in self.prefix_overrides.iter().map(|s| s.as_bytes()) {
                if !start_space {
                    item = &item[1..];
                }
                if sql.starts_with(item) {
                    sql = &sql[item.len()..];
                    break;
                }
            }
        }

        if !start_space {
            psql.push(b' ');
        }
        psql.extend_from_slice(sql);
    }

    pub(crate) fn inner_add_values<V: ToValue>(&mut self, sql: &str, vals: Vec<V>) {
        if !vals.is_empty() {
            self.inner_add_sql(sql);

            let params = self.parent.params();
            for item in vals.into_iter() {
                params.push(item.to_value());
            }
        }
    }

    pub(crate) fn inner_for_each<V: ToValue>(
        &mut self,
        open: &str,
        close: &str,
        sep: &str,
        list: Vec<V>,
    ) {
        if !list.is_empty() {
            if !open.is_empty() {
                self.inner_add_sql(open);
            }

            let psql = self.parent.sql();

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

            for item in list.into_iter() {
                params.push(item.to_value());
            }
        }
    }
}

impl WhereSql<BaseSql> {
    pub fn new() -> Self {
        Self::with_parent(BaseSql::default())
    }

    pub fn to_sql_params(self) -> (String, Vec<Value>) {
        let b = self.0.end_trim();
        let sql = unsafe { String::from_utf8_unchecked(b.sql) };
        (sql, b.params)
    }
}

impl<T: GeneratorSql> WhereSql<T> {
    #[inline]
    pub(crate) fn with_parent(parent: T) -> Self {
        Self(TrimSql::with_parent(parent, " where ", "", &[AND, OR], &[]))
    }

    #[inline]
    pub(crate) fn end_where(self) -> T {
        self.0.end_trim()
    }

    #[inline]
    pub fn add_sql(mut self, sql: &str) -> Self {
        self.0.inner_add_sql(sql);
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
    pub fn if_add_value<V: ToValue>(mut self, cond: bool, sql: &str, val: V) -> Self {
        if cond {
            self.0.inner_add_sql(sql);
            self.0.parent.params().push(val.to_value());
        }
        self
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
    pub fn add_values<V: ToValue>(mut self, sql: &str, vals: Vec<V>) -> Self {
        self.0.inner_add_values(sql, vals);
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
    pub fn if_add_values<V: ToValue>(mut self, cond: bool, sql: &str, vals: Vec<V>) -> Self {
        if cond {
            self.0.inner_add_values(sql, vals);
        }
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
    pub fn for_each<V: ToValue>(
        mut self,
        open: &str,
        close: &str,
        sep: &str,
        vals: Vec<V>,
    ) -> Self {
        self.0.inner_for_each(open, close, sep, vals);
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
    pub fn cmp<V: ToValue>(mut self, table: &str, col: &str, expr: &str, val: V) -> Self {
        debug_assert!(!expr.is_empty());
        self.inner_expr(AND, table, col, expr, val);
        self
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
        self.inner_expr(AND, table, col, "=", val);
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
        self.inner_like(AND, table, col, LikeType::Full, val);
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
        self.inner_like(AND, table, col, LikeType::Right, val);
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
        let prefix = self.0.prefix.clone();
        let psql = self.0.sql();

        if !psql.ends_with(prefix.as_bytes()) {
            psql.extend_from_slice(AND.as_bytes());
        }

        push_col(psql, table, col);

        psql.extend_from_slice(b" between ? and ?");

        let params = self.0.params();
        params.push(v1.to_value());
        params.push(v2.to_value());

        self
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
    pub fn between_opt<V: ToValue>(
        self,
        table: &str,
        col: &str,
        v1: Option<V>,
        v2: Option<V>,
    ) -> Self {
        if let (Some(v1), Some(v2)) = (v1, v2) {
            self.between(table, col, v1, v2)
        } else {
            self
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
    pub fn in_<V: ToValue>(mut self, table: &str, col: &str, vals: Vec<V>) -> Self {
        if !vals.is_empty() {
            let prefix = self.0.prefix.clone();
            let psql = self.0.sql();

            if !psql.ends_with(prefix.as_bytes()) {
                psql.extend_from_slice(AND.as_bytes());
            }

            push_col(psql, table, col);

            psql.extend_from_slice(b" in (");
            for _ in 0..vals.len() {
                psql.extend_from_slice(b"?, ");
            }
            psql.truncate(psql.len() - 2);
            psql.push(b')');

            let params = self.0.params();
            for val in vals {
                params.push(val.to_value());
            }
        }

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

    fn inner_expr<V: ToValue>(&mut self, opera: &str, table: &str, col: &str, expr: &str, val: V) {
        debug_assert!(!col.is_empty());

        let prefix = self.0.prefix.clone();
        let psql = self.0.sql();

        if !psql.ends_with(prefix.as_bytes()) {
            psql.extend_from_slice(opera.as_bytes());
        }

        push_col(psql, table, col);
        psql.push(b' ');
        psql.extend_from_slice(expr.as_bytes());
        psql.extend_from_slice(b" ?");

        self.0.params().push(val.to_value());
    }

    fn inner_like(&mut self, opera: &str, table: &str, col: &str, like_type: LikeType, val: &str) {
        if !val.is_empty() {
            let mut s = CompactString::new("");
            if like_type != LikeType::Right {
                s.push('%');
            }
            s.push_str(val);
            if like_type != LikeType::Left {
                s.push('%');
            }

            self.inner_expr(opera, table, col, "like", s.as_str());
        }
    }
}

fn push_col(out: &mut Vec<u8>, table: &str, col: &str) {
    if !table.is_empty() {
        out.extend_from_slice(table.as_bytes());
        out.push(b'.');
    }
    out.extend_from_slice(col.as_bytes());
}
