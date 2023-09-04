pub mod sys_api;
pub mod sys_config;
pub mod sys_dict;
pub mod sys_menu;
pub mod sys_permission;
pub mod sys_role;
pub mod sys_user;
pub mod sys_user_state;

use serde::{Deserialize, Serialize};

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
