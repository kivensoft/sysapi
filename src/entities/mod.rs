use serde::{Deserialize, Serialize};

pub mod sys_api;
pub mod sys_config;
pub mod sys_dict;
pub mod sys_menu;
pub mod sys_permission;
pub mod sys_role;
pub mod sys_user;
pub mod sys_user_state;

#[derive(Serialize, Deserialize)]
pub struct PageData<T> {
    pub total: u32,
    pub list: Vec<T>,
}

#[derive(Clone, Default)]
pub struct PageInfo {
    pub index: u32,
    pub size: u32,
    pub total: Option<u32>,
}

#[derive(Deserialize, Clone)]
pub struct PageQuery<T> {
    #[serde(flatten)]
    pub inner: T,
    pub i: u32,
    pub p: u32,
    pub a: Option<i32>,
}

impl PageInfo {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn with(index: u32, size: u32) -> Self {
        Self {
            index,
            size,
            total: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_total(index: u32, size: u32, total: Option<u32>) -> Self {
        Self { index, size, total }
    }
}

impl<T> PageQuery<T> {
    pub fn page_info(&self) -> PageInfo {
        PageInfo {
            index: self.i,
            size: self.p,
            total: match self.a {
                Some(total) => {
                    if total < 0 {
                        None
                    } else {
                        Some(total as u32)
                    }
                }
                None => None,
            },
        }
    }
}
