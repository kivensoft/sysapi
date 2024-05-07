//! 实用工具函数单元
#[allow(dead_code)]
pub mod bits;
pub mod md5_crypt;
pub mod mq_util;
pub mod code_tree;
// #[allow(dead_code)]
pub mod time;

pub const PUBLIC_PERMIT_CODE: i16 = -1;
pub const ANONYMOUS_PERMIT_CODE: i16 = -2;
pub const INNER_GROUP_CODE: i16 = -1;

pub const PUBLIC_PERMIT_NAME: &str = "公共许可";
pub const ANONYMOUS_PERMIT_NAME: &str = "匿名许可";
pub const INNER_GROUP_NAME: &str = "内置权限";

#[macro_export]
macro_rules! opt_some {
    ($val:expr) => {
        match $val {
            Some(v) => v,
            None => unsafe { std::hint::unreachable_unchecked() },
        }
    };
}
