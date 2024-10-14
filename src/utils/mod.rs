//! 实用工具函数单元

#[allow(dead_code)]
pub mod audit;
#[allow(dead_code)]
pub mod bits;
pub mod consts;
pub mod kv;
#[allow(dead_code)]
pub mod md5_crypt;
pub mod multi_cache;
// #[allow(dead_code)]
// pub mod multi_level;
#[allow(dead_code)]
pub mod rcall;
#[allow(dead_code)]
pub mod staticmut;
// #[allow(dead_code)]
pub mod time;
pub mod uni_redis;

pub use kv::*;

#[macro_export]
macro_rules! opt_some {
    ($val:expr) => {
        match $val {
            Some(v) => v,
            None => unsafe { std::hint::unreachable_unchecked() },
        }
    };
}
