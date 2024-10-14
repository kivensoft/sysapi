pub mod account;
pub mod api;
pub mod config;
pub mod debug;
pub mod dict;
pub mod login;
pub mod menu;
pub mod permission;
pub mod role;
pub mod tools;
pub mod user;

const REC_NOT_EXISTS: &str = "记录不存在";

#[derive(serde::Deserialize)]
pub struct GetReq {
    pub id: u32,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct UseBuilltinReq {
    pub use_builtin: Option<bool>,
}
