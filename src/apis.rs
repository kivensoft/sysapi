pub mod account;
pub mod api;
pub mod auth;
pub mod config;
pub mod debug;
pub mod dict;
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
