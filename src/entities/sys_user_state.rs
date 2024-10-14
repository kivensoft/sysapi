//! 用户状态表
use std::net::Ipv4Addr;

use gensql::{table, DbResult};
use localtime::LocalTime;

#[table("t_sys_user_state")]
pub struct SysUserState {
    /// 用户id
    #[table(id)]
    user_id: u32,
    /// 总登录次数
    total_login: u32,
    /// 最后登录时间
    last_login_time: LocalTime,
    /// 最后登录ip
    last_login_ip: String,
}

impl SysUserState {
    /// 更新登录状态，当前的登录次数+1，并更新最后登录时间
    pub async fn incr(user_id: u32, ip: &Ipv4Addr) -> DbResult<()> {
        let ip = ip.to_string();
        let now = LocalTime::now();

        match SysUserState::select_by_id(user_id).await? {
            Some(mut user_state) => {
                user_state.total_login = Some(user_state.total_login.unwrap() + 1);
                user_state.last_login_time = Some(now);
                user_state.last_login_ip = Some(ip);
                SysUserState::update_by_id(user_state).await?;
            }
            None => {
                let val = SysUserState {
                    user_id: Some(user_id),
                    total_login: Some(1),
                    last_login_time: Some(now),
                    last_login_ip: Some(ip),
                };
                SysUserState::insert(val).await?;
            }
        };

        Ok(())
    }
}
