use anyhow::Result;
use gensql::table_define;
use localtime::LocalTime;

table_define!("t_sys_user_state", SysUserState,
    user_id:            u32       => USER_ID,
    total_login:        u32       => TOTAL_LOGIN,
    last_login_time:    LocalTime => LAST_LOGIN_TIME,
    last_login_ip:      String    => LAST_LOGIN_IP,
);

impl SysUserState {

}
