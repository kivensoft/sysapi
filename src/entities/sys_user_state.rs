use anyhow::Result;
use gensql::table_define;
use localtime::LocalTime;

table_define!("t_sys_user_state", SysUserState,
    user_id:            u32,
    total_login:        u32,
    last_login_time:    LocalTime,
    last_login_ip:      String,
);
