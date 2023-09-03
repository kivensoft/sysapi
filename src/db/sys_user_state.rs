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
    /// 删除记录
    pub async fn delete_by_id(id: u32) -> Result<u32> {
        super::exec_sql(&Self::stmt_delete(&id)).await
    }

    /// 插入记录，返回(插入记录数量, 自增ID的值)
    pub async fn insert(value: &SysUserState) -> Result<(u32, u32)> {
        super::insert_sql(&Self::stmt_insert(value)).await
    }

    /// 更新记录
    pub async fn update_by_id(value: &SysUserState) -> Result<u32> {
        super::exec_sql(&Self::stmt_update(value)).await
    }

    /// 查询记录
    pub async fn select_by_id(id: u32) -> Result<Option<SysUserState>> {
        Ok(super::query_one_sql(&Self::stmt_select(&id)).await?.map(Self::row_map))
    }

}
