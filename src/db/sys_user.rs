use anyhow::Result;
use gensql::{table_define, get_conn, query_one_sql, query_all_sql, Queryable};
use localtime::LocalTime;

use crate::utils;

use super::{PageData, PageInfo, sys_role::SysRole};

table_define!{"t_sys_user", SysUser,
    user_id:        u32,
    role_id:        u32,
    icon_id:        String,
    disabled:       u8,
    username:       String,
    password:       String,
    nickname:       String,
    mobile:         String,
    email:          String,
    updated_time:   LocalTime,
    created_time:   LocalTime,
}

impl SysUser {
    /// 查询记录
     pub async fn select_page(value: &SysUser, page: PageInfo) -> Result<PageData<SysUser>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
                .eq_opt("", Self::ROLE_ID, &value.role_id)
                .like_opt("", Self::USERNAME, &value.username)
                .like_opt("", Self::NICKNAME, &value.nickname)
                .like_opt("", Self::MOBILE, &value.mobile)
                .like_opt("", Self::EMAIL, &value.email)
                .end_where()
            .build_with_page(page.index, page.size, page.total)?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            page.total.unwrap_or(0)
        } else {
            conn.query_one_sql(&tsql, &params).await?.map(|(total,)| total).unwrap_or(0)
        };

        let list = conn.query_all_sql(psql, params, Self::row_map).await?;

        Ok(PageData { total, list, })
    }

    /// 按登录账号查询，登录账号可以是 登录名/电子邮箱/手机号 任意一个
    pub async fn select_by_account(account: &str) -> Result<Option<SysUser>> {
        type T = SysUser;
        const FIELDS: [&str; 5] = [T::USER_ID, T::ROLE_ID, T::DISABLED,
                T::USERNAME, T::PASSWORD];

        let col = match utils::check_account_type(account) {
            utils::AccountType::Username => Self::USERNAME,
            utils::AccountType::Email => Self::EMAIL,
            utils::AccountType::Mobile => Self::MOBILE,
        };

        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", &FIELDS)
            .from(Self::TABLE)
            .where_sql()
            .eq("", col, &account.to_owned())
            .end_where()
            .build();

        let rec = query_one_sql(&sql, &params).await?
            .map(gensql::row_map!(SysUser,
                user_id,
                role_id,
                disabled,
                username,
                password,
            ));

        Ok(rec)
    }

    pub async fn select_by(value: &SysUser) -> Result<Vec<SysUser>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_slice("", Self::FIELDS)
            .from(Self::TABLE)
            .where_sql()
            .eq_opt("", Self::ROLE_ID, &value.role_id)
            .like_opt("", Self::USERNAME, &value.username)
            .like_opt("", Self::NICKNAME, &value.nickname)
            .like_opt("", Self::MOBILE, &value.mobile)
            .like_opt("", Self::EMAIL, &value.email)
            .end_where()
            .build();

        let recs = query_all_sql(&sql, &params, Self::row_map).await?;

        Ok(recs)
    }

    /// 根据用户id查询角色id
    pub async fn select_role_by_id(id: u32) -> Result<Option<u32>> {
        let (sql, params) = gensql::SelectSql::new()
            .select(Self::ROLE_ID)
            .from(Self::TABLE)
            .where_sql()
            .eq("", Self::USER_ID, &id)
            .end_where()
            .build();

        query_one_sql(&sql, &params).await
    }

    /// 根据用户id查询权限
    pub async fn select_permissions_by_id(id: u32) -> Result<Option<String>> {
        type R = SysRole;
        let (sql, params) = gensql::SelectSql::new()
            .select_ext("r", R::PERMISSIONS)
            .from_alias(Self::TABLE, "t")
            .join(R::TABLE, "r")
                .on_eq("t", Self::ROLE_ID, "r", R::ROLE_ID)
                .end_join()
            .where_sql()
                .eq("t", Self::USER_ID, &id)
                .end_where()
            .build();
        query_one_sql(&sql, &params).await
    }

}
