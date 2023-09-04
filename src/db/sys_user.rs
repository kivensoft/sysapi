use anyhow::Result;
use gensql::{table_define, get_conn, query_one_sql, query_all_sql, Queryable};
use localtime::LocalTime;

use crate::utils;

use super::{PageData, PageInfo, sys_role::SysRole};

table_define!("t_sys_user", SysUser,
    user_id:        u32         => USER_ID,
    role_id:        u32         => ROLE_ID,
    icon_id:        String      => ICON_ID,
    disabled:       u8          => DISABLED,
    username:       String      => USERNAME,
    password:       String      => PASSWORD,
    nickname:       String      => NICKNAME,
    mobile:         String      => MOBILE,
    email:          String      => EMAIL,
    updated_time:   LocalTime   => UPDATED_TIME,
    created_time:   LocalTime   => CREATED_TIME,
);

impl SysUser {
    /// 查询记录
     pub async fn select_page(value: &SysUser, page: PageInfo) -> Result<PageData<SysUser>> {
        let (tsql, psql, params) = gensql::SelectSql::with_page(page.index, page.size)
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq_opt("", Self::ROLE_ID, &value.role_id)
            .and_like_opt("", Self::USERNAME, &value.username)
            .and_like_opt("", Self::NICKNAME, &value.nickname)
            .and_like_opt("", Self::MOBILE, &value.mobile)
            .and_like_opt("", Self::EMAIL, &value.email)
            .end_where()
            .build_with_page()?;

        let mut conn = get_conn().await?;

        let total = if tsql.is_empty() {
            0
        } else {
            conn.query_one_sql(tsql, params.clone()).await?.map(|(total,)| total).unwrap_or(0)
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
            .and_eq("", col, &account.to_owned())
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
            .select_slice("", &Self::fields())
            .from(Self::TABLE)
            .where_sql()
            .and_eq_opt("", Self::ROLE_ID, &value.role_id)
            .and_like_opt("", Self::USERNAME, &value.username)
            .and_like_opt("", Self::NICKNAME, &value.nickname)
            .and_like_opt("", Self::MOBILE, &value.mobile)
            .and_like_opt("", Self::EMAIL, &value.email)
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
            .and_eq("", Self::USER_ID, &id)
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
            .where_sql()
            .and_eq("t", Self::USER_ID, &id)
            .end_where()
            .build();
        query_one_sql(&sql, &params).await
    }

}
