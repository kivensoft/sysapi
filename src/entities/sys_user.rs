//! 用户表
use super::{sys_role::SysRole, PageData, PageInfo};
use crate::{
    entities::sys_user_state::SysUserState,
    services::{gmc, uri},
    utils::{
        self, consts, time::{gen_time_desc, unix_timestamp}
    },
    AppConf, AppGlobal,
};
use anyhow_ext::{Context, Result};
use base64::{engine::general_purpose, Engine};
use gensql::{table, DbResult, Queryable};
use httpserver::http_bail;
use localtime::LocalTime;
use std::{net::Ipv4Addr, sync::Arc};

/// 允许最大的登录失败次数
const MAX_FAIL_COUNT: u32 = 10;
/// 登录失败达到限制值后的禁止登录时长(单位: 秒)
const DISABLE_LOGIN_TTL: u32 = 300;
const CATEGORY: &str = utils::consts::gmc::SYS_USER;

/// 登录账号类型
pub enum AccountType {
    Username,
    Email,
    Mobile,
}

/// 账户禁用状态类型
#[allow(dead_code)]
pub enum DisabledType {
    /// 正常
    Normal,
    /// 禁用
    Disabled,
    /// 删除
    Deleted,
}

/// 系统接口表
#[table("t_sys_user")]
pub struct SysUser {
    /// 用户id
    #[table(id)]
    user_id: u32,
    /// 角色id
    role_id: u32,
    /// 用户头像
    icon_id: String,
    /// 禁用标志, 0: 启用, 1: 禁用
    disabled: u8,
    /// 用户名
    username: String,
    /// 口令
    password: String,
    /// 昵称
    nickname: String,
    /// 手机号
    mobile: String,
    /// 电子邮件
    email: String,
    /// 更新时间
    updated_time: LocalTime,
    /// 创建时间
    created_time: LocalTime,
}

impl SysUser {
    pub async fn insert_with_notify(self) -> DbResult<(u32, u32)> {
        let user_id = self.user_id;
        let ret = self.insert().await;
        if ret.is_ok() {
            Self::notify_changed(user_id).await;
        }
        ret
    }

    pub async fn update_with_notify(self) -> DbResult<bool> {
        let user_id = self.user_id;
        let ret = self.update_by_id().await;
        if ret.is_ok() {
            Self::notify_changed(user_id).await;
        }
        ret
    }

    pub async fn delete_with_notify(id: u32) -> DbResult<bool> {
        match Self::select_by_id(id).await? {
            Some(record) => {
                let user_id = record.user_id;
                let ret = Self::delete_by_id(id).await;
                if ret.is_ok() {
                    Self::notify_changed(user_id).await;
                }
                ret
            }
            None => Ok(false),
        }
    }

    pub async fn notify_changed(id: Option<u32>) {
        let id = match id {
            Some(n) => format!("{n}"),
            None => String::new(),
        };
        crate::services::gmc::get_cache().notify(CATEGORY, &id).await
    }

    /// 查询记录
    pub async fn select_page(value: SysUser, page: PageInfo) -> DbResult<PageData<SysUser>> {
        let (tsql, psql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| {
                w.eq_opt("", Self::ROLE_ID, value.role_id)
                    .eq_opt("", Self::DISABLED, value.disabled)
                    .expr_if(
                        value.disabled.is_none(),
                        "",
                        Self::DISABLED,
                        "!=",
                        DisabledType::Deleted as u8,
                    )
                    .like_opt("", Self::USERNAME, value.username)
                    .like_opt("", Self::NICKNAME, value.nickname)
                    .like_opt("", Self::MOBILE, value.mobile)
                    .like_opt("", Self::EMAIL, value.email)
            })
            .build_with_page(page.index, page.size, page.total);

        let mut conn = gensql::get_conn().await?;

        let total = match page.total {
            Some(total) => total as usize,
            None => conn.query_one(tsql, params.clone()).await?.unwrap_or(0),
        };

        let list = conn.query_fast(psql, params).await?;

        Ok(PageData { total, list })
    }

    /// 按登录账号查询，登录账号可以是 登录名/电子邮箱/手机号 任意一个
    pub async fn select_by_account(account: &str) -> DbResult<Option<SysUser>> {
        let col = match check_account_type(account) {
            AccountType::Username => Self::USERNAME,
            AccountType::Email => Self::EMAIL,
            AccountType::Mobile => Self::MOBILE,
        };

        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| w.eq("", col, account.to_owned()))
            .build();

        gensql::sql_query_one(sql, params).await
    }

    /// 根据 user_name/nickname,mobile/email 查找用户
    pub async fn select_by(value: SysUser) -> DbResult<Vec<SysUser>> {
        let (sql, params) = gensql::SelectSql::new()
            .from(Self::TABLE_NAME)
            .where_sql(|w| {
                w.eq_opt("", Self::ROLE_ID, value.role_id)
                    .like_opt("", Self::USERNAME, value.username)
                    .like_opt("", Self::NICKNAME, value.nickname)
                    .like_opt("", Self::MOBILE, value.mobile)
                    .like_opt("", Self::EMAIL, value.email)
            })
            .build();

        gensql::sql_query_fast(sql, params).await
    }

    pub async fn select_by_id_with_cache(id: u32) -> DbResult<Option<Arc<SysUser>>> {
        let mut buf = itoa::Buffer::new();
        let cache_key = buf.format(id);

        // 优先使用缓存查询
        if let Some(rec) = gmc::get_cache().get_json(CATEGORY, cache_key).await {
            return Ok(Some(rec));
        }

        Self::select_by_id(id).await.map(|v| v.map(Arc::new))
    }

    /// 根据用户id查询角色id和最后更新时间（用于权限校验）
    pub async fn select_role_and_updated(id: u32) -> DbResult<Option<(u32, LocalTime)>> {
        let (sql, params) = gensql::SelectSql::new()
            .select_columns("", &[Self::ROLE_ID, Self::UPDATED_TIME])
            .from(Self::TABLE_NAME)
            .where_sql(|w| w.eq("", Self::USER_ID, id))
            .build();

        gensql::sql_query_one(sql, params).await
    }

    /// 根据用户id查询权限
    pub async fn select_permissions_by_id(id: u32) -> DbResult<Option<String>> {
        type T = SysUser;
        type R = SysRole;
        const T: &str = "t";
        const R: &str = "r";

        let (sql, params) = gensql::SelectSql::new()
            .select(R, R::PERMISSIONS)
            .from_as(T::TABLE_NAME, T)
            .join(R::TABLE_NAME, R, |j| j.on_eq(Self::ROLE_ID, R, R::ROLE_ID))
            .where_sql(|w| w.eq(T, Self::USER_ID, id))
            .build();

        gensql::sql_query_one(sql, params).await
    }
}

/// 获取账号名类型（登录名/邮箱/手机号）
/// Arguments
///
/// * `account`: 账号名
///
pub fn check_account_type(account: &str) -> AccountType {
    if account.len() == 11 && is_number(account) {
        return AccountType::Mobile;
    }

    if account.find('@').is_some() {
        return AccountType::Email;
    }
    AccountType::Username
}

/// 生成token
///
/// Arguments
///
/// * `user_id`: 用户id
///
pub fn create_jwt_token(user_id: u32) -> Result<String> {
    let ac = AppConf::get();
    jwt::encode(
        serde_json::json!({
            "uid": user_id.to_string(),
            "created": unix_timestamp(),
        }),
        &ac.jwt_key,
        &ac.jwt_iss,
        AppGlobal::get().jwt_ttl as u64,
    )
    .context("生成jwt令牌失败")
}

/// 生成刷新token的key
///
/// Arguments
///
/// * `token`: 令牌
/// * `key`": 生成key的键
pub fn create_refresh_token(token: &str, key: &str) -> String {
    let mut hasher = md5::Context::new();
    hasher.consume(token);
    hasher.consume(key);

    general_purpose::URL_SAFE_NO_PAD.encode(&hasher.compute()[..])
}

/// 用户登录操作
///
/// Arguments
///
/// * `rid`: http请求id
/// * `account`": 账号(用户名/手机号/电子邮箱)
/// * `password`": 口令
/// * `ip`": 客户端ip
///
pub async fn user_login(account: &str, password: &str, ip: &Ipv4Addr) -> Result<SysUser> {
    let cache_key = gen_login_fail_key(account);

    // 校验是否已经达到最大尝试次数，如已达到，返回失败信息
    check_login_count(&cache_key).await?;

    // 加载账号对应的记录
    let user = get_user_by_account(account).await?;
    // 校验口令
    check_password(account, password, user.password.as_ref().unwrap()).await?;

    // 更新用户登录次数，时间等状态
    SysUserState::incr(user.user_id.unwrap(), ip).await?;

    // 清空缓冲中对应账号的失败次数信息
    uri::del(&cache_key).await;

    Ok(user)
}

/// 校验失败登录次数
///
/// Arguments
///
/// * `cache_key`: 基于登录账号的缓存键名
///
async fn check_login_count(cache_key: &str) -> Result<()> {
    // 获取缓冲中保存的当前用户登录失败次数
    let mut fail_count = 0;
    if let Some(s) = uri::get::<String>(cache_key).await {
        if let Ok(n) = s.parse() {
            fail_count = n;
        }
    }

    // 判断当前登录次数是否达到限制值
    if fail_count >= MAX_FAIL_COUNT {
        if let Some(ttl) = uri::ttl(cache_key).await {
            if ttl > 0 {
                http_bail!("账号已锁定, 请过{}后再进行登录", gen_time_desc(ttl as u32));
            } else if ttl == uri::TTL_NOT_EXPIRE {
                log::error!("redis缓存项{cache_key}没有过期时间，口令错误锁定后将无法解锁该账号");
            }
        }
    }

    Ok(())
}

/// 生成用于记录口令错误次数的键名
fn gen_login_fail_key(account: &str) -> String {
    format!("{}:{}:{}", AppConf::get().redis_pre, consts::CK_LOGIN_FAIL, account)
}

/// 根据登录账号(用户名/邮件/手机号)查找, 并校验记录状态，返回用户记录
async fn get_user_by_account(account: &str) -> Result<SysUser> {
    // 加载账号对应的记录
    let user = match SysUser::select_by_account(account).await? {
        Some(user) => user,
        None => http_bail!("账号不存在"),
    };

    // 校验账号是否有效
    if user.disabled.unwrap() != 0 {
        http_bail!("账号已被禁用");
    }

    Ok(user)
}

/// 校验登录口令
async fn check_password(account: &str, password: &str, pw_hash: &str) -> Result<()> {
    let veri_ret = utils::md5_crypt::verify(password, pw_hash).dot()?;
    // 校验口令失败
    if !veri_ret {
        let cache_key = gen_login_fail_key(account);
        let count = uri::incr(&cache_key, DISABLE_LOGIN_TTL as u64).await;
        let remainder = MAX_FAIL_COUNT - count.unwrap_or(0) as u32;

        if remainder > 0 {
            http_bail!("口令错误, 您还可以尝试{}次", remainder);
        } else {
            http_bail!(
                "账号已锁定, 请过{}后再进行登录",
                gen_time_desc(DISABLE_LOGIN_TTL)
            );
        }
    }

    Ok(())
}

/// 判断字符串是否由全数字组成
fn is_number(s: &str) -> bool {
    for c in s.as_bytes() {
        if *c < b'0' || *c > b'9' {
            return false;
        }
    }
    true
}
