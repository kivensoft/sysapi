//! 实用工具函数单元

#[allow(dead_code)]
pub mod bits;
#[allow(dead_code)]
pub mod time;
pub mod unix_crypt;

pub enum AccountType {
    Username,
    Email,
    Mobile,
}

pub const PUBLIC_PERMIT_CODE: i16 = -1;
pub const ANONYMOUS_PERMIT_CODE: i16 = -2;
pub const INNER_GROUP_CODE: i16 = -1;

pub const PUBLIC_PERMIT_NAME: &str = "公共许可";
pub const ANONYMOUS_PERMIT_NAME: &str = "匿名许可";
pub const INNER_GROUP_NAME: &str = "内置权限";

/// 获取账号名类型（登录名/邮箱/手机号）
pub fn check_account_type(account: &str) -> AccountType {
    if account.len() == 11 && is_number(account) {
        return AccountType::Mobile;
    }

    if account.find('@').is_some() {
        return AccountType::Email;
    }
    AccountType::Username
}

/// 判断字符串是否由全数字组成
fn is_number(s: &str) -> bool {
    for c in s.as_bytes() {
        if *c < b'0' || *c > b'9' {
            return false;
        }
    }
    return true;
}
