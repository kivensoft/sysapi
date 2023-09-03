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

/// 获取账号名类型（登录名/邮箱/手机号）
pub fn check_account_type(account: &str) -> AccountType {
    if account.len() == 11 {
        let mut is_number = true;
        for c in account.as_bytes() {
            if *c < b'0' || *c > b'9' {
                is_number = false;
                break;
            }
        }
        if is_number {
            return AccountType::Mobile;
        }
    }

    if account.find('@').is_some() {
        return AccountType::Email;
    }
    AccountType::Username
}
