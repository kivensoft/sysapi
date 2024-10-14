pub const DEFAULT_PASSWORD: &str = "sysapi123456";
pub const TOKEN_KEY: &str = "token";
pub const INVALID_TOKEN_KEY: &str = "invalid:token";
pub const CK_LOGIN_FAIL: &str = "login:fail";
pub const CC_LOGIN: &str = "login";
pub const CC_LOGOUT: &str = "logout";

pub mod cfg {
    pub const CK_DEFAULT_PASSWORD: &str = "缺省口令";
    pub const MEM_CACHE_EXPIRE: u64 = 300;
    pub const REDIS_EXPIRE: u64 = 600;
}

pub mod gmc {
    pub const MOD_KEY: &str = "mod";
    pub const TABLE_KEY: &str = "table";
    pub const SYS_API: &str = "sys:api";
    pub const SYS_CONFIG: &str = "sys:config";
    pub const SYS_DICT: &str = "sys:dict";
    pub const SYS_MENU: &str = "sys:menu";
    pub const SYS_PERMISSION: &str = "sys:permission";
    pub const SYS_ROLE: &str = "sys:role";
    pub const SYS_USER: &str = "sys:user";
}
