use anyhow_ext::{Context, Result};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::entities::sys_log::SysLog;

// #region 审计类型常量定义
pub const LOGIN: &str = "登录";
pub const LOGOUT: &str = "登出";
pub const REFRESH: &str = "更新令牌";

pub const ACCOUNT_UPD: &str = "登录用户:修改";
pub const ACCOUNT_UPD_PASSWORD: &str = "登录用户:改密";
pub const ACCOUNT_UPD_MOBILE: &str = "登录用户:改手机";
pub const ACCOUNT_UPD_EMAIL: &str = "登录用户:改邮箱";

pub const USER_ADD: &str = "用户:添加";
pub const USER_UPD: &str = "用户:修改";
pub const USER_DEL: &str = "用户:删除";
pub const USER_UPD_STATUS: &str = "用户:改状态";
pub const USER_UPD_PASSWORD: &str = "用户:改密";

pub const CONFIG_ADD: &str = "配置:添加";
pub const CONFIG_UPD: &str = "配置:修改";
pub const CONFIG_DEL: &str = "配置:删除";

pub const DICT_ADD: &str = "字典:添加";
pub const DICT_UPD: &str = "字典:修改";
pub const DICT_DEL: &str = "字典:删除";
pub const DICT_BAT: &str = "字典:批量排序";

pub const PERMISSIONS_ADD: &str = "权限:添加";
pub const PERMISSIONS_UPD: &str = "权限:修改";
pub const PERMISSIONS_DEL: &str = "权限:删除";
pub const PERMISSIONS_REARRANGE: &str = "权限:重排";

pub const API_ADD: &str = "接口:添加";
pub const API_UPD: &str = "接口:修改";
pub const API_DEL: &str = "接口:删除";
pub const API_REARRANGE: &str = "接口:排序";

pub const MENU_ADD: &str = "菜单:添加";
pub const MENU_UPD: &str = "菜单:修改";
pub const MENU_DEL: &str = "菜单:删除";
pub const MENU_REARRANGE: &str = "菜单:排序";

pub const ROLE_ADD: &str = "角色:添加";
pub const ROLE_UPD: &str = "角色:修改";
pub const ROLE_DEL: &str = "角色:删除";
// #endregion

type Attrs<'a> = &'a [&'a str];

/// 将指定的审计日志信息追加到系统审计日志中。
///
/// ### 参数
/// - `category`: 日志的分类，用于区分不同类型的日志
/// - `user_id`: 用户ID，用于标识进行操作的用户
/// - `value`: 字符串类型，表示需要记录的日志内容。
///
pub fn log(category: &str, user_id: u32, value: String) {
    _log(category, user_id, value);
}

/// 将指定的审计日志信息追加到系统审计日志中。
///
/// ### 参数
/// - `category`: 日志的分类，用于区分不同类型的日志
/// - `user_id`: 用户ID，用于标识进行操作的用户
/// - `text`: 表示需要记录的日志内容, 如果是Ok(s)则记录，如果是Err(e)则输出错误日志。
///
pub fn log_text(category: &str, user_id: u32, text: Result<String>) {
    match text.dot() {
        Ok(text) => log(category, user_id, text),
        Err(e) => log::error!("audit::log 写入审计日志错误: {e:?}"),
    }
}

/// 记录用户操作的审计日志，将操作内容`value`以JSON格式的日志记录
///
/// 本函数接收一个类别名、用户ID和一个任意类型的值，将该值序列化为JSON格式的字符串后进行记录
/// 使用了泛型<T>以支持任意可序列化为JSON的类型
///
/// ### 参数
/// - `category`: 日志的分类，用于区分不同类型的日志
/// - `user_id`: 用户ID，用于标识进行操作的用户
/// - `value`: &T类型，表示要记录的值，T必须实现Serialize trait以支持序列化
///
/// ### 说明
/// - 使用`serde_json::to_string`函数将`value`参数序列化为JSON格式的字符串
pub fn log_json<T: Serialize>(category: &str, user_id: u32, value: &T) {
    log_text(category, user_id, to_text(value));
}

/// 记录用户操作的审计日志，通过对比当前对象和原始对象的值，只记录变化的值
///
/// ### 类型参数
/// - `T`: 实现了序列化和差异比较的类型
///
/// ### 参数
/// - `category`: 日志的分类，用于区分不同类型的日志
/// - `user_id`: 用户ID，用于标识进行操作的用户
/// - `value`: 当前值的引用，用于计算差异
/// - `origin`: 原始值的引用，用于计算差异
///
pub fn log_diff<T: Serialize>(
    category: &str,
    user_id: u32,
    value: &T,
    origin: &T,
    require: Attrs,
    ignore: Attrs,
) {
    log_text(category, user_id, diff(value, origin, require, ignore));
}

/// 比较两个值的差异，并返回差异的JSON表示
///
/// ### 泛型参数
/// - `T`: 实现了`Serialize` trait的类型，用于序列化为JSON字符串。
///
/// ### 参数
/// - `value`: 要比较的第一个值的引用
/// - `origin`: 要比较的第二个值的引用
/// - `require`: 必须记录的字段数组
/// - `ignore`: 必须忽略的字段数组
///
/// ### 返回
/// 返回一个`Result`，其中包含差异的JSON表示（成功时）或序列化错误（失败时）
///
pub fn diff<T: Serialize>(value: &T, origin: &T, require: Attrs, ignore: Attrs) -> Result<String> {
    let src = to_text(value).dot()?;
    let dst = to_text(origin).dot()?;
    _diff(&src, &dst, require, ignore)
}

/// 将任意可序列化类型转换为JSON格式的字符串，如果序列化错误则记录源代码行号
pub fn to_text<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("json序列化错误").dot()
}

/// 将字符串转从蛇形命名转换为驼峰命名
///
/// 此函数接收一个字符串切片`value`和一个布尔值`capitalize_first`，返回一个转换为驼峰命名的`CompactString`。
/// 参数`capitalize_first`用于指定第一个字符是否大写。
///
/// # 参数
/// - `value`: &str，需要转换的字符串切片。
/// - `capitalize_first`: bool，如果为true，则第一个字符大写；如果为false，则第一个字符小写。
///
/// # 返回值
/// - `CompactString`，转换为驼峰命名的字符串。
///
/// # 示例
/// ```rust
/// let result = snake_case_to_camel_case("hello_world", true);
/// assert_eq!(result, "HelloWorld");
/// ```
/// ```rust
/// let result = snake_case_to_camel_case("hello_world", false);
/// assert_eq!(result, "helloWorld");
/// ```
pub fn snake_case_to_camel_case(value: &str, capitalize_first: bool) -> String {
    let mut result = String::with_capacity(value.len());
    let mut capitalize_next = capitalize_first;

    for ch in value.chars() {
        if '_' == ch {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }

    result
}

fn _diff(src: &str, dst: &str, require: Attrs, ignore: Attrs) -> Result<String> {
    let src_val: Value = serde_json::from_str(src)
        .context("json反序列化错误")
        .dot()?;
    let dst_val: Value = serde_json::from_str(dst)
        .context("json反序列化错误")
        .dot()?;
    let require: Vec<_> = require
        .iter()
        .map(|v| snake_case_to_camel_case(v, false))
        .collect();
    let ignore: Vec<_> = ignore
        .iter()
        .map(|v| snake_case_to_camel_case(v, false))
        .collect();

    let mut mod_val = Map::new();
    if let Value::Object(src_map) = src_val {
        if let Value::Object(dst_map) = dst_val {
            for (k, v) in src_map.into_iter() {
                // 必须记录的字段
                if require.iter().any(|i| i.as_str() == k.as_str()) {
                    mod_val.insert(k, v);
                }
                // 不是必须忽略的字段
                else if !ignore.iter().any(|i| i.as_str() == k.as_str()) {
                    // 原始值存在且与当前值不同, 则记录这个字段
                    if let Some(v2) = dst_map.get(&k) {
                        if v != *v2 {
                            mod_val.insert(k, v);
                        }
                    }
                }
            }
        }
    }

    let mod_val = Value::Object(mod_val);
    let result = serde_json::to_string(&mod_val)
        .context("json序列化错误")
        .dot()?;

    Ok(result)
}

// -------------------- 对接外部实际实现的接口 --------------------

/// 尝试将日志信息追加到系统日志中，如果出现错误则记录错误信息
fn _log(category: &str, user_id: u32, value: String) {
    let category = category.to_string();
    tokio::spawn(async move {
        if let Err(e) = SysLog::append(category, user_id, value).await.dot() {
            log::error!("audit::log 写入审计日志错误: {e:?}");
        }
    });
}
