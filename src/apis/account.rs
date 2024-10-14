//! 当前登录用户相关接口
use crate::{
    entities::{sys_menu::SysMenu, sys_role::SysRole, sys_user::SysUser},
    services::uri,
    utils::{audit, bits, md5_crypt},
    AppConf,
};
use anyhow_ext::Result;
use httpserver::{fail_if, http_bail, http_error, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::{Deserialize, Serialize};

/// 允许验证码尝试的最大次数
const MAX_FAIL_COUNT: u8 = 3;
/// 验证码发送一次的有效时长(单位: 秒)
const AUTH_CODE_TTL: u32 = 180;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthCodeInfo {
    auth_code: String,
    try_count: u8,
    created_time: u64,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LocalSysMenu<'a> {
    #[serde(flatten)]
    pub menu: &'a SysMenu,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub menus: Option<Vec<LocalSysMenu<'a>>>,
}

/// 获取当前账号配置信息
pub async fn profile(ctx: HttpContext) -> HttpResponse {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        user_id: u32,
        username: String,
        nickname: String,
    }

    let user_id = ctx.uid.parse().unwrap();
    let sys_user = SysUser::select_by_id(user_id)
        .await?
        .ok_or_else(|| http_error!("账号已被删除"))?;

    Resp::ok(&Res {
        user_id: sys_user.user_id.unwrap(),
        username: sys_user.username.unwrap(),
        nickname: sys_user.nickname.unwrap(),
    })
}

/// 获取当前账号详细配置信息(用于编辑)
pub async fn get(ctx: HttpContext) -> HttpResponse {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        user_id: u32,
        role_id: u32,
        username: String,
        nickname: String,
        mobile: Option<String>,
        email: Option<String>,
        updated_time: LocalTime,
        created_time: LocalTime,

        role_name: String,
        icon: String,
    }

    let user_id = ctx.uid.parse().unwrap();
    let sys_user = SysUser::select_by_id(user_id)
        .await?
        .ok_or_else(|| http_error!("用户[{user_id}]不存在"))?;

    let role_id = sys_user.role_id.unwrap();
    let sys_role = SysRole::select_by_id(role_id)
        .await?
        .ok_or_else(|| http_error!("角色[{role_id}]不存在"))?;

    Resp::ok(&Res {
        user_id: sys_user.user_id.unwrap(),
        role_id,
        username: sys_user.username.unwrap(),
        nickname: sys_user.nickname.unwrap(),
        mobile: sys_user.mobile,
        email: sys_user.email,
        updated_time: sys_user.updated_time.unwrap(),
        created_time: sys_user.created_time.unwrap(),

        role_name: sys_role.role_name.unwrap(),
        icon: "".to_owned(),
    })
}

/// 更新当前账号的配置信息
pub async fn update(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    struct Req {
        nickname: String,
    }

    let user_id = ctx.uid.parse().unwrap();
    let param: Req = ctx.parse_json()?;
    let sys_user = SysUser {
        user_id: Some(user_id),
        nickname: Some(param.nickname),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };

    let audit_data = sys_user.clone();
    let orig = match SysUser::select_by_id(user_id).await? {
        Some(v) => v,
        None => http_bail!("用户不存在"),
    };

    // 写入数据库
    sys_user.update_by_id().await?;

    // 写入审计日志
    audit::log_diff(
        audit::ACCOUNT_UPD,
        ctx.user_id(),
        &audit_data,
        &orig,
        &[SysUser::USER_ID],
        &[SysUser::UPDATED_TIME],
    );

    Resp::ok_with_empty()
}

/// 更改当前用户的口令
pub async fn change_password(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        old_password: String,
        new_password: String,
    }

    let user_id = ctx.uid.parse().unwrap();
    let param: Req = ctx.parse_json()?;
    let sys_user = SysUser::select_by_id(user_id)
        .await?
        .ok_or_else(|| http_error!("用户[{user_id}]不存在"))?;

    // 校验口令是否正确
    let checked = md5_crypt::verify(&param.old_password, sys_user.password.as_ref().unwrap())
        .map_err(|_| http_error!("无法校验口令"))?;
    fail_if!(!checked, "旧密码不正确");
    // 生成新的加密口令
    let pwd =
        md5_crypt::encrypt(&param.new_password).map_err(|_| http_error!("无法生成加密口令"))?;

    let sys_user = SysUser {
        user_id: Some(user_id),
        password: Some(pwd),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };
    let mut audit_data = sys_user.clone();

    // 写入数据库
    sys_user.update_by_id().await?;
    // 写入审计日志
    audit_data.updated_time = None;
    audit::log_json(audit::ACCOUNT_UPD_PASSWORD, user_id, &audit_data);

    Resp::ok_with_empty()
}

/// 更改当前用户的手机号码
pub async fn change_mobile(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        mobile: String,
        auth_code: String,
    }

    let user_id = ctx.uid.parse().unwrap();
    let param: Req = ctx.parse_json()?;

    check_auth_code(uri::CK_MOBILE_AUTH_CODE, &param.mobile, &param.auth_code).await?;

    let sys_user = SysUser {
        user_id: Some(user_id),
        mobile: Some(param.mobile),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };
    let mut audit_data = sys_user.clone();

    // 写入数据库
    sys_user.update_by_id().await?;
    // 写入审计日志
    audit_data.updated_time = None;
    audit::log_json(audit::ACCOUNT_UPD_MOBILE, user_id, &audit_data);

    Resp::ok_with_empty()
}

/// 更改当前用户的邮箱
pub async fn change_email(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        email: String,
        auth_code: String,
    }

    let user_id = ctx.uid.parse().unwrap();
    let param: Req = ctx.parse_json()?;

    check_auth_code(uri::CK_EMAIL_AUTH_CODE, &param.email, &param.auth_code).await?;

    let sys_user = SysUser {
        user_id: Some(user_id),
        email: Some(param.email),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };
    let mut audit_data = sys_user.clone();

    // 写入数据库
    sys_user.update_by_id().await?;
    // 写入审计日志
    audit_data.updated_time = None;
    audit::log_json(audit::ACCOUNT_UPD_EMAIL, user_id, &audit_data);

    Resp::ok_with_empty()
}

/// 获取当前账号允许访问的菜单树
pub async fn menus(ctx: HttpContext) -> HttpResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        client_type: i16,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res<'a> {
        menus: Vec<LocalSysMenu<'a>>,
    }

    let user_id = ctx.uid.parse().unwrap();
    let param: Req = ctx.parse_json()?;
    let user_permits = SysUser::select_permissions_by_id(user_id)
        .await?
        .ok_or_else(|| http_error!("账号对应的角色丢失"))?;

    let all_menus = SysMenu::select_by_client_type(param.client_type).await?;
    let user_menus = filter_user_menus(&all_menus, &user_permits);

    // 将扁平结构的菜单列表转成树结构
    let mut menus = convert_to_tree(user_menus);
    // 递归删除叶子节点中不带链接的菜单项
    trim_empty_menu(&mut menus);

    Resp::ok(&Res { menus })
}

async fn check_auth_code(key_type: &str, key: &str, code: &str) -> Result<()> {
    let key = format!("{}:{}:{}", AppConf::get().redis_pre, key_type, key);
    let value: String = match uri::get(&key).await {
        Some(v) => v,
        None => http_bail!("验证码已失效, 请重新发送"),
    };

    // 校验验证码是否正确
    let mut aci: AuthCodeInfo = serde_json::from_str(&value).map_err(|e| {
        log::error!("缓存反序列化错误: {e:?}");
        http_error!("缓存错误")
    })?;
    if aci.auth_code != code {
        if aci.try_count < MAX_FAIL_COUNT {
            aci.try_count += 1;
            let value = serde_json::to_string(&aci).unwrap();
            uri::set(&key, &value, AUTH_CODE_TTL as u64).await;
        } else {
            uri::del(&key).await;
        }
        http_bail!("验证码错误");
    }

    uri::del(&key).await;

    Ok(())
}

/// 根据参数permits，过滤menus，返回该permits有权限访问的菜单列表
fn filter_user_menus<'a>(menus: &'a [SysMenu], permits: &str) -> Vec<&'a SysMenu> {
    let pbs = bits::string_to_bools(permits);
    let pbs_len = pbs.len() as i16;
    let mut user_menus = Vec::new();

    for menu in menus {
        let pcode = menu.permission_code.unwrap();
        // 权限索引为负数或者对应的权限位允许, 在数组中添加菜单项
        if pcode < 0 || (pcode < pbs_len && pbs[pcode as usize]) {
            user_menus.push(menu);
        }
    }

    user_menus
}

/// 删除菜单列表中的菜单链接内容为"#"的菜单，该菜单项表示为有下级子菜单
fn trim_empty_menu(menus: &mut Vec<LocalSysMenu>) {
    // for menu in menus.iter_mut() {
    //     if let Some(children) = menu.menus.as_mut() {
    //         children.retain(|m| m.menu.menu_link.as_ref().unwrap() != "#");
    //     }
    // }
    menus.retain(|m| {
        if m.menu.menu_link.as_ref().unwrap() == "#" {
            return match &m.menus {
                Some(children) => !children.is_empty(),
                None => false,
            };
        }
        true
    });
}

/// 将扁平化的菜单列表转换为树形结构
fn convert_to_tree(menus: Vec<&SysMenu>) -> Vec<LocalSysMenu> {
    let mut tree_menus = Vec::new();

    // 将扁平结构的菜单列表转成树结构
    for menu in menus {
        let menu_code_len = match &menu.menu_code {
            Some(s) => s.len(),
            None => 0,
        };

        let new_menu = LocalSysMenu { menu, menus: None };

        match menu_code_len {
            2 => tree_menus.push(new_menu),
            4 => {
                let idx = tree_menus.len() - 1;
                let last = &mut tree_menus[idx];
                if last.menus.is_none() {
                    last.menus = Some(Vec::new());
                }
                last.menus.as_mut().unwrap().push(new_menu);
            }
            _ => {}
        }
    }

    tree_menus
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_account_get_user_menus() {
        let menus_vec = vec![
            ("01", "首页", "/", -1),
            ("02", "系统设置", "#", 0),
            ("0201", "用户管理", "/user", 0),
            ("0202", "字典管理", "/dict", 0),
            ("03", "社区管理", "#", -1),
            ("0301", "公告管理", "/board", 1),
            ("0302", "住户审核", "/check", 2),
            ("04", "车辆管理", "/car", -1),
        ];

        let mut menus = Vec::new();
        for menu in &menus_vec {
            let item = super::SysMenu {
                menu_code: Some(menu.0.to_string()),
                menu_name: Some(menu.1.to_string()),
                menu_link: Some(menu.2.to_string()),
                permission_code: Some(menu.3),
                ..Default::default()
            };
            menus.push(item);
        }

        // 测试权限过滤模式
        let user_menus = super::filter_user_menus(&menus, "80");
        let check_data = ["01", "02", "0201", "0202", "03", "04"];
        for i in 0..check_data.len() {
            assert_eq!(check_data[i], user_menus[i].menu_code.as_ref().unwrap());
        }

        // 测试菜单树化
        let user_menus = super::convert_to_tree(user_menus);
        let check_data = ["01", "02", "03", "04"];
        for i in 0..check_data.len() {
            assert_eq!(
                check_data[i],
                user_menus[i].menu.menu_code.as_ref().unwrap()
            );
        }
        let sub_menus = &user_menus[1].menus.as_ref().unwrap();
        let check_data = ["0201", "0202"];
        for i in 0..check_data.len() {
            assert_eq!(check_data[i], sub_menus[i].menu.menu_code.as_ref().unwrap());
        }

        // 测试权限过滤模式
        let user_menus = super::filter_user_menus(&menus, "c0");
        let check_data = ["01", "02", "0201", "0202", "03", "0301", "04"];
        for i in 0..check_data.len() {
            assert_eq!(check_data[i], user_menus[i].menu_code.as_ref().unwrap());
        }

        // 测试菜单树化
        let user_menus = super::convert_to_tree(user_menus);
        let check_data = ["01", "02", "03", "04"];
        for i in 0..check_data.len() {
            assert_eq!(
                check_data[i],
                user_menus[i].menu.menu_code.as_ref().unwrap()
            );
        }
        let sub_menus = &user_menus[1].menus.as_ref().unwrap();
        let check_data = ["0201", "0202"];
        for i in 0..check_data.len() {
            assert_eq!(check_data[i], sub_menus[i].menu.menu_code.as_ref().unwrap());
        }
        let sub_menus = &user_menus[2].menus.as_ref().unwrap();
        let check_data = ["0301"];
        for i in 0..check_data.len() {
            assert_eq!(check_data[i], sub_menus[i].menu.menu_code.as_ref().unwrap());
        }
    }
}
