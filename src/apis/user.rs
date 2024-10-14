//! 用户表接口
use std::borrow::Cow;

use crate::{
    entities::{
        sys_config::SysConfig,
        sys_user::{DisabledType, SysUser},
        PageQuery,
    },
    utils::{audit, consts, md5_crypt},
};
use anyhow_ext::{Context, Result};
use httpserver::{http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysUser>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysUser::select_page(param.inner, pg).await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysUser::select_by_id(param.id).await?;

    match rec {
        Some(mut rec) => {
            rec.password = None;
            Resp::ok(&rec)
        }
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;

    let mut param: Req = ctx.parse_json()?;

    httpserver::check_required!(param, role_id, username);

    param.user_id = None;

    // 可选字段设置默认值
    if param.disabled.is_none() {
        param.disabled = Some(DisabledType::Normal as u8);
    }
    if param.nickname.is_none() {
        param.nickname = param.username.clone();
    }

    // 如果用户未指定密码，则使用缺省密码
    let password = match &param.password {
        Some(pwd) => Cow::Borrowed(pwd),
        None => Cow::Owned(get_default_password().await?),
    };
    // 对口令进行加密
    param.password = Some(md5_crypt::encrypt(&password)?);

    // 必填字段缺省值
    param.updated_time = Some(LocalTime::now());
    param.created_time = Some(LocalTime::now());

    let mut audit_data = param.clone();

    // 写入数据库
    let id = param.insert_with_notify().await?.1;

    // 写入审计日志
    audit_data.user_id = Some(id);
    audit_data.created_time = None;
    audit_data.updated_time = None;
    audit::log_json(audit::USER_ADD, ctx.user_id(), &audit_data);

    Resp::ok(&SysUser {
        user_id: Some(id),
        ..Default::default()
    })
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;

    let mut param: Req = ctx.parse_json()?;

    httpserver::check_required!(param, user_id);

    // 加载原有数据，日志审计需要使用
    let orig = SysUser::select_by_id(param.user_id.unwrap()).await?;
    let orig = match orig {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };

    // 禁止更新的字段
    param.created_time = None;

    // 自动更新字段
    param.updated_time = Some(LocalTime::now());

    // 动态计算的字段
    if let Some(pw) = &param.password {
        if !pw.is_empty() {
            param.password = Some(md5_crypt::encrypt(pw)?);
        }
    }

    // 生成审计日志的内容
    let audit_data = audit::diff(
        &param,
        &orig,
        &[SysUser::USER_ID],
        &[SysUser::CREATED_TIME, SysUser::UPDATED_TIME],
    )
    .dot();

    // 写入数据库
    param.update_with_notify().await?;

    // 写入审计日志
    audit::log_text(audit::USER_UPD, ctx.user_id(), audit_data);

    Resp::ok(&SysUser {
        user_id: orig.user_id,
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let audit_data = match SysUser::select_by_id(param.id).await.dot()? {
        Some(v) => v,
        None => http_bail!("用户不存在"),
    };

    SysUser::delete_with_notify(param.id).await?;
    // 写入审计日志
    audit::log_json(audit::USER_DEL, ctx.user_id(), &audit_data);

    Resp::ok_with_empty()
}

/// 改变状态
pub async fn disable(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;

    let param: Req = ctx.parse_json()?;
    httpserver::check_required!(param, user_id, disabled);

    let user = SysUser {
        user_id: param.user_id,
        disabled: param.disabled,
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };
    let mut audit_data = user.clone();

    // 写入数据库
    SysUser::update_with_notify(user).await?;

    // 写入审计日志
    audit_data.updated_time = None;
    audit::log_json(audit::USER_UPD_STATUS, ctx.user_id(), &audit_data);

    Resp::ok_with_empty()
}

/// 重置密码
pub async fn resetpw(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;
    type Res = SysUser;

    let param: Req = ctx.parse_json()?;
    httpserver::check_required!(param, user_id);

    let pwd = match param.password {
        Some(v) => v,
        None => get_default_password().await?,
    };
    // 加密后的口令
    let enc_pwd = md5_crypt::encrypt(&pwd)?;

    let user = SysUser {
        user_id: param.user_id,
        password: Some(enc_pwd),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };
    let mut audit_data = user.clone();

    // 写入数据库
    SysUser::update_with_notify(user).await?;

    // 记录审计日志
    audit_data.updated_time = None;
    audit::log_json(audit::USER_UPD_PASSWORD, ctx.user_id(), &audit_data);

    Resp::ok(&Res {
        password: Some(pwd),
        ..Default::default()
    })
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        users: Vec<SysUser>,
    }

    let param: Req = ctx.parse_json()?;
    let recs = SysUser::select_by(param).await?;

    Resp::ok(&Res { users: recs })
}

// 从数据库配置表中加载默认密码
async fn get_default_password() -> Result<String> {
    let pw = SysConfig::get_value(consts::cfg::CK_DEFAULT_PASSWORD).await?;
    Ok(pw.map_or(String::from(consts::DEFAULT_PASSWORD), |v| (*v).clone()))
}
