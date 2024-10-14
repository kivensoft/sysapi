//! 系统配置接口
use crate::{entities::{sys_config::SysConfig, PageQuery}, utils::audit};
use anyhow_ext::anyhow;
use httpserver::{check_required, http_bail, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysConfig>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysConfig::select_page(param.inner, pg).await?;

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysConfig::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysConfig;

    let mut param: Req = ctx.parse_json()?;

    check_required!(param, category, cfg_name, cfg_value);

    param.cfg_id = None;
    param.updated_time = Some(LocalTime::now());

    let mut audit_data = param.clone();

    // 写入数据库
    let id = param.insert_with_notify().await?.1;

    // 写入审计日志
    audit_data.cfg_id = Some(id);
    audit_data.updated_time = None;
    audit::log_json(audit::CONFIG_ADD, ctx.user_id(), &audit_data);

    let res = SysConfig {
        cfg_id: Some(id),
        ..Default::default()
    };

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysConfig;

    let mut param: Req = ctx.parse_json()?;

    check_required!(param, cfg_id, cfg_name, cfg_value);

    let audit_data = param.clone();
    let orig = match SysConfig::select_by_id(param.cfg_id.unwrap()).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };

    param.updated_time = Some(LocalTime::now());
    let audit_data = audit::diff(
        &audit_data,
        &orig,
        &[SysConfig::CFG_ID],
        &[SysConfig::UPDATED_TIME],
    );

    // 写入数据库
    param.update_with_notify().await?;

    // 写入审计日志
    audit::log_text(audit::CONFIG_UPD, ctx.user_id(), audit_data);

    // 发送配置变更消息通知
    let res = SysConfig {
        cfg_id: orig.cfg_id,
        ..Default::default()
    };

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;

    let orig = match SysConfig::select_by_id(param.id).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };

    // 写入数据库
    SysConfig::delete_with_notify(param.id).await?;

    // 写入日志审计
    audit::log_json(audit::CONFIG_DEL, ctx.user_id(), &orig);

    Resp::ok_with_empty()
}
