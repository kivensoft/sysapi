//! 字典表接口
use std::sync::Arc;

use crate::{
    apis::UseBuilltinReq,
    entities::{
        sys_dict::{SysDict, SysDictExt},
        PageData, PageQuery,
    },
    utils::{audit, IntStr},
};
use httpserver::{
    check_required, http_bail, http_error, log_debug, HttpContext, HttpResponse, Resp,
};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysDict>;
    type Res = PageData<SysDictExt>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data: Res = SysDict::select_page(param.inner, pg).await?;
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysDict::select_by_id(param.id)
        .await?
        .ok_or(http_error!(super::REC_NOT_EXISTS.to_string()))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysDict;
    type Res = SysDict;

    let mut param: Req = ctx.parse_json()?;

    check_required!(param, dict_type, dict_code, dict_name);

    param.dict_id = None;
    let mut audit_data = param.clone();
    param.updated_time = Some(LocalTime::now());
    let mut res: Res = param.clone();

    // 写入数据库
    let id = param.insert_with_notify().await?.1;

    // 写入审计日志
    audit_data.dict_id = Some(id);
    audit::log_json(audit::DICT_ADD, ctx.user_id(), &audit_data);

    // 发送数据变更通知消息
    res.dict_id = Some(id);

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysDict;
    type Res = SysDict;

    let mut param: Req = ctx.parse_json()?;

    check_required!(param, dict_id, dict_code, dict_name);

    let orig = match SysDict::select_by_id(param.dict_id.unwrap()).await? {
        Some(r) => r,
        None => http_bail!("记录不存在"),
    };

    param.updated_time = Some(LocalTime::now());
    let audit_data = audit::diff(&param, &orig, &[SysDict::DICT_ID], &[SysDict::UPDATED_TIME]);

    // 写入数据库
    param.update_with_notify().await?;

    // 写入审计日志
    audit::log_text(audit::DICT_UPD, ctx.user_id(), audit_data);

    Resp::ok(&Res {
        dict_id: orig.dict_id,
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;

    let orig = match SysDict::select_by_id(param.id).await? {
        Some(v) => v,
        None => http_bail!("记录不存在"),
    };

    // 删除记录
    SysDict::delete_with_notify(param.id).await?;

    // 写入日志审计
    audit::log_json(audit::DICT_DEL, ctx.user_id(), &orig);


    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResponse {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u8,
    }

    #[derive(Serialize)]
    struct Res {
        pub total: usize,
        pub list: Arc<Vec<SysDict>>,
    }

    let param: Req = ctx.parse_json()?;
    let list = SysDict::select_by_type(param.dict_type).await?;

    Resp::ok(&Res{ total: list.len(), list })
}

/// 批量修改指定类型的字典项集合
pub async fn batch(ctx: HttpContext) -> HttpResponse {
    #[derive(serde::Deserialize, serde::Serialize, Clone)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u8,
        dicts: Vec<SysDict>,
    }

    let param: Req = ctx.parse_json()?;
    let audit_data = param.clone();

    // 写入数据库
    SysDict::batch_by_type(param.dict_type, param.dicts).await?;

    // 写入审计日志
    audit::log_json(audit::DICT_BAT, ctx.user_id(), &audit_data);

    Resp::ok_with_empty()
}

/// 获取权限组列表
pub async fn permission_groups(ctx: HttpContext) -> HttpResponse {
    type Req = UseBuilltinReq;
    type Res = PageData<IntStr>;

    let param = ctx.parse_json_opt::<Req>()?;
    let use_builltin = param.and_then(|v| v.use_builtin).unwrap_or(false);

    let list = SysDict::get_permission_groups(use_builltin).await?;

    Resp::ok(&Res::with_list(list))
}

/// 重新排序dict的id值使其连续
pub async fn resort(ctx: HttpContext) -> HttpResponse {
    log_debug!(ctx.id, "重新排序{}表", SysDict::TABLE_NAME);
    SysDict::resort(ctx.id).await?;
    Resp::ok_with_empty()
}
