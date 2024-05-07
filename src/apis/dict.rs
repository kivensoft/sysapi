//! 字典表接口
use crate::{
    entities::{sys_dict::SysDict, PageData, PageQuery},
    services::rmq::ChannelName,
    utils::mq_util::{emit, RecChanged},
};
use anyhow_ext::anyhow;
use httpserver::{check_required, HttpContext, HttpResponse, Resp};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResponse {
    type Req = PageQuery<SysDict>;

    let param: Req = ctx.parse_json()?;
    let pg = param.page_info();
    let page_data = SysDict::select_page(param.inner, pg)
        .await?;
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let param: Req = ctx.parse_json()?;
    let rec = SysDict::select_by_id(param.id)
        .await?
        .ok_or(anyhow!(super::REC_NOT_EXISTS))?;

    Resp::ok(&rec)
}

/// 添加单条记录
pub async fn insert(ctx: HttpContext) -> HttpResponse {
    type Req = SysDict;
    type Res = SysDict;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;

    check_required!(param, dict_type, dict_code, dict_name);

    param.dict_id = None;
    param.updated_time = Some(LocalTime::now());

    // 新增记录需要设置dict_code的值为所属类别中的最大值 + 1
    let max_code = SysDict::select_max_code(param.dict_type.unwrap())
        .await?;
    param.dict_code = Some(max_code.map_or(0, |v| v + 1));

    let id = SysDict::insert(param).await?.1;

    let res = SysDict {
        dict_id: Some(id),
        ..Default::default()
    };
    emit(rid, ChannelName::ModDict, &RecChanged::with_insert(&res));

    Resp::ok(&Res {
        dict_id: Some(id),
        ..Default::default()
    })
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysDict;
    type Res = SysDict;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;
    let dict_id = param.dict_id;

    check_required!(param, dict_id, dict_type, dict_code, dict_name);

    param.updated_time = Some(LocalTime::now());

    SysDict::update_by_id(param).await?;

    let res = SysDict {
        dict_id,
        ..Default::default()
    };
    emit(rid, ChannelName::ModDict, &RecChanged::with_update(&res));

    Resp::ok(&Res {
        dict_id,
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysDict::delete_by_id(param.id).await?;

    let ev_data = SysDict {
        dict_id: Some(param.id),
        ..Default::default()
    };
    emit(
        rid,
        ChannelName::ModDict,
        &RecChanged::with_delete(&ev_data),
    );

    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResponse {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u16,
    }

    type Res = PageData<SysDict>;

    let param: Req = ctx.parse_json()?;
    let list = SysDict::select_by_type(param.dict_type)
        .await?;

    Resp::ok(&Res {
        total: list.len() as u32,
        list,
    })
}

/// 批量修改指定类型的字典项集合
pub async fn batch(ctx: HttpContext) -> HttpResponse {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u16,
        dict_names: Vec<String>,
    }

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysDict::batch_update_by_type(param.dict_type, &param.dict_names)
        .await?;

    emit(rid, ChannelName::ModDict, &RecChanged::<()>::with_all());

    Resp::ok_with_empty()
}
