//! 系统字典表接口

use crate::{db::{PageQuery, sys_dict::SysDict}, services::rmq};
use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysDict>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysDict::select_page(param.data(), param.page()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysDict::select_by_id(param.id).await;

    match check_result!(rec) {
        Some(rec) => Resp::ok(&rec),
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysDict;
    type Res = SysDict;

    let mut param: Req = ctx.into_json().await?;

    check_required!(param, dict_type, dict_code, dict_name);

    param.updated_time = Some(LocalTime::now());

    // 新增记录需要设置dict_code的值为所属类别中的最大值 + 1
    if param.dict_id.is_none() {
        let max_code = SysDict::select_max_code(param.dict_type.unwrap()).await;
        param.dict_code = Some(check_result!(max_code).map_or(0, |v| v + 1));
    }

    let id = check_result!(match param.dict_id {
        Some(id) => SysDict::update_by_id(&param).await.map(|_| id),
        None => SysDict::insert(&param).await.map(|(_, id)| id),
    });

    tokio::spawn(async move {
        let msg = SysDict { dict_id: Some(id), ..Default::default() };
        let chan = rmq::make_channel(rmq::ChannelName::ModDict);
        let op = match param.dict_id {
            Some(_) => rmq::RecChanged::publish_update(&chan, msg).await,
            None => rmq::RecChanged::publish_insert(&chan, msg).await,
        };
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });


    Resp::ok( &Res {
        dict_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let r = SysDict::delete_by_id(param.id).await;
    check_result!(r);

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::ModDict);
        let op = rmq::RecChanged::publish_delete(&chan, SysDict {
            dict_id: Some(param.id),
            ..Default::default()
        }).await;
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResult {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u16,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        dicts: Vec<SysDict>,
    }

    let param: Req = ctx.into_json().await?;
    let rec = SysDict::select_by_type(param.dict_type).await;
    let rec = check_result!(rec);

    Resp::ok(&Res { dicts: rec })
}

/// 批量修改指定类型的字典项集合
pub async fn batch(ctx: HttpContext) -> HttpResult {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u16,
        dict_names: Vec<String>,
    }

    let param: Req = ctx.into_json().await?;
    check_result!(SysDict::batch_update_by_type(param.dict_type, &param.dict_names).await);

    Resp::ok_with_empty()
}
