//! 系统字典表接口

use crate::{entities::{PageQuery, sys_dict::SysDict, PageData}, services::rmq};
use httpserver::{HttpContext, Resp, HttpResult, check_required, check_result};
use localtime::LocalTime;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysDict>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysDict::select_page(param.data(), param.to_page_info()).await;
    let page_data = check_result!(page_data);
    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysDict::select_by_id(&param.id).await;

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

    let typ = match param.dict_id {
        Some(_) => rmq::RecordChangedType::Update,
        None => rmq::RecordChangedType::Insert,
    };
    rmq::publish_rec_change_spawm(rmq::ChannelName::ModDict, typ, SysDict {
        dict_id: Some(id),
        ..Default::default()
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
    let r = SysDict::delete_by_id(&param.id).await;
    check_result!(r);

    rmq::publish_rec_change_spawm(rmq::ChannelName::ModDict,
        rmq::RecordChangedType::Delete,
        SysDict {
            dict_id: Some(param.id),
            ..Default::default()
        }
    );

    Resp::ok_with_empty()
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResult {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Req {
        dict_type: u16,
    }

    type Res = PageData<SysDict>;

    let param: Req = ctx.into_json().await?;
    let rec = SysDict::select_by_type(param.dict_type).await;
    let list = check_result!(rec);

    Resp::ok(&Res { total: list.len() as u32, list, })
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

    rmq::publish_rec_change_spawm::<Option<()>>(rmq::ChannelName::ModDict,
        rmq::RecordChangedType::All, None,);

    Resp::ok_with_empty()
}
