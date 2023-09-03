//! 用户表接口

use crate::{db::{PageQuery, sys_user::SysUser}, utils::unix_crypt, services::rmq};
use httpserver::{HttpContext, Resp, HttpResult, check_result};
use localtime::LocalTime;
use serde::Serialize;

/// 记录列表
pub async fn list(ctx: HttpContext) -> HttpResult {
    type Req = PageQuery<SysUser>;

    let param: Req = ctx.into_json().await?;
    let page_data = SysUser::select_page(param.data(), param.page()).await;
    let mut page_data = check_result!(page_data);

    // 剔除不需要返回的字段
    for item in page_data.list.iter_mut() {
        item.password = None;
    }

    Resp::ok(&page_data)
}

/// 获取单条记录
pub async fn get(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let rec = SysUser::select_by_id(param.id).await;

    match check_result!(rec) {
        Some(mut rec) => {
            rec.password = None;
            Resp::ok(&rec)
        }
        None => Resp::fail(super::REC_NOT_EXISTS),
    }
}

/// 更新单条记录
pub async fn post(ctx: HttpContext) -> HttpResult {
    type Req = SysUser;
    type Res = SysUser;

    let mut param: Req = ctx.into_json().await?;

    httpserver::check_required!(param, role_id, username, nickname, disabled);

    if param.mobile.is_none() { param.mobile = Some("".to_owned()); }
    if param.email.is_none() { param.email = Some("".to_owned()); }
    if param.icon_id.is_none() { param.icon_id = Some("".to_owned()); }

    param.updated_time = Some(LocalTime::now());

    let id = check_result!(match param.user_id {
        Some(id) => {
            // 禁止更新的字段
            param.password = None;
            param.created_time = None;

            SysUser::update_dyn_by_id(&param).await.map(|_| id)
        }
        None => {
            // 对口令进行加密
            httpserver::check_required!(param, password);
            let pwd = check_result!(unix_crypt::encrypt(&param.password.unwrap()));
            param.password = Some(pwd);

            param.created_time = Some(LocalTime::now());

            SysUser::insert(&param).await.map(|(_, id)| id)
        }
    });

    tokio::spawn(async move {
        let msg = SysUser { user_id: Some(id), ..Default::default() };
        let chan = rmq::make_channel(rmq::ChannelName::ModUser);
        let op = match param.user_id {
            Some(_) => rmq::RecChanged::publish_update(&chan, msg).await,
            None => rmq::RecChanged::publish_insert(&chan, msg).await,
        };
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });


    Resp::ok( &Res {
        user_id: Some(id),
        ..Default::default()
    })
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResult {
    type Req = super::GetReq;

    let param: Req = ctx.into_json().await?;
    let op = SysUser::delete_by_id(param.id).await;
    check_result!(op);

    tokio::spawn(async move {
        let chan = rmq::make_channel(rmq::ChannelName::ModUser);
        let op = rmq::RecChanged::publish_delete(&chan, SysUser {
            user_id: Some(param.id),
            ..Default::default()
        }).await;
        if let Err(e) = op {
            log::error!("redis发布消息失败: {e:?}");
        }
    });

    Resp::ok_with_empty()
}

/// 改变状态
pub async fn change_disabled(ctx: HttpContext) -> HttpResult {
    type Req = SysUser;

    let param: Req = ctx.into_json().await?;

    httpserver::check_required!(param, user_id, disabled);

    let user = SysUser {
        user_id: param.user_id,
        disabled: param.disabled,
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };

    SysUser::update_dyn_by_id(&user).await?;

    Resp::ok_with_empty()
}

/// 重置密码
pub async fn reset_password(ctx: HttpContext) -> HttpResult {
    type Req = SysUser;
    type Res = SysUser;

    let param: Req = ctx.into_json().await?;

    httpserver::check_required!(param, user_id, password);

    let pwd = match param.password {
        Some(v) => v,
        None => unix_crypt::rand_password(8),
    };
    let enc_pwd = unix_crypt::encrypt(&pwd)?;

    let user = SysUser {
        user_id: param.user_id,
        password: Some(enc_pwd),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };

    SysUser::update_dyn_by_id(&user).await?;

    Resp::ok( &Res {
        password: Some(pwd),
        ..Default::default()
    })
}

/// 获取指定类别的所有字典项
pub async fn items(ctx: HttpContext) -> HttpResult {
    type Req = SysUser;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Res {
        users: Vec<SysUser>,
    }

    let param: Req = ctx.into_json().await?;
    let recs = check_result!(SysUser::select_by(&param).await);

    Resp::ok(&Res { users: recs })
}
