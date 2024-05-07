//! 用户表接口
use crate::{
    entities::{sys_user::SysUser, PageQuery},
    services::rmq::ChannelName,
    utils::{
        md5_crypt,
        mq_util::{emit, RecChanged},
    },
};
use httpserver::{HttpContext, HttpResponse, Resp};
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

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;

    httpserver::check_required!(param, role_id, username, nickname, disabled);

    param.user_id = None;
    param.updated_time = Some(LocalTime::now());
    param.created_time = Some(LocalTime::now());

    if param.mobile.is_none() {
        param.mobile = Some("".to_owned());
    }
    if param.email.is_none() {
        param.email = Some("".to_owned());
    }
    if param.icon_id.is_none() {
        param.icon_id = Some("".to_owned());
    }

    // 对口令进行加密
    httpserver::check_required!(param, password);
    let pwd = md5_crypt::encrypt(&param.password.unwrap())?;
    param.password = Some(pwd);

    let id = SysUser::insert(param).await?.1;

    let res = SysUser {
        user_id: Some(id),
        ..Default::default()
    };
    emit(rid, ChannelName::ModUser, &RecChanged::with_insert(&res));

    Resp::ok(&res)
}

/// 更新单条记录
pub async fn update(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;

    let rid = ctx.id;
    let mut param: Req = ctx.parse_json()?;

    httpserver::check_required!(param, user_id, role_id, username, nickname, disabled);

    if param.mobile.is_none() {
        param.mobile = Some("".to_owned());
    }
    if param.email.is_none() {
        param.email = Some("".to_owned());
    }
    if param.icon_id.is_none() {
        param.icon_id = Some("".to_owned());
    }

    param.updated_time = Some(LocalTime::now());

    // 禁止更新的字段
    param.password = None;
    param.created_time = None;

    let user_id = param.user_id;
    SysUser::update_by_id_selective(param).await?;

    let res = SysUser {
        user_id,
        ..Default::default()
    };
    emit(rid, ChannelName::ModUser, &RecChanged::with_update(&res));

    Resp::ok(&res)
}

/// 删除记录
pub async fn del(ctx: HttpContext) -> HttpResponse {
    type Req = super::GetReq;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;
    SysUser::delete_by_id(param.id).await?;

    emit(
        rid,
        ChannelName::ModUser,
        &RecChanged::with_delete(&SysUser {
            user_id: Some(param.id),
            ..Default::default()
        }),
    );

    Resp::ok_with_empty()
}

/// 改变状态
pub async fn change_disabled(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;

    httpserver::check_required!(param, user_id, disabled);

    let user = SysUser {
        user_id: param.user_id,
        disabled: param.disabled,
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };

    SysUser::update_by_id_selective(user).await?;

    emit(
        rid,
        ChannelName::ModUser,
        &RecChanged::with_update(&SysUser {
            user_id: param.user_id,
            ..Default::default()
        }),
    );

    Resp::ok_with_empty()
}

/// 重置密码
pub async fn reset_password(ctx: HttpContext) -> HttpResponse {
    type Req = SysUser;
    type Res = SysUser;

    let rid = ctx.id;
    let param: Req = ctx.parse_json()?;

    httpserver::check_required!(param, user_id, password);

    let pwd = match param.password {
        Some(v) => v,
        None => md5_crypt::rand_password(8),
    };
    let enc_pwd = md5_crypt::encrypt(&pwd)?;

    let user = SysUser {
        user_id: param.user_id,
        password: Some(enc_pwd),
        updated_time: Some(LocalTime::now()),
        ..Default::default()
    };

    SysUser::update_by_id_selective(user).await?;

    emit(
        rid,
        ChannelName::ModUser,
        &RecChanged::with_update(&SysUser {
            user_id: param.user_id,
            ..Default::default()
        }),
    );

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
