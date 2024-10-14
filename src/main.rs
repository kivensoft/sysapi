//! 主程序单元
#[macro_use]
mod utils;
mod apis;
mod auth;
mod entities;
mod services;

use std::{fmt::Write, time::Duration};

use anyhow_ext::{Context, Result};
use futures::SinkExt;
use gensql::DbConfig;
use httpserver::{if_else, log_debug, HttpServer};
use localtime::LocalTime;
use smallstr::SmallString;
use utils::consts;

const BANNER: &str = r#"
   _____ Kivensoft  ___  ?       _
  / ___/__  _______/   |  ____  (_)
  \__ \/ / / / ___/ /| | / __ \/ /
 ___/ / /_/ (__  ) ___ |/ /_/ / /
/____/\__, /____/_/  |_/ .___/_/
     /____/           /_/
"#;

const APP_NAME: &str = include_str!(concat!(env!("OUT_DIR"), "/.app_name"));

/// app版本号, 来自编译时由build.rs从cargo.toml中读取的版本号(读取内容写入.version文件)
const APP_VER: &str = include_str!(concat!(env!("OUT_DIR"), "/.version"));

const CONTEXT_PATH: &str = "/api"; // 接口请求的上下文路径
const TOKEN_CACHE_TASK_INTERVAL: u64 = 600;

appcfg::appglobal_define! {app_global, AppGlobal,
    startup_time: i64,
    jwt_ttl: u32,
}

appcfg::appconfig_define! {app_conf, AppConf,
    log_level       : String => ["L",  "log-level",        "LogLevel",          "日志级别(trace/debug/info/warn/error/off)"],
    log_file        : String => ["F",  "log-file",         "LogFile",           "日志的相对路径或绝对路径文件名"],
    log_max         : String => ["M",  "log-max",          "LogFileMaxSize",    "日志文件的最大长度 (单位: k|m|g)"],
    log_async       : bool   => ["",   "log-async",        "",                  "启用异步日志"],
    no_console      : bool   => ["",   "no-console",       "",                  "禁止将日志输出到控制台"],
    threads         : String => ["t",  "threads",          "Threads",           "设置应用的线程数"],
    listen          : String => ["l",  "listen",           "Listen",            "服务监听端点 (ip地址:端口号)"],
    gateway         : String => ["g",  "gateway",          "Gateway",           "api网关端点 (ip地址:端口号)"],
    reg_interval    : String => ["",   "reg_interval",     "RegInterval",       "服务注册心跳保持时间 (单位: 秒)"],
    token_cache_size: String => ["",   "token-cache-size", "tokenCacheSize",    "令牌缓存大小"],
    server_id       : String => ["",   "server-id",        "ServerId",          "微服务id, 多个相同的微服务，设置不同的id加以区分"],
    db_host         : String => ["",   "db-host",          "DbHost",            "数据库服务主机名"],
    db_port         : String => ["",   "db-port",          "DbPort",            "数据库服务端口"],
    db_user         : String => ["",   "db-user",          "DbUser",            "数据库用户名"],
    db_pass         : String => ["",   "db-pass",          "DbPass",            "数据库密码"],
    db_name         : String => ["",   "db-name",          "DbName",            "数据库名称"],
    redis_host      : String => ["",   "redis-host",       "RedisHost",         "缓存服务主机名"],
    redis_port      : String => ["",   "redis-port",       "RedisPort",         "缓存服务端口"],
    redis_user      : String => ["",   "redis-user",       "RedisUser",         "缓存服务用户名"],
    redis_pass      : String => ["",   "redis-pass",       "RedisPass",         "缓存服务口令"],
    redis_name      : String => ["",   "redis-name",       "RedisName",         "缓存服务数据库名称"],
    redis_pre       : String => ["",   "redis-pre",        "RedisPre",          "缓存项前缀"],
    jwt_key         : String => ["",   "jwt-key",          "JwtKey",            "令牌密钥"],
    jwt_iss         : String => ["",   "jwt-iss",          "JwtIss",            "令牌发行者"],
    jwt_ttl         : String => ["",   "jwt-ttl",          "JwtTtl",            "令牌存活时间 (单位: 分钟)"],
    jwt_refresh     : String => ["",   "jwt-refresh",      "JwtRefresh",        "刷新令牌的密钥"],
}

impl Default for AppConf {
    fn default() -> Self {
        Self {
            log_level: String::from("info"),
            log_file: String::with_capacity(0),
            log_max: String::from("10m"),
            log_async: false,
            no_console: false,
            threads: String::from("2"),
            listen: String::from("127.0.0.1:6401"),
            gateway: String::new(),
            reg_interval: String::from("30"),
            token_cache_size: String::from("256"),
            server_id: String::from("1"),
            db_host: String::from("127.0.0.1"),
            db_port: String::from("3306"),
            db_user: String::from("root"),
            db_pass: String::from("password"),
            db_name: String::new(),
            redis_host: String::from("127.0.0.1"),
            redis_port: String::from("6379"),
            redis_user: String::new(),
            redis_pass: String::from("password"),
            redis_name: String::from("0"),
            redis_pre: String::from("sysapi"),
            jwt_key: String::from("SysApi CopyRight by kivensoft 2023-05-04"),
            jwt_iss: String::from("SysApi"),
            jwt_ttl: String::from("1440"),
            jwt_refresh: String::from("SysApi copyright kivensoft 2023-09-27"),
            // db_url:       String::from("mysql://root:password@127.0.0.1:3306/exampledb?characterEncoding=UTF-8&useSSL=false&serverTimezone=GMT%2B8"),
            // cache_url:    String::from("redis://:password@127.0.0.1/0"),
        }
    }
}

macro_rules! arg_err {
    ($text:literal) => {
        concat!("参数 ", $text, " 格式错误")
    };
}

/// 初始化命令行参数和配置文件
fn init() -> Option<(&'static mut AppConf, &'static mut AppGlobal)> {
    let mut buf = SmallString::<[u8; 512]>::new();

    write!(buf, "{APP_NAME} 版本 {APP_VER} 版权所有 Kivensoft 2021-2023.").unwrap();
    let version = buf.as_str();

    // 从命令行和配置文件读取配置
    let ac = AppConf::init();
    if !appcfg::parse_args(ac, version).expect("解析参数失败") {
        return None;
    }

    // 初始化全局常量参数
    let ag = AppGlobal::init(AppGlobal {
        startup_time: LocalTime::now().timestamp(),
        jwt_ttl: ac.jwt_ttl.parse().expect(arg_err!("jwt-ttl")),
    });

    if ac.listen.starts_with("0.0.0.0:") {
        panic!("arg listen 必须指定具体的ip而不能使用`0.0.0.0`")
    }
    if !ac.listen.is_empty() && ac.listen.as_bytes()[0] == b':' {
        ac.listen.insert_str(0, "127.0.0.1");
    };

    // if log_level == log::Level::Trace {
    //     println!("配置详情: {ac:#?}\n");
    // }

    // 在控制台输出logo
    if let Some((s1, s2)) = BANNER.split_once('%') {
        let s2 = &s2[APP_VER.len() - 1..];
        buf.clear();
        write!(buf, "{s1}{APP_VER}{s2}").unwrap();
        appcfg::print_banner(&buf, true);
    }

    Some((ac, ag))
}

fn init_log(ac: &AppConf) {
    // 初始化日志组件
    let log_level = asynclog::parse_level(&ac.log_level).expect(arg_err!("log-level"));
    let log_max = asynclog::parse_size(&ac.log_max).expect(arg_err!("log-max"));

    asynclog::Builder::new()
        .level(log_level)
        .log_file(ac.log_file.clone())
        .log_file_max(log_max)
        .use_console(!ac.no_console)
        .use_async(ac.log_async)
        .builder()
        .expect("初始化日志组件失败");
    asynclog::set_level("mio".to_owned(), log::LevelFilter::Info);
    asynclog::set_level("tokio_util".to_owned(), log::LevelFilter::Info);
    asynclog::set_level("hyper_util".to_owned(), log::LevelFilter::Info);
}

/// 创建http服务接口
fn reg_apis(srv: &mut HttpServer) {
    httpserver::register_apis!(srv, "/sys/",
        "login": apis::login::login,
        "logout": apis::login::logout,
        "refreshToken": apis::login::refresh_token,
        "authenticate": apis::login::authenticate,
    );

    macro_rules! cc {
        ($path:literal) => {
            concat!("/sys/", $path, "/")
        };
    }

    httpserver::register_apis!(srv, cc!("account"),
        "profile": apis::account::profile,
        "get": apis::account::get,
        "update": apis::account::update,
        "changePassword": apis::account::change_password,
        "changeMobile": apis::account::change_mobile,
        "changeEmail": apis::account::change_email,
        "menus": apis::account::menus,
    );

    httpserver::register_apis!(srv, cc!("api"),
        "list": apis::api::list,
        "get": apis::api::get,
        "insert": apis::api::insert,
        "update": apis::api::update,
        "del": apis::api::del,
        "items": apis::api::items,
        "repermission": apis::api::repermission,
        "rearrange": apis::api::rearrange,
        "groups": apis::api::groups,
        "permissions": apis::api::permissions,
    );

    httpserver::register_apis!(srv, cc!("config"),
        "list": apis::config::list,
        "get": apis::config::get,
        "insert": apis::config::insert,
        "update": apis::config::update,
        "del": apis::config::del,
    );

    httpserver::register_apis!(srv, cc!("debug"),
        "resetPermission": apis::debug::reset_permission,
        "redisClear": apis::debug::redis_clear,
        "redisGet": apis::debug::redis_get,
        "redisSet": apis::debug::redis_set,
    );

    httpserver::register_apis!(srv, cc!("dict"),
        "list": apis::dict::list,
        "get": apis::dict::get,
        "insert": apis::dict::insert,
        "update": apis::dict::update,
        "del": apis::dict::del,
        "items": apis::dict::items,
        "batch": apis::dict::batch,
        "permissionGroups": apis::dict::permission_groups,
        "resort": apis::dict::resort,
    );

    httpserver::register_apis!(srv, cc!("menu"),
        "list": apis::menu::list,
        "get": apis::menu::get,
        "insert": apis::menu::insert,
        "update": apis::menu::update,
        "del": apis::menu::del,
        "topLevel": apis::menu::top_level,
        "tree": apis::menu::tree,
        "rearrange": apis::menu::rearrange,
    );

    httpserver::register_apis!(srv, cc!("permission"),
        "list": apis::permission::list,
        "get": apis::permission::get,
        "insert": apis::permission::insert,
        "update": apis::permission::update,
        "del": apis::permission::del,
        "items": apis::permission::items,
        "tree": apis::permission::tree,
        "rearrange": apis::permission::rearrange,
    );

    httpserver::register_apis!(srv, cc!("role"),
        "list": apis::role::list,
        "get": apis::role::get,
        "insert": apis::role::insert,
        "update": apis::role::update,
        "del": apis::role::del,
        "items": apis::role::items,
    );

    httpserver::register_apis!(srv, cc!("tools"),
        "ping": apis::tools::ping,
        "status": apis::tools::status,
        "ip": apis::tools::ip,
        "qrcode": apis::tools::qrcode,
        "createPassword": apis::tools::create_password,
    );

    httpserver::register_apis!(srv, cc!("user"),
        "list": apis::user::list,
        "get": apis::user::get,
        "insert": apis::user::insert,
        "update": apis::user::update,
        "del": apis::user::del,
        "disable": apis::user::disable,
        "resetpw": apis::user::resetpw,
        "items": apis::user::items,
    );
}

/// 从网关服务器加载配置
async fn load_remote_config(ac: &mut AppConf) -> Result<()> {
    let cfgs = utils::rcall::load_config("common")
        .await
        .context("加载远程配置失败")?;
    for item in cfgs.into_iter() {
        let val = item.value;
        match item.key.as_str() {
            "mysql.host" => ac.db_host = val,
            "mysql.port" => ac.db_port = val,
            "mysql.user" => ac.db_user = val,
            "mysql.pass" => ac.db_pass = val,
            "mysql.name" => ac.db_name = val,

            "redis.host" => ac.redis_host = val,
            "redis.port" => ac.redis_port = val,
            "redis.user" => ac.redis_user = val,
            "redis.pass" => ac.redis_pass = val,
            "redis.name" => ac.redis_name = val,

            "token.key" => ac.jwt_key = val,
            "token.iss" => ac.jwt_iss = val,
            "token.ttl" => ac.jwt_ttl = val,
            "token.refresh" => ac.jwt_refresh = val,

            _ => {}
        }
    }
    Ok(())
}

async fn init_redis(ac: &AppConf) -> Result<()> {
    use consts::cfg::{REDIS_EXPIRE, MEM_CACHE_EXPIRE};

    // 初始化缓存连接
    let redis_url = services::uri::RedisConfig {
        host: &ac.redis_host,
        port: &ac.redis_port,
        user: &ac.redis_user,
        pass: &ac.redis_pass,
        db: &ac.redis_name,
    }
    .build_url();

    let redis = services::uri::UniRedisImpl::new(&ac.redis_pre, REDIS_EXPIRE);

    // 初始化消息队列
    services::mq::init(&redis_url, false);
    // 初始化redis存取服务
    services::uri::init(redis_url).dot().context("初始化缓存连接失败")?;
    // 初始化通用多层缓存服务
    services::gmc::init(1024, MEM_CACHE_EXPIRE as u32, redis,
        &ac.redis_pre, REDIS_EXPIRE as u32).await;

    Ok(())
}

async fn init_db_pool(ac: &AppConf) -> Result<()> {
    gensql::init_pool(DbConfig {
        host: &ac.db_host,
        port: &ac.db_port,
        user: &ac.db_user,
        pass: &ac.db_pass,
        db: &ac.db_name,
    })
    .context("初始化数据库连接失败")?;

    // 测试数据库连接是否正确
    gensql::try_connect().await.context("数据库连接失败")
}

async fn async_main(ac: &mut AppConf, ag: &mut AppGlobal) -> Result<()> {
    init_log(ac);

    let have_gateway = !ac.gateway.is_empty();
    let reg_interval = ac.reg_interval.parse()?;
    let token_cache_size = ac.token_cache_size.parse()?;

    // 加载远程配置
    if have_gateway {
        if let Err(e) = load_remote_config(ac).await {
            log::error!("加载远程配置失败: {e:?}");
            log::debug!("使用本地配置启动服务...");
        }
    }
    ag.jwt_ttl = ac.jwt_ttl.parse::<u32>().context("配置jwt-ttl格式错误")? * 60;

    // 输出配置详情
    if log::log_enabled!(log::Level::Trace) {
        println!("配置详情: {:#?}\n", ac);
    }

    // 初始化缓存连接
    init_redis(ac).await?;
    // 初始化数据库连接
    init_db_pool(ac).await?;

    // 启动服务注册心跳任务
    if have_gateway {
        tokio::spawn(utils::rcall::start_heart_break_task(reg_interval));
    }

    let addr: std::net::SocketAddr = ac.listen.parse().expect(arg_err!("listen"));
    let (cancel_sender, cancel_manager) = httpserver::new_cancel();

    let mut srv = HttpServer::new();
    srv.set_context_path(CONTEXT_PATH);
    srv.set_middleware(httpserver::AccessLog);
    srv.set_fuzzy_find(httpserver::FuzzyFind::None);
    srv.set_cancel_manager(cancel_manager);

    reg_apis(&mut srv);
    srv.reg_websocket("/ws/notify", on_websocket);

    // 设置权限校验中间件
    let (jwt_key, jwt_iss) = if_else!(
        have_gateway,
        ("", ""),
        (ac.jwt_key.as_str(), ac.jwt_iss.as_str())
    );

    let authent = auth::Authentication::new(
        CONTEXT_PATH,
        jwt_key, jwt_iss,
        token_cache_size,
        Duration::from_secs(consts::cfg::MEM_CACHE_EXPIRE),
        format!("{}:{}", ac.redis_pre, consts::TOKEN_KEY),
        consts::cfg::REDIS_EXPIRE
    ).await?;

    let authent = srv.set_middleware(authent);
    authent.start_recycle_task(TOKEN_CACHE_TASK_INTERVAL);
    authent.clone().start_listen(
        format!("{}:{}:{}", ac.redis_pre, consts::gmc::MOD_KEY, consts::gmc::SYS_ROLE),
        format!("{}:{}:{}", ac.redis_pre, consts::gmc::MOD_KEY, consts::gmc::SYS_API),
        format!("{}:{}:{}", ac.redis_pre, consts::gmc::MOD_KEY, consts::gmc::SYS_USER),
        format!("{}:{}", ac.redis_pre, consts::CC_LOGOUT),
    ).await?;
    auth::init_authentication(authent);

    // 启动http server监听
    let listener = srv.listen(addr).await.expect("http server listen error");

    // 运行http服务
    tokio::spawn(async move {
        if have_gateway {
            if let Err(e) = utils::rcall::reg_to_gateway().await {
                log::error!("首次向网关注册服务失败: {e:?}");
            }
        }
        if let Err(e) = srv.arc().serve(listener).await {
            log::error!("http服务运行失败: {e:?}");
        }
    });

    // 监听ctrl+c事件
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            println!("正在关闭{APP_NAME}服务...");
            cancel_sender
                .cancel_and_wait(Duration::from_secs(5))
                .await?;
        }
        Err(e) => {
            log::error!("无法监听ctrl+c事件: {e:?}");
        }
    }

    Ok(())
}

// #[tokio::main(worker_threads = 4)]
// #[tokio::main(flavor = "current_thread")]
fn main() {
    let (ac, ag) = match init() {
        Some((ac, ag)) => (ac, ag),
        None => return,
    };
    println!("正在启动服务...");

    let threads = ac.threads.parse::<usize>().expect(arg_err!("threads"));

    #[cfg(not(feature = "multi_thread"))]
    let mut builder = {
        assert!(
            threads == 1,
            "{APP_NAME} current version unsupport multi-threads"
        );
        tokio::runtime::Builder::new_current_thread()
    };

    #[cfg(feature = "multi_thread")]
    let mut builder = {
        assert!(threads <= 256, "multi-threads range in 0-256");

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if threads > 0 {
            builder.worker_threads(threads);
        }

        builder
    };

    builder.enable_all().build().unwrap().block_on(async move {
        if let Err(e) = async_main(ac, ag).await {
            eprintln!("程序运行发生错误: {e:?}");
        }
    })
}

async fn on_websocket(ctx: httpserver::WsContext) -> Result<()> {
    use futures_util::StreamExt;
    use httpserver::WsMessage;

    log_debug!(ctx.id, "启动新的websocket连接...");

    let mut websocket = ctx.websocket.await?;

    while let Some(message) = websocket.next().await {
        match message? {
            WsMessage::Text(msg) => {
                log_debug!(ctx.id, "Received text message: {msg}");
                websocket
                    .send(WsMessage::text(format!("我收到消息: {msg}")))
                    .await?;
            }
            WsMessage::Binary(msg) => {
                log_debug!(ctx.id, "Received binary message: {msg:02X?}");
                websocket
                    .send(WsMessage::binary(b"Thank you, come again.".to_vec()))
                    .await?;
            }
            WsMessage::Ping(msg) => {
                // No need to send a reply: tungstenite takes care of this for you.
                log_debug!(ctx.id, "Received ping message: {msg:02X?}");
            }
            WsMessage::Pong(msg) => {
                log_debug!(ctx.id, "Received pong message: {msg:02X?}");
            }
            WsMessage::Close(msg) => {
                // No need to send a reply: tungstenite takes care of this for you.
                if let Some(msg) = &msg {
                    log_debug!(
                        ctx.id,
                        "Received close message with code {} and message: {}",
                        msg.code,
                        msg.reason
                    );
                } else {
                    log_debug!(ctx.id, "Received close message");
                }
            }
            WsMessage::Frame(_msg) => {
                unreachable!();
            }
        }
    }

    Ok(())
}
