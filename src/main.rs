//! 主程序单元
#[macro_use]
mod utils;

mod apis;
mod auth;
mod entities;
mod services;

use std::{fmt::Write, time::Duration};

use anyhow_ext::{Context, Result};
use httpserver::HttpServer;
use localtime::LocalTime;
use services::rcall;
use smallstr::SmallString;
use tokio::signal;

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

const CONTENT_PATH: &str = "/api"; // 接口请求的上下文路径
const TOKEN_CACHE_TASK_INTERVAL: u64 = 600;

appconfig::appglobal_define! {app_global, AppGlobal,
    startup_time: i64,
    jwt_ttl: u32,
}

appconfig::appconfig_define! {app_conf, AppConf,
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
    db_host         : String => ["",   "db-host",          "DbHost",            "数据库服务主机名"],
    db_port         : String => ["",   "db-port",          "DbPort",            "数据库服务端口"],
    db_user         : String => ["",   "db-user",          "DbUser",            "数据库用户名"],
    db_pass         : String => ["",   "db-pass",          "DbPass",            "数据库密码"],
    db_name         : String => ["",   "db-name",          "DbName",            "数据库名称"],
    cache_host      : String => ["",   "cache-host",       "CacheHost",         "缓存服务主机名"],
    cache_port      : String => ["",   "cache-port",       "CachePort",         "缓存服务端口"],
    cache_user      : String => ["",   "cache-user",       "CacheUser",         "缓存服务用户名"],
    cache_pass      : String => ["",   "cache-pass",       "CachePass",         "缓存服务口令"],
    cache_name      : String => ["",   "cache-name",       "CacheName",         "缓存服务数据库名称"],
    cache_pre       : String => ["",   "cache-pre",        "CachePre",          "缓存项前缀"],
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
            db_host: String::from("127.0.0.1"),
            db_port: String::from("3306"),
            db_user: String::from("root"),
            db_pass: String::from("password"),
            db_name: String::new(),
            cache_host: String::from("127.0.0.1"),
            cache_port: String::from("6379"),
            cache_user: String::new(),
            cache_pass: String::from("password"),
            cache_name: String::from("0"),
            cache_pre: String::from("sysapi"),
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

    write!(
        buf,
        "{APP_NAME} 版本 {APP_VER} 版权所有 Kivensoft 2021-2023."
    )
    .unwrap();
    let version = buf.as_str();

    // 从命令行和配置文件读取配置
    let ac = AppConf::init();
    if !appconfig::parse_args(ac, version).expect("解析参数失败") {
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

    // 初始化日志组件
    let log_level = asynclog::parse_level(&ac.log_level).expect(arg_err!("log-level"));
    let log_max = asynclog::parse_size(&ac.log_max).expect(arg_err!("log-max"));

    // if log_level == log::Level::Trace {
    //     println!("配置详情: {ac:#?}\n");
    // }

    asynclog::init_log(
        log_level,
        ac.log_file.clone(),
        log_max,
        !ac.no_console,
        ac.log_async,
    )
    .expect("初始化日志组件失败");
    asynclog::set_level("mio".to_owned(), log::LevelFilter::Info);
    asynclog::set_level("tokio_util".to_owned(), log::LevelFilter::Info);
    asynclog::set_level("hyper_util".to_owned(), log::LevelFilter::Info);

    // 在控制台输出logo
    if let Some((s1, s2)) = BANNER.split_once('%') {
        let s2 = &s2[APP_VER.len() - 1..];
        buf.clear();
        write!(buf, "{s1}{APP_VER}{s2}").unwrap();
        appconfig::print_banner(&buf, true);
    }

    Some((ac, ag))
}

/// 创建http服务接口
fn reg_apis(srv: &mut HttpServer) {
    httpserver::register_apis!(srv, "/",
        "login": apis::login::login,
        "logout": apis::login::logout,
        "refresh": apis::login::refresh,
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
        "reset-permission": apis::debug::reset_permission,
        "redis-clear": apis::debug::redis_clear,
        "redis-get": apis::debug::redis_get,
        "redis-set": apis::debug::redis_set,
    );

    httpserver::register_apis!(srv, cc!("dict"),
        "list": apis::dict::list,
        "get": apis::dict::get,
        "insert": apis::dict::insert,
        "update": apis::dict::update,
        "del": apis::dict::del,
        "items": apis::dict::items,
        "batch": apis::dict::batch,
    );

    httpserver::register_apis!(srv, cc!("menu"),
        "list": apis::menu::list,
        "get": apis::menu::get,
        "insert": apis::menu::insert,
        "update": apis::menu::update,
        "del": apis::menu::del,
        "top-level": apis::menu::top_level,
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
        "gen-pass": apis::tools::gen_pass,
    );

    httpserver::register_apis!(srv, cc!("user"),
        "list": apis::user::list,
        "get": apis::user::get,
        "insert": apis::user::insert,
        "update": apis::user::update,
        "del": apis::user::del,
        "change-disabled": apis::user::change_disabled,
        "reset-password": apis::user::reset_password,
        "items": apis::user::items,
    );
}

/// 从网关服务器加载配置
async fn load_remote_config(ac: &mut AppConf) -> Result<()> {
    let cfgs = rcall::load_config("common").await.context("加载远程配置失败")?;
    for item in cfgs.into_iter() {
        let val = item.value;
        match item.key.as_str() {
            "mysql.host" => ac.db_host = val,
            "mysql.port" => ac.db_port = val,
            "mysql.user" => ac.db_user = val,
            "mysql.pass" => ac.db_pass = val,
            "mysql.name" => ac.db_name = val,

            "redis.host" => ac.cache_host = val,
            "redis.port" => ac.cache_port = val,
            "redis.user" => ac.cache_user = val,
            "redis.pass" => ac.cache_pass = val,
            "redis.name" => ac.cache_name = val,

            "token.key" => ac.jwt_key = val,
            "token.iss" => ac.jwt_iss = val,
            "token.ttl" => ac.jwt_ttl = val,
            "token.refresh" => ac.jwt_refresh = val,

            _ => {}
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
    let addr: std::net::SocketAddr = ac.listen.parse().expect(arg_err!("listen"));
    let (cancel_sender, cancel_manager) = httpserver::new_cancel();

    let mut srv = HttpServer::new();
    srv.set_content_path(CONTENT_PATH);
    srv.set_middleware(httpserver::AccessLog);
    srv.set_fuzzy_find(httpserver::FuzzyFind::None);
    srv.set_cancel_manager(cancel_manager);

    reg_apis(&mut srv);

    let reg_interval = ac.reg_interval.parse().unwrap();
    let have_gateway = !ac.gateway.is_empty();
    let token_cache_size = ac.token_cache_size.parse().unwrap();

    let async_fn = async move {
        // 加载远程配置
        if have_gateway {
            if let Err(e) = load_remote_config(ac).await {
                log::error!("加载远程配置失败: {e:?}");
                log::debug!("使用本地配置启动服务...");
            }
        }

        ag.jwt_ttl = ac.jwt_ttl.parse::<u32>().expect("配置jwt-ttl格式错误") * 60;
        log::trace!("配置: {ac:#?}");

        // 初始化数据库连接和缓存连接
        services::rcache::init_pool(ac).expect("初始化缓存连接失败");
        gensql::init_pool(
            &ac.db_user,
            &ac.db_pass,
            &ac.db_host,
            &ac.db_port,
            &ac.db_name,
        )
        .expect("初始化数据库连接失败");

        // 测试数据库连接是否正确
        gensql::try_connect().await.expect("数据库连接失败");

        // 启动服务注册心跳任务
        if have_gateway {
            tokio::spawn(rcall::start_heart_break_task(reg_interval));
        }

        // 设置权限校验中间件
        {
            let (jwt_key, jwt_iss) = if have_gateway {
                ("", "")
            } else {
                (ac.jwt_key.as_str(), ac.jwt_iss.as_str())
            };
            auth::init(
                CONTENT_PATH,
                jwt_key,
                jwt_iss,
                token_cache_size,
                TOKEN_CACHE_TASK_INTERVAL,
            )
            .await;
            srv.set_middleware(auth::Authentication);
        }

        // 启动http server监听
        let listener = srv.listen(addr).await.expect("http server listen error");

        // 运行http服务
        tokio::spawn(async move {
            if have_gateway {
                if let Err(e) = rcall::reg_to_gateway().await {
                    log::error!("首次向网关注册服务失败: {e:?}");
                }
            }
            if let Err(e) = srv.serve(listener).await {
                log::error!("http服务运行失败: {e:?}");
            }
        });

        // 监听ctrl+c事件
        match signal::ctrl_c().await {
            Ok(()) => {
                println!("正在关闭{APP_NAME}服务...");
                cancel_sender.cancel_and_wait(Duration::from_millis(100)).await.unwrap();
            }
            Err(e) => {
                log::error!("无法监听ctrl+c事件: {e:?}");
            }
        }
    };

    #[cfg(not(feature = "multi_thread"))]
    let mut builder = {
        assert!(threads == 1, "{APP_NAME} current version unsupport multi-threads");
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

    builder.enable_all()
        .build()
        .unwrap()
        .block_on(async_fn)
}
