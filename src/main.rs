mod auth;
mod apis;
mod db;
mod services;
mod utils;

use std::fmt::Write;
use compact_str::format_compact;
use httpserver::HttpServer;
use localtime::LocalTime;
use services::rpc;
use smallstr::SmallString;
use tokio::signal;

const BANNER: &str = r#"
   _____ Kivensoft  ___          _
  / ___/__  _______/   |  ____  (_)
  \__ \/ / / / ___/ /| | / __ \/ /
 ___/ / /_/ (__  ) ___ |/ /_/ / /
/____/\__, /____/_/  |_/ .___/_/
     /____/  %        /_/
"#;

const APP_NAME: &str = "sysapi";
/// app版本号, 来自编译时由build.rs从cargo.toml中读取的版本号(读取内容写入.version文件)
const APP_VER: &str = include_str!(concat!(env!("OUT_DIR"), "/.version"));
const SERVICE_PREFIX: &str = "/sys/";

appconfig::appglobal_define!(app_global, AppGlobal,
    startup_time: i64,
    jwt_ttl: u32,
);

appconfig::appconfig_define!(app_conf, AppConf,
    log_level   : String => ["L",  "log-level",    "LogLevel",          "日志级别(trace/debug/info/warn/error/off)"],
    log_file    : String => ["F",  "log-file",     "LogFile",           "日志的相对路径或绝对路径文件名"],
    log_max     : String => ["M",  "log-max",      "LogFileMaxSize",    "日志文件的最大长度 (单位: k|m|g)"],
    log_async   : bool   => ["",   "log-async",    "LogAsync",          "启用异步日志"],
    no_console  : bool   => ["",   "log-max",      "NoConsole",         "禁止将日志输出到控制台"],
    threads     : String => ["t",  "threads",      "Threads",           "设置应用的线程数"],
    listen      : String => ["l",  "listen",       "Listen",            "服务监听端点 (ip地址:端口号)"],
    gateway     : String => ["g",  "gateway",      "Gateway",           "api网关端点 (ip地址:端口号)"],
    reg_interval: String => ["",   "reg_interval", "RegInterval",       "服务注册心跳保持时间 (单位: 秒)"],
    db_host     : String => ["",   "db-host",      "DbHost",            "数据库服务主机名"],
    db_port     : String => ["",   "db-port",      "DbPort",            "数据库服务端口"],
    db_user     : String => ["",   "db-user",      "DbUser",            "数据库用户名"],
    db_pass     : String => ["",   "db-pass",      "DbPass",            "数据库密码"],
    db_name     : String => ["",   "db-name",      "DbName",            "数据库名称"],
    // db_extra    : String => ["",   "db-extra",     "DbExtra",           "数据库扩展属性 (example: key1=value1&key2=value2)"],
    cache_host  : String => ["",   "cache-host",   "CacheHost",         "缓存服务主机名"],
    cache_port  : String => ["",   "cache-port",   "CachePort",         "缓存服务端口"],
    cache_user  : String => ["",   "cache-user",   "CacheUser",         "缓存服务用户名"],
    cache_pass  : String => ["",   "cache-pass",   "CachePass",         "缓存服务口令"],
    cache_name  : String => ["",   "cache-name",   "CacheName",         "缓存服务数据库名称"],
    cache_pre   : String => ["",   "cache-pre",    "CachePre",          "缓存项前缀"],
    jwt_key     : String => ["",   "jwt-key",      "JwtKey",            "令牌密钥"],
    jwt_iss     : String => ["",   "jwt-iss",      "JwtIss",            "令牌发行者"],
    jwt_ttl     : String => ["",   "jwt-ttl",      "JwtTtl",            "令牌存活时间 (单位: 分钟)"],
    jwt_refresh : String => ["",   "jwt-refresh",  "JwtRefresh",        "刷新令牌的密钥"],
);

impl Default for AppConf {
    fn default() -> Self {
        Self {
            log_level:      String::from("info"),
            log_file:       String::with_capacity(0),
            log_max:        String::from("10m"),
            log_async:      false,
            no_console:     false,
            threads:        String::from("1"),
            listen:         String::from("127.0.0.1:6401"),
            gateway:        String::from("127.0.0.1:6400"),
            reg_interval:   String::from("30"),
            db_host:        String::from("127.0.0.1"),
            db_port:        String::from("3306"),
            db_user:        String::from("root"),
            db_pass:        String::from("password"),
            db_name:        String::new(),
            // db_extra:       String::new(),
            cache_host:     String::from("127.0.0.1"),
            cache_port:     String::from("6379"),
            cache_user:     String::new(),
            cache_pass:     String::from("password"),
            cache_name:     String::from("0"),
            cache_pre:      String::from("sysapi"),
            jwt_key:        String::from("SysApi CopyRight by kivensoft 2023-05-04"),
            jwt_iss:        String::from("SysApi"),
            jwt_ttl:        String::from("1440"),
            jwt_refresh:    String::from("SysApi copyright kivensoft 2023-09-27"),
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

    asynclog::init_log(log_level, ac.log_file.clone(), log_max,
        !ac.no_console, ac.log_async).expect("初始化日志组件失败");
    asynclog::set_level("mio".to_owned(), log::LevelFilter::Info);
    asynclog::set_level("hyper".to_owned(), log::LevelFilter::Info);
    asynclog::set_level("want".to_owned(), log::LevelFilter::Info);

    // 在控制台输出logo
    if let Some((s1, s2)) = BANNER.split_once('%') {
        let s2 = &s2[s2.len() - 1..];
        buf.clear();
        write!(buf, "{s1}{APP_VER}{s2}").unwrap();
        appconfig::print_banner(&buf, true);
    }

    Some((ac, ag))
}

/// 创建http服务接口
fn reg_apis(srv: &mut HttpServer) {
    macro_rules! cc {
        ($path:literal) => {
            concat!($path, "/")
        };
    }

    httpserver::register_apis!(srv, cc!("account"),
        "profile": apis::account::profile,
        "get": apis::account::get,
        "post": apis::account::post,
        "change-password": apis::account::change_password,
        "change-mobile": apis::account::change_mobile,
        "change-email": apis::account::change_email,
        "menus": apis::account::menus,
    );

    httpserver::register_apis!(srv, cc!("api"),
        "list": apis::api::list,
        "get": apis::api::get,
        "post": apis::api::post,
        "del": apis::api::del,
        "items": apis::api::items,
        "rearrange": apis::api::rearrange,
        "groups": apis::api::groups,
        "permissions": apis::api::permissions,
    );

    httpserver::register_apis!(srv, cc!("auth"),
        "login": apis::auth::login,
        "logout": apis::auth::logout,
        "refresh": apis::auth::refresh,
        "authenticate": apis::auth::authenticate,
    );

    httpserver::register_apis!(srv, cc!("config"),
        "list": apis::config::list,
        "get": apis::config::get,
        "post": apis::config::post,
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
        "post": apis::dict::post,
        "del": apis::dict::del,
        "items": apis::dict::items,
        "batch": apis::dict::batch,
    );

    httpserver::register_apis!(srv, cc!("menu"),
        "list": apis::menu::list,
        "get": apis::menu::get,
        "post": apis::menu::post,
        "del": apis::menu::del,
        "top-level": apis::menu::top_level,
        "tree": apis::menu::tree,
        "rearrange": apis::menu::rearrange,
    );

    httpserver::register_apis!(srv, cc!("permission"),
        "list": apis::permission::list,
        "get": apis::permission::get,
        "post": apis::permission::post,
        "del": apis::permission::del,
        "items": apis::permission::items,
        "tree": apis::permission::tree,
        "rearrange": apis::permission::rearrange,
    );

    httpserver::register_apis!(srv, cc!("role"),
        "list": apis::role::list,
        "get": apis::role::get,
        "post": apis::role::post,
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
        "post": apis::user::post,
        "del": apis::user::del,
        "change-disabled": apis::user::change_disabled,
        "reset-password": apis::user::reset_password,
        "items": apis::user::items,
    );

}

/// 从网关服务器加载配置
async fn load_remote_config(ac: &mut AppConf) -> anyhow::Result<()> {
    let cfgs = rpc::load_config("common").await?;
    for item in cfgs.into_iter() {
        let val = item.value;
        match item.key.as_str() {
            "mysql.host"    => ac.db_host = val,
            "mysql.port"    => ac.db_port = val,
            "mysql.user"    => ac.db_user = val,
            "mysql.pass"    => ac.db_pass = val,
            "mysql.name"    => ac.db_name = val,

            "redis.host"    => ac.cache_host = val,
            "redis.port"    => ac.cache_port = val,
            "redis.user"    => ac.cache_user = val,
            "redis.pass"    => ac.cache_pass = val,
            "redis.name"    => ac.cache_name = val,

            "token.key"     => ac.jwt_key = val,
            "token.iss"     => ac.jwt_iss = val,
            "token.ttl"     => ac.jwt_ttl = val,
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
    log::info!("正在启动服务...");

    let threads = ac.threads.parse::<usize>().expect(arg_err!("threads"));
    let addr: std::net::SocketAddr = ac.listen.parse().expect(arg_err!("listen"));
    let mut srv = HttpServer::new(
        &format_compact!("{}{}", auth::API_PATH_PRE, SERVICE_PREFIX), true);

    reg_apis(&mut srv);

    let reg_interval = ac.reg_interval.parse().unwrap();
    let have_gateway = !ac.gateway.is_empty();

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
        gensql::init_pool(&ac.db_user, &ac.db_pass, &ac.db_host, &ac.db_port,
            &ac.db_name).expect("初始化数据库连接失败");

        // 测试数据库连接是否正确
        gensql::try_connect().await.expect("数据库连接失败");

        // 启动服务注册心跳任务
        if have_gateway {
            tokio::spawn(rpc::start_heart_break_task(reg_interval));
        }

        // 设置权限校验中间件
        auth::init().await;
        srv.middleware(auth::Authentication);

        // 运行http服务
        tokio::spawn(async move {
            if let Err(e) = srv.run_with_callbacck(addr, move || async move {
                if have_gateway {
                    if let Err(e) = rpc::reg_to_gateway().await {
                        log::error!("首次向网关注册服务失败: {e:?}");
                    }
                }
                Ok(())
            }).await {
                log::error!("启动http服务失败: {e:?}");
            }
        });

        match signal::ctrl_c().await {
            Ok(()) => {
                log::info!("关闭{APP_NAME}服务");
            },
            Err(e) => {
                log::error!("Unable to listen for shutdown signal: {e:?}");
            },
        }
    };


    #[cfg(not(feature = "multi_thread"))]
    {
        assert!(threads == 1, "{APP_NAME} 当前版本是单线程版本");

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async_fn);
    }

    #[cfg(feature = "multi_thread")]
    {
        assert!(threads >= 0 && threads <= 256, "线程数量范围在 0-256 之间");

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if threads > 0 {
            builder.worker_threads(threads);
        }

        builder.enable_all()
            .build()
            .unwrap()
            .block_on(async_fn)
    }

}
