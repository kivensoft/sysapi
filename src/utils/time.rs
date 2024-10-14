//! 日期时间相关函数
use std::time::SystemTime;

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 秒值转换为基于友好时间表示的时间字符串
pub fn gen_time_desc(mut timestamp_secs: u32) -> String {
    let mut num_buf = itoa::Buffer::new();
    let mut time_desc = String::new();

    if timestamp_secs >= 3600 {
        time_desc.push_str(num_buf.format(timestamp_secs / 3600));
        time_desc.push_str("小时");
        timestamp_secs %= 3600;
    }

    if timestamp_secs >= 60 {
        time_desc.push_str(num_buf.format(timestamp_secs / 60));
        time_desc.push_str("分钟");
        timestamp_secs %= 60;
    }

    if !time_desc.is_empty() && timestamp_secs != 0 {
        time_desc.push_str(num_buf.format(timestamp_secs));
        time_desc.push('秒');
    }

    time_desc
}
