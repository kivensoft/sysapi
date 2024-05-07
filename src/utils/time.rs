//! 日期时间相关函数
use compact_str::CompactString;
use std::time::SystemTime;

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 秒值转换为基于友好时间表示的时间字符串
pub fn gen_time_desc(mut secs: u32) -> CompactString {
    let mut num_buf = itoa::Buffer::new();
    let mut time_desc = CompactString::new("");

    if secs >= 3600 {
        time_desc.push_str(num_buf.format(secs / 3600));
        time_desc.push_str("小时");
        secs %= 3600;
    }

    if secs >= 60 {
        time_desc.push_str(num_buf.format(secs / 60));
        time_desc.push_str("分钟");
        secs %= 60;
    }

    if !time_desc.is_empty() && secs != 0 {
        time_desc.push_str(num_buf.format(secs));
        time_desc.push_str("秒");
    }

    time_desc
}
