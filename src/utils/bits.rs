//! 位操作相关函数
//!
//! Author: kiven lee
//! Date: 2024-03-16

/// 读取16进制字符串中指定索引位的值
///
/// Arguments:
///
/// * `hex`: 16进制字符串
/// * `index`: 要读取的位的索引值
///
/// Returns:
///
/// * false: 如果位索引超出字符串长度
/// * 位值: 对应索引位的值
pub fn get(hex: &str, index: usize) -> bool {
    if index >= hex.len() << 2 {
        return false;
    }
    let b = c2b(hex.as_bytes()[index >> 2]);
    (b & (8 >> (index & 3))) != 0
}

/// 设置16进制字符串中指定索引位置的值
///
/// Arguments:
///
/// * `hex`: 16进制字符串
/// * `index`: 要设置位的索引值
/// * `value`: 新的位值
pub fn set(hex: &mut String, index: usize, value: bool) {
    let hex_vec = unsafe { hex.as_mut_vec() };

    // 计算索引位对应的字符串长度
    let req_len = ((index + 8) >> 3) << 1;
    // 如果字符串长度不足, 后面补'0'
    if req_len > hex_vec.len() {
        hex_vec.resize(req_len, b'0');
    }

    // 设置对应的位标志
    let pos = index >> 2;
    let b = c2b(hex_vec[pos]);
    let n = (8 >> (index & 3)) as u8;
    let b = if value { b | n } else { b & !n };
    hex_vec[pos] = if b < 10 { 48 + b } else { 87 + b };
}

/// 查找第一个符合条件的值所在的索引
///
/// Arguments:
///
/// * `hex`: 16进制字符串
/// * `value`: 要查找的值
///
/// Returns:
///
/// * u32::MAX: 未找到匹配值
/// * 位值: 找到的第一个匹配值的索引位
pub fn find(hex: &str, value: bool) -> Option<usize> {
    find_range(hex, value, 0, hex.len() << 2)
}

/// 在指定范围内查找查找第一个符合条件的值所在的索引
///
/// Arguments:
///
/// * `hex`: 16进制字符串
/// * `value`: 要查找的值
/// * `start`: 起始位置
/// * `end`: 结束位置(半开区间)
///
/// Returns:
///
/// * u32::MAX: 未找到匹配值
/// * 位值: 找到的第一个匹配值的索引位
pub fn find_range(hex: &str, value: bool, start: usize, end: usize) -> Option<usize> {
    let hex = hex.as_bytes();
    let hex_bits_len = hex.len() << 2;
    let end = if end > hex_bits_len {
        hex_bits_len
    } else {
        end
    };

    if start >= end {
        return None;
    }

    let mut pos = start;

    // 处理起始不与4对齐的位
    let off = pos & 3;
    if off != 0 {
        let b = c2b(hex[pos >> 2]);
        let len = std::cmp::min(4 - off, end - pos);
        if let Some(idx) = find_in_byte(b, value, off, len) {
            return Some((pos & !3) + idx);
        }
        pos += len;
    }

    // 处理4对齐的bit
    let align_end = end & !3;
    while pos < align_end {
        let b = c2b(hex[pos >> 2]);
        // if let Some(idx) = find_in_byte(b, value, 0, 4) {
        //     return Some(pos + idx);
        // }
        // 改成循环展开, 能让运算速度快上一些
        if value {
            if (b & 8) != 0 {
                return Some(pos);
            }
            if (b & 4) != 0 {
                return Some(pos + 1);
            }
            if (b & 2) != 0 {
                return Some(pos + 2);
            }
            if (b & 1) != 0 {
                return Some(pos + 3);
            }
        } else {
            if (b & 8) == 0 {
                return Some(pos);
            }
            if (b & 4) == 0 {
                return Some(pos + 1);
            }
            if (b & 2) == 0 {
                return Some(pos + 2);
            }
            if (b & 1) == 0 {
                return Some(pos + 3);
            }
        }
        pos += 4;
    }

    // 处理尾部非4对齐的bit
    if pos < end {
        let b = c2b(hex[pos >> 2]);
        let len = end - align_end;
        if let Some(idx) = find_in_byte(b, value, 0, len) {
            return Some(pos + idx);
        }
    }

    None
}

/// 从后面开始查找第一个符合条件的值所在的索引
///
/// Arguments:
///
/// * `hex`: 16进制字符串
/// * `value`: 要查找的值
///
/// Returns:
///
/// * u32::MAX: 未找到匹配值
/// * 位值: 找到的第一个匹配值的索引位
pub fn last_find(hex: &str, value: bool) -> Option<usize> {
    last_find_range(hex, value, 0, hex.len() << 2)
}

/// 在指定范围内从后面开始查找第一个符合条件的值所在的索引
///
/// Arguments:
///
/// * `hex`: 16进制字符串
/// * `value`: 要查找的值
/// * `start`: 起始位置
/// * `end`: 结束位置(半开区间)
///
/// Returns:
///
/// * u32::MAX: 未找到匹配值
/// * 位值: 找到的第一个匹配值的索引位
pub fn last_find_range(hex: &str, value: bool, start: usize, end: usize) -> Option<usize> {
    let hex = hex.as_bytes();
    let hex_bits_len = hex.len() << 2;
    let start = start as isize;
    let end = if end > hex_bits_len {
        hex_bits_len
    } else {
        end
    } as isize;

    if start >= end {
        return None;
    }

    let mut pos = end & !3;

    // 处理尾部非4对齐的bit
    let remain_end = end as usize & 3;
    if remain_end != 0 {
        let b = c2b(hex[pos as usize >> 2]);
        let all_bits = (end - start) as usize;
        let (off, len) = if remain_end <= all_bits {
            (0, remain_end)
        } else {
            (start as usize & 3, all_bits)
        };
        if let Some(idx) = last_find_in_byte(b, value, off, len) {
            return Some(pos as usize + idx);
        }
    }
    pos -= 4;

    // 处理4对齐的bit
    while pos >= start {
        let b = c2b(hex[pos as usize >> 2]);
        if let Some(idx) = last_find_in_byte(b, value, 0, 4) {
            return Some(pos as usize + idx);
        }
        pos -= 4;
    }

    // 处理头部4不对齐的位
    let off = start as usize & 3;
    if start & !3 != end & !3 && off != 0 {
        let b = c2b(hex[pos as usize >> 2]);
        if let Some(idx) = last_find_in_byte(b, value, off, 4 - off) {
            return Some(pos as usize + idx);
        }
    }

    None
}

/// bool数组转成16进制字符串
pub fn bools_to_string(val: &[bool]) -> String {
    let vlen = val.len();
    if vlen == 0 {
        return String::new();
    }

    // 位对齐,不足8位则补齐8位,得到需要的字节长度
    let blen = (vlen + 7) >> 3 << 1;
    let mut chars = Vec::with_capacity(blen);
    chars.resize(blen, b'0');

    // 4倍数的位判断, 写入数组
    for (i, item) in chars.iter_mut().enumerate().take(vlen >> 3 << 1) {
        let mut b = 0;
        let idx = i << 2;
        if val[idx] {
            b |= 8;
        }
        if val[idx + 1] {
            b |= 4;
        }
        if val[idx + 2] {
            b |= 2;
        }
        if val[idx + 3] {
            b |= 1;
        }
        *item = if b < 10 { 48 + b } else { 87 + b };
    }

    // 剩余位数, 如果有, 则写入最后一个字节
    let remaining = vlen & 7;
    if remaining > 0 {
        let mut b = 0;
        let idx = vlen & !7;
        for j in 0..remaining {
            if val[idx + j] {
                b |= 0x80 >> j;
            }
        }
        let b1 = (b >> 4) & 15;
        let b2 = b & 15;
        chars[blen - 2] = if b1 < 10 { 48 + b1 } else { 87 + b1 };
        chars[blen - 1] = if b2 < 10 { 48 + b2 } else { 87 + b2 };
    }

    String::from_utf8(chars).unwrap()
}

/// bool数组转成16进制字符串
pub fn bools_to_compact_string(val: &[bool]) -> String {
    let mut ret = bools_to_string(val);
    let bs = unsafe { ret.as_mut_vec() };
    // 从尾部开始查找为0的数据, 尾部为0的截断
    loop {
        let len = bs.len();
        if len > 1 && bs[len - 1] == b'0' && bs[len - 2] == b'0' {
            bs.pop();
            bs.pop();
        } else {
            break;
        }
    }
    ret
}

/// 16进制字符串转成bool数组
pub fn string_to_bools(hex: &str) -> Vec<bool> {
    let hex_len = hex.len();
    let hex = hex.as_bytes();
    if hex_len == 0 {
        return Vec::new();
    }

    // 生成bool数组并设置初始值
    let bits_len = hex_len << 2;
    let mut ret = Vec::with_capacity(bits_len);
    ret.resize(bits_len, false);

    for (i, item) in hex.iter().enumerate().take(hex_len) {
        let b = c2b(*item);
        let idx = i << 2;
        if (b & 8) != 0 {
            ret[idx] = true;
        }
        if (b & 4) != 0 {
            ret[idx + 1] = true;
        }
        if (b & 2) != 0 {
            ret[idx + 2] = true;
        }
        if (b & 1) != 0 {
            ret[idx + 3] = true;
        }
    }

    ret
}

fn find_in_byte(b: u8, val: bool, off: usize, len: usize) -> Option<usize> {
    for i in off..(off + len) {
        let c = b & (8u8 >> i);
        if val && c != 0 || !val && c == 0 {
            return Some(i);
        }
    }
    None
}

fn last_find_in_byte(b: u8, val: bool, off: usize, len: usize) -> Option<usize> {
    for i in (off..(off + len)).rev() {
        let c = b & (8 >> i);
        if val && c != 0 || !val && c == 0 {
            return Some(i);
        }
    }
    None
}

/// 16进制表示的字符转成二进制表示
fn c2b(mut c: u8) -> u8 {
    if (c & 64) != 0 {
        c += 9
    }
    c & 15
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_bits_get() {
        let hex_data = "1248";
        for i in 1..15 {
            if i % 3 == 0 {
                assert!(super::get(hex_data, i));
            } else {
                assert!(!super::get(hex_data, i));
            }
        }
    }

    #[test]
    fn test_bits_set() {
        let mut hex_data = String::new();
        for i in 1..15 {
            if i % 3 == 0 {
                super::set(&mut hex_data, i, true);
                assert_eq!((i + 8) >> 3 << 1, hex_data.len());
            }
        }

        assert_eq!("1248", hex_data);
    }

    #[test]
    fn test_bits_find() {
        assert_eq!(None, super::find("0000", true));
        assert_eq!(Some(3), super::find("1248", true));
        assert_eq!(Some(6), super::find("0248", true));
        assert_eq!(Some(9), super::find("0048", true));
        assert_eq!(Some(12), super::find("0008", true));
        for i in 0..16 {
            let mut hex = String::from("0000");
            super::set(&mut hex, i, true);
            let some_i = Some(i);
            assert_eq!(some_i, super::find(&hex, true));
            for j in 0..16 {
                for k in j..16 {
                    if j <= i && k >= i {
                        assert_eq!(some_i, super::find_range(&hex, true, j, k + 1));
                    } else {
                        assert_eq!(None, super::find_range(&hex, true, j, k + 1));
                    }
                }
            }
        }
    }

    #[test]
    fn test_bits_last_find() {
        assert_eq!(None, super::last_find("0000", true));
        assert_eq!(Some(12), super::last_find("1248", true));
        assert_eq!(Some(9), super::last_find("1240", true));
        assert_eq!(Some(6), super::last_find("1200", true));
        assert_eq!(Some(3), super::last_find("1000", true));
        for i in 0..16 {
            let mut hex = String::from("0000");
            super::set(&mut hex, i, true);
            let some_i = Some(i);
            assert_eq!(some_i, super::last_find(&hex, true));
            for j in 0..16 {
                for k in j..16 {
                    if j <= i && k >= i {
                        assert_eq!(some_i, super::last_find_range(&hex, true, j, k + 1));
                    } else {
                        assert_eq!(None, super::last_find_range(&hex, true, j, k + 1));
                    }
                }
            }
        }
    }

    #[test]
    fn test_bits_bools_to_string() {
        assert_eq!(
            "1248",
            super::bools_to_string(&[
                false, false, false, true, false, false, true, false, false, true, false, false,
                true, false, false, false
            ])
        );

        assert_eq!(
            "1240",
            super::bools_to_compact_string(&[
                false, false, false, true, false, false, true, false, false, true, false, false,
                false, false, false, false
            ])
        );

        assert_eq!(
            "12",
            super::bools_to_compact_string(&[
                false, false, false, true, false, false, true, false, false, false, false, false,
                false, false, false, false
            ])
        );

        assert_eq!(
            "1240",
            super::bools_to_compact_string(&[
                false, false, false, true, false, false, true, false, false, true
            ])
        );
    }

    #[test]
    fn test_bits_string_to_bools() {
        assert_eq!(
            vec![
                false, false, false, true, false, false, true, false, false, true, false, false,
                true, false, false, false
            ],
            super::string_to_bools("1248")
        );

        assert_eq!(
            vec![
                false, false, false, true, false, false, true, false, false, true, false, false,
                false, false, false, false
            ],
            super::string_to_bools("1240")
        );
    }
}
