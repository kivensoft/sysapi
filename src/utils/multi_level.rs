//! 基于字符串的多级结构辅助函数

use anyhow_ext::{bail, Context, Result};
use compact_str::{format_compact, CompactString};

pub struct MultiLevel;

impl MultiLevel {
    /// 获取指定层级的大小
    pub fn get_level(levels: &[u16], depth: Option<usize>) -> Option<u16> {
        match depth {
            Some(depth) if depth < levels.len() => Some(levels[depth]),
            _ => None,
        }
	}

    /// 获取指定层级的总长度
    pub fn get_length(levels: &[u16], depth: Option<usize>) -> u16 {
        match depth {
            Some(depth) if depth < levels.len() =>
                levels.iter().take(depth + 1).sum(),
            _ => 0,
        }
	}

    /// 获取编码的最后一级编码
    pub fn last_code<'a>(levels: &[u16], code: Option<&'a str>) -> Result<Option<&'a str>> {
        match Self::get_level(levels, Self::parse_depth(levels, code)?) {
            Some(size) => {
                match code {
                    Some(code) => Ok(Some(&code[code.len() - size as usize..])),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn parent_code<'a>(levels: &[u16], code: Option<&'a str>) -> Result<Option<&'a str>> {
        match Self::get_level(levels, Self::parse_depth(levels, code)?) {
            Some(size) => {
                match code {
                    Some(code) => Ok(Some(&code[..code.len() - size as usize])),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn next_sibling(levels: &[u16], code: Option<&str>) -> Result<CompactString> {
        let mut parent_code = CompactString::with_capacity(32);
        let (current_code, size) = match code {
            Some(code) if !code.is_empty() => {
                let depth = Self::parse_depth2(levels, code)?;
                let size = levels[depth];
                let idx = code.len() - size as usize;
                parent_code.push_str(&code[..idx]);
                let curr_code: &str = &code[idx..];
                let cc = curr_code.parse().with_context(
                    || format!("MultiLevel::_next_sibling fail: curr_code = {}", curr_code))?;
                (cc, size)
            }
            _ => (0, levels[0])
        };

        Ok(format_compact!("{}{:02$}", parent_code, current_code + 1, size as usize))
    }

    fn like_children(levels: &[u16], code: Option<&str>) -> Result<CompactString> {
        let mut cs = CompactString::with_capacity(32);
        let depth = match code {
            Some(code) if !code.is_empty() => {
                cs.push_str(code);
                Self::parse_depth2(levels, code)?
            }
            _ => 0
        };
        for _ in 0..levels[depth] {
            cs.push('_');
        }
        Ok(cs)
    }

    fn like_all_children(code: Option<&str>) -> Option<CompactString> {
        code.map(|code| format_compact!("{}_%", code))
    }

    fn parse_depth(levels: &[u16], code: Option<&str>) -> Result<Option<usize>> {
        match code {
            Some(code) if !code.is_empty() => Self::parse_depth2(levels, code).map(Some),
            _ => Ok(None),
        }
	}

    fn parse_depth2(levels: &[u16], code: &str) -> Result<usize> {
        let (clen, mut sum) = (code.len() as u16, 0);
        for (i, lv) in levels.iter().enumerate() {
            sum += lv;
            if sum == clen {
                return Ok(i);
            }
            if sum > clen {
                break;
            }
        }
        bail!("MultiLevel::parse_depth fail, code = {}, levels = {:?}", code, levels)
	}

}
