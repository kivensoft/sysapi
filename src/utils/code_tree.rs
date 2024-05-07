//! 基于字符串的多级结构辅助函数

use compact_str::CompactString;

pub trait CodeTree {
    /// 获取代码的级别
    ///
    /// Arguments:
    ///
    /// * `code`: 代码
    ///
    fn get_level(code: &str) -> usize;

    /// 获取指定级别的代码
    ///
    /// Arguments:
    ///
    /// * `code`: 代码
    /// * `level`: 要获取的级别
    ///
    fn get_super(code: &str, level: usize) -> &str;

    /// 获取父级代码
    ///
    /// Arguments:
    ///
    /// * `code`: 代码
    ///
    fn get_parent(code: &str) -> &str;

    fn gen_value(parent: &str, index: usize) -> CompactString;

    fn cur_index(value: &str) -> usize;
}

// pub fn convert_to_tree<'a, T>(
//     source: &'a Vec<&'a T>,
//     root_key: &str,
//     get_parent_key: fn(&T) -> &K,
//     get_key: fn(&T) -> &K,
//     get_child: fn(&T) -> &Vec<T>,
//     set_child: fn(&mut T, &'a Vec<T>),
//     is_leaf_node: Option<fn(&T) -> bool>,
//     compare_key: Option<fn(v1: &K, v2: &K) -> i32>,
// ) -> Vec<&'a T> {
//     // 将vec转换为双向列表，方便
//     // let source: LinkedList<&T> = source.iter().map(|v| *v).collect();
//     let mut top = Vec::new();

//     // 如果compare_key不为None，则列表尚未排序，首先要进行排序

//     top
// }
