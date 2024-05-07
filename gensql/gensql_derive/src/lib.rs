//! generator table struct const value
//!
//! Examples
//!
//! ```rust
//! #[table("t_user")]
//! struct User {
//!     #[table(id)]
//!     user_id: u64,
//!
//!     user_name: String,
//!
//!     #[table(field = "type")]
//!     #[serde(rename = "type")]
//!     type_: u32,
//!
//!     #[table(not_field)]
//!     role_name: String,
//! }
//!
//! #[table]
//! struct UserExt {
//!     #[serde(flatten)]
//!     inner: User,
//!
//!     login_type_id: u16,
//!     login_count: u32,
//!
//!     #[table(ignore)]
//!     login_type: u32,
//! }
//! ```
// author: kiven
// slince 2023-06-15

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{punctuated::Punctuated, DeriveInput, Expr, Field, Ident, LitStr, Meta, Token};

type StructFields = syn::punctuated::Punctuated<syn::Field, syn::Token!(,)>;

const TABLE_ATTR: &str = "table";
const TABLE_ATTR_ID: &str = "id";
const TABLE_ATTR_FIELD: &str = "field";
const TABLE_ATTR_NOT_FIELD: &str = "not_field";
const TABLE_ATTR_IGNORE: &str = "ignore";
const SERDE_ATTR: &str = "serde";
const SERDE_ATTR_FLATTEN: &str = "flatten";

struct TagField<'a> {
    id: bool,
    not_field: bool,
    ignore: bool,
    serde_flatten: bool,
    field: &'a Field,
    alias: String,
}

#[proc_macro_attribute]
pub fn table(attr: TokenStream, item: TokenStream) -> TokenStream {
    let struct_data: DeriveInput = syn::parse(item).unwrap();
    let struct_name = struct_data.ident.clone();
    let table_name = parse_table_name(attr);
    let struct_meta = &struct_data.attrs;
    let raw_fields = get_fields_from_derive_input(&struct_data).unwrap();
    let all_fields = parse_struct_fields(&raw_fields);

    let table_field_arr = get_fields_array(&all_fields, true);
    let all_field_arr = get_fields_array(&all_fields, false);
    let table_fields_len = all_fields
        .iter()
        .filter(|v| !v.not_field && !v.ignore && !v.serde_flatten)
        .count();
    let all_fields_len = all_fields
        .iter()
        .filter(|v| !v.ignore && !v.serde_flatten)
        .count();
    let id_field = all_fields.iter().find(|v| v.id);
    let table_field_name_arr = all_fields
        .iter()
        .filter(|v| !v.not_field && !v.ignore && !v.serde_flatten)
        .map(|v| v.alias.to_string())
        .collect::<Vec<_>>();

    let mut out = proc_macro2::TokenStream::new();

    // struct声明
    {
        let struct_vis = &struct_data.vis;
        let fields_quote = generator_fields_decalare(&all_fields);
        out.extend(quote!(
            #(#struct_meta)*
            #[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone)]
            #[serde(rename_all = "camelCase")]
            #struct_vis struct #struct_name {
                #(#fields_quote)*
            }
        ));
    }

    // struct关联的常量声明
    {
        let table_name_quote = table_name
            .as_ref()
            .map(|name| {
                quote! {
                    /// 对象所属数据库表名称
                    pub const TABLE_NAME: &'static str = #name;
                }
            })
            .unwrap_or(proc_macro2::TokenStream::new());
        let all_field_arr = all_fields.iter()
            .filter(|v| !v.serde_flatten && !v.ignore)
            .map(|v| v.alias.to_string());
        let const_fields_quote = get_const_field_names(&all_fields);

        out.extend(quote!(
            impl #struct_name {
                #table_name_quote
                #(#const_fields_quote)*
                /// 数据库表的字段列表
                pub const FIELDS: [&'static str; #table_fields_len] = [#(#table_field_name_arr),*];
                /// 对象所有有效可映射到查询结果的字段名列表
                pub const ALL_FIELDS: [&'static str; #all_fields_len] = [#(#all_field_arr),*];
            }
        ));
    }

    // 增删改查函数实现
    if table_name.is_some() && id_field.is_some() {
        let table_name = table_name.as_ref().map_or("", |s| s.as_str());
        let placeholder = vec!["?"; table_fields_len].join(", ");
        let table_field_names = table_field_name_arr.join(", ");
        let insert_sql = format!(
            "insert into {} ({}) values({})",
            table_name, table_field_names, placeholder
        );
        let id_name = id_field.unwrap().field.ident.as_ref().unwrap();
        let id_type = &id_field.unwrap().field.ty;
        let where_sql = format!("where {} = ?", id_name.to_string());
        let delete_sql = format!("delete from {} {}", table_name, where_sql);
        let update_fields: Vec<_> = all_fields
            .iter()
            .filter(|v| !v.id && !v.not_field && !v.ignore && !v.serde_flatten)
            .collect();
        let update_sql = format!(
            "update {} set {} {}",
            table_name,
            update_fields.iter().fold(String::new(), |mut s, f| {
                if !s.is_empty() {
                    s.push_str(", ")
                }
                s.push_str(&f.alias);
                s.push_str(" = ?");
                s
            }),
            where_sql
        );
        let update_fields: Vec<_> = update_fields.iter()
            .map(|f| &f.field.ident).collect();
        let select_sql = format!(
            "select {} from {} {}",
            table_field_names, table_name, where_sql
        );
        let select_valid_dyn_values = all_fields
            .iter()
            .filter(|v| !v.not_field && !v.ignore && !v.serde_flatten)
            .map(|v| {
                let f = &v.field.ident;
                let a = &v.alias;
                quote!(
                    if let Some(v) = value.#f {
                        fields.push(#a);
                        params.push(v.into());
                    }
                )
            });
        let mut update_valid_dyn_values: Vec<_> = all_fields
            .iter()
            .filter(|v| !v.id && !v.not_field && !v.ignore && !v.serde_flatten)
            .map(|v| {
                let f = &v.field.ident;
                let a = &v.alias;
                quote!(
                    if let Some(v) = value.#f {
                        fields.push(#a);
                        params.push(v.into());
                    }
                )
            }).collect();
        update_valid_dyn_values.push(
            quote!(
                if let Some(v) = value.#id_name {
                    params.push(v.into());
                }
            )
        );

        out.extend(quote! {
            impl #struct_name {
                /// 插入记录，不忽略None值，返回(插入记录数量, 自增ID的值)
                pub async fn insert(value: #struct_name) -> gensql::DbResult<(u32, u32)> {
                    let (sql, params) = Self::prepare_insert(value);
                    gensql::db_log_sql_params(&sql, &params);
                    gensql::sql_insert(sql, params).await
                }

                /// 删除记录，参数为记录id，返回删除是否成功标志
                pub async fn delete_by_id(id: #id_type) -> gensql::DbResult<bool> {
                    let sql = Self::sql_delete_by_id();
                    let params: Vec<gensql::Value> = vec![id.into()];
                    gensql::db_log_sql_params(&sql, &params);
                    Ok(gensql::sql_exec(sql, params).await? > 0)
                }

                /// 更新记录，不忽略None值，以id为条件进行定位修改，返回修改是否成功标志
                pub async fn update_by_id(value: #struct_name) -> gensql::DbResult<bool> {
                    let (sql, params) = Self::prepare_update_by_id(value);
                    gensql::db_log_sql_params(&sql, &params);
                    Ok(gensql::sql_exec(sql, params).await? > 0)
                }

                /// 查找记录，参数为记录id
                pub async fn select_by_id(id: #id_type) -> gensql::DbResult<Option<#struct_name>> {
                    let sql = Self::sql_select_by_id();
                    let params: Vec<gensql::Value> = vec![id.into()];
                    gensql::db_log_sql_params(&sql, &params);
                    gensql::sql_query_one(sql, params).await
                }

                /// 插入记录，忽略None值，返回(插入记录数量, 自增ID的值)
                pub async fn insert_selective(value: #struct_name) -> gensql::DbResult<(u32, u32)> {
                    let (sql, params) = Self::prepare_insert_selective(value);
                    gensql::db_log_sql_params(&sql, &params);
                    gensql::sql_insert(sql, params).await
                }

                /// 更新记录，忽略None值，以id为条件进行定位修改，返回修改是否成功标志
                pub async fn update_by_id_selective(value: #struct_name) -> gensql::DbResult<bool> {
                    let (sql, params) = Self::prepare_update_by_id_selective(value);
                    gensql::db_log_sql_params(&sql, &params);
                    Ok(gensql::sql_exec(sql, params).await? > 0)
                }

                /// 插入记录，不忽略None值，返回(插入记录数量, 自增ID的值)
                pub fn prepare_insert(value: #struct_name) -> (&'static str, Vec<gensql::Value>) {
                    let sql = #insert_sql;
                    let params: Vec<gensql::Value> = vec![#(value.#table_field_arr.into()),*];
                    (sql, params)
                }

                /// 删除记录，参数为记录id，返回删除是否成功标志
                pub fn sql_delete_by_id() -> &'static str {
                    #delete_sql
                }

                /// 更新记录，不忽略None值，以id为条件进行定位修改，返回修改是否成功标志
                pub fn prepare_update_by_id(value: #struct_name) -> (&'static str, Vec<gensql::Value>) {
                    let sql = #update_sql;
                    let params: Vec<gensql::Value> = vec![#(value.#update_fields.into()),* , value.#id_name.into()];
                    (sql, params)
                }

                /// 查找记录，参数为记录id
                pub fn sql_select_by_id() -> &'static str {
                    #select_sql
                }

                /// 插入记录，忽略None值，返回(插入记录数量, 自增ID的值)
                pub fn prepare_insert_selective(value: #struct_name) -> (String, Vec<gensql::Value>) {
                    // 找出所有不为None的值和对应的字段名
                    let mut fields = Vec::with_capacity(32);
                    let mut params = Vec::<gensql::Value>::with_capacity(32);
                    #(#select_valid_dyn_values)*

                    // 构造sql语句
                    let mut sql = String::with_capacity(256);
                    sql.push_str(concat!("insert into ", #table_name, " ("));
                    for f in &fields {
                        sql.push_str(f);
                        sql.push_str(", ");
                    }
                    sql.truncate(sql.len() - 2);
                    sql.push_str(") values (");
                    for _ in &fields {
                        sql.push_str("?, ");
                    }
                    sql.truncate(sql.len() - 2);
                    sql.push(')');

                    (sql, params)
                }

                /// 更新记录，忽略None值，以id为条件进行定位修改，返回修改是否成功标志
                pub fn prepare_update_by_id_selective(value: #struct_name) -> (String, Vec<gensql::Value>) {
                    // 找出所有不为None的值和对应的字段名, 注意：id需要排到最后，因为id是where条件
                    let mut fields = Vec::with_capacity(32);
                    let mut params = Vec::<gensql::Value>::with_capacity(32);
                    #(#update_valid_dyn_values)*

                    // 构造sql语句
                    let mut sql = String::with_capacity(256);
                    sql.push_str(concat!("update ", #table_name, " set "));
                    for f in &fields {
                        sql.push_str(f);
                        sql.push_str(" = ?, ");
                    }
                    sql.truncate(sql.len() - 2);
                    sql.push(' ');
                    sql.push_str(#where_sql);

                    (sql, params)
                }
            }
        });
    }

    // 数据库行对象转换函数
    {
        let from_row_mut_quote: Vec<_> = all_fields.iter()
            .filter(|v| !v.ignore && !v.serde_flatten)
            .map(|v| {
                let f = &v.field.ident;
                let c = &v.alias;
                quote!(#f: row.take(#c).flatten(),)
            }).collect();
        let flatten_field_arr_orig = all_fields
            .iter()
            .filter(|f| f.serde_flatten && !f.ignore)
            .map(|f| (&f.field.ident, &f.field.ty));

        let flatten_field_arr = flatten_field_arr_orig
            .clone()
            .map(|(ident, ty)| quote!(#ident: <#ty>::from_row_mut(row),));

        let flatten_field_arr_mut = flatten_field_arr_orig
            .clone()
            .map(|(ident, ty)| quote!(#ident: <#ty>::fast_from_row_mut(idx_arr, i, row),));

        let flatten_field_arr_idx = flatten_field_arr_orig
            .clone()
            .map(|(_, ty)| quote!(<#ty>::fast_map_index_mut(idx_vec, columns);));

        let has_ignore = all_fields.iter().find(|v| v.ignore);
        let default_quote = if has_ignore.is_none() {
            quote!()
        } else {
            quote!(..Default::default())
        };

        out.extend(quote! {
            impl #struct_name {
                pub fn from_row_mut(row: &mut gensql::Row) -> #struct_name {
                    #struct_name {
                        #(#from_row_mut_quote)*
                        #(#flatten_field_arr)*
                        #default_quote
                    }
                }

                pub fn fast_from_row_mut(idx_arr: &[i32], i: &mut usize, row: &mut gensql::Row) -> #struct_name {
                    #struct_name {
                        #(#all_field_arr: #struct_name::take(idx_arr, i, row).flatten(),)*
                        #(#flatten_field_arr_mut)*
                        #default_quote
                    }
                }

                pub fn fast_map_index_mut(idx_vec: &mut Vec<i32>, columns: &[&[u8]]) {
                    for field in Self::ALL_FIELDS {
                        let mut idx = -1;
                        for (i, col) in columns.iter().enumerate() {
                            if field.as_bytes() == *col {
                                idx = i as i32;
                                break;
                            }
                        }
                        idx_vec.push(idx);
                    }
                    #(#flatten_field_arr_idx)*
                }

                fn take<T: gensql::FromValue>(idx_arr: &[i32], i: &mut usize, row: &mut gensql::Row) -> Option<T> {
                    let val = match idx_arr[*i] {
                        -1 => None,
                        idx => row.take(idx as usize).flatten()
                    };
                    *i += 1;
                    val
                }
            }

            impl gensql::FromRow for #struct_name {
                fn from_row_opt(mut row: gensql::Row) -> std::result::Result<#struct_name, gensql::FromRowError> {
                    Ok(Self::from_row_mut(&mut row))
                }
            }

            impl gensql::FastFromRow for #struct_name {
                fn fast_from_row(idx_arr: &[i32], mut row: gensql::Row) -> #struct_name {
                    let mut i = 0;
                    #struct_name::fast_from_row_mut(idx_arr, &mut i, &mut row)
                }

                fn fast_map_index(columns: &[&[u8]]) -> Vec<i32> {
                    let mut idx_vec = Vec::with_capacity(32);
                    #struct_name::fast_map_index_mut(&mut idx_vec, columns);
                    idx_vec
                }
            }
        });
    }

    out.into()
}

/// 获取所有有效字段(排除属性为 #[table(ignore)] 和 #[serde(flatten)] 的字段)
fn parse_struct_fields<'a>(fields: &'a StructFields) -> Vec<TagField<'a>> {
    let mut result = Vec::new();

    // 循环结构中的所有字段
    for field in fields {
        let mut tag_field = TagField {
            id: false,
            not_field: false,
            ignore: false,
            serde_flatten: false,
            field,
            alias: String::with_capacity(0),
        };
        let mut finded = false;

        // 循环字段中的所有属性, 这几个属性是互斥的，找到任何一个都结束循环
        for attr in &field.attrs {
            if attr.path().is_ident(TABLE_ATTR) {
                let nested = attr
                    .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                    .unwrap();
                for meta in nested {
                    // 属性是 #[table(xxx)] 类型
                    if let Meta::Path(path) = meta {
                        if path.is_ident(TABLE_ATTR_IGNORE) {
                            tag_field.ignore = true;
                            finded = true;
                        } else if path.is_ident(TABLE_ATTR_NOT_FIELD) {
                            tag_field.not_field = true;
                            finded = true;
                        } else if path.is_ident(TABLE_ATTR_ID) {
                            tag_field.id = true;
                            finded = true;
                        }
                    } else if let Meta::NameValue(nv) = meta {
                        if nv.path.is_ident(TABLE_ATTR_FIELD) {
                            if let Expr::Lit(syn::ExprLit {lit: syn::Lit::Str(ref s), ..}) = nv.value {
                                tag_field.alias = s.value();
                            }
                        }
                    }
                    if finded {
                        break;
                    }
                }
            } else if attr.path().is_ident(SERDE_ATTR) {
                let nested = attr
                    .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                    .unwrap();
                for meta in nested {
                    if let Meta::Path(path) = meta {
                        if path.is_ident(SERDE_ATTR_FLATTEN) {
                            tag_field.serde_flatten = true;
                        }
                    }
                }
            }

            if finded {
                break;
            }
        }

        if tag_field.alias.is_empty() {
            tag_field.alias = tag_field.field.ident.as_ref().unwrap().to_string();
        }

        result.push(tag_field);
    }
    result
}

fn get_fields_from_derive_input(d: &syn::DeriveInput) -> syn::Result<&StructFields> {
    if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = d.data
    {
        return Ok(named);
    }
    Err(syn::Error::new_spanned(
        d,
        "Must define on a Struct, not Enum".to_string(),
    ))
}

fn parse_table_name(attr: TokenStream) -> Option<String> {
    if !attr.is_empty() {
        let table_name: LitStr =
            syn::parse(attr).expect("failed to parse attributes, expected a string");
        Some(table_name.value())
    } else {
        None
    }
}

fn generator_fields_decalare(fields: &[TagField]) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .map(|tag_field| {
            let name = &tag_field.field.ident;
            let ty = &tag_field.field.ty;
            let meta: Vec<_> = tag_field
                .field
                .attrs
                .iter()
                .filter(|v| !v.path().is_ident(TABLE_ATTR))
                .collect();

            if tag_field.serde_flatten && !tag_field.ignore {
                quote!(
                    #(#meta)*
                    pub #name: #ty,
                )
            } else {
                quote!(
                    #(#meta)*
                    #[serde(skip_serializing_if = "Option::is_none")]
                    #[serde(default)]
                    pub #name: Option<#ty>,
                )
            }
        })
        .collect()
}

fn get_const_field_names(fields: &[TagField]) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .filter(|tag_field| !tag_field.ignore && !tag_field.serde_flatten)
        .map(|tag_field| {
            let id = &tag_field.field.ident.as_ref().unwrap().to_string();
            let id_upper = id.to_uppercase();
            let id_uppercase = Ident::new(&id_upper, Span::call_site());
            let alias = &tag_field.alias;
            quote! {
                pub const #id_uppercase: &'static str = #alias;
            }
        })
        .collect()
}

fn get_fields_array<'a>(fields: &'a [TagField], only_table: bool) -> Vec<&'a Ident> {
    fields
        .iter()
        .filter(|f| !f.ignore && !f.serde_flatten && (!only_table || !f.not_field))
        .map(|f| f.field.ident.as_ref().unwrap())
        .collect()
}
