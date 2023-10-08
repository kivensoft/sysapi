//! generator table struct const value
// author: kiven
// slince 2023-06-15

use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::{DeriveInput, Ident, LitStr, Field};
use quote::quote;

type StructFields = syn::punctuated::Punctuated<syn::Field,syn::Token!(,)>;

#[proc_macro_derive(Table, attributes(table))]
pub fn derive_table(input: TokenStream) -> TokenStream {
    // 基于 input 构建 AST 语法树
    let ast: DeriveInput = syn::parse(input).unwrap();

    // let Data::Struct(struct_data) = ast.data else{
    //     panic!("MyDefault derive macro must use in struct");
    // };

    let struct_name = ast.ident.clone();
    let fields = get_fields_from_derive_input(&ast).unwrap();
    let table_name = get_table_name(&ast);
    let const_field_names = gen_fields_const(&fields);
    let id_field = get_id_from_fields(&fields);
    let fields_len = fields.len();
    let fields_array = gen_fields_array(&struct_name, &fields);

    let table_declare = match &table_name {
        Some(table_name) => quote!(
            pub const TABLE: &str = #table_name;
        ),
        None => quote!(),
    };
    let id_declare = match id_field {
        Some(field) => {
            let field = field.ident.as_ref().unwrap().to_string().to_uppercase();
            let field = Ident::new(&field, Span::call_site());
            quote!(
                pub const ID: &str = #struct_name::#field;
            )
        },
        None => quote!(),
    };
    let fields_declare = match &table_name {
        Some(_) => quote!(
            pub const FIELDS: &[&'static str; #fields_len] = &[#fields_array];
        ),
        None => quote!(),
    };

    let gen = quote! {
        impl #struct_name {
            #table_declare
            #id_declare
            #const_field_names
            #fields_declare
        }
    };

    gen.into()
}

fn gen_fields_const(fields: &StructFields) -> proc_macro2::TokenStream {
    let mut ret = quote!();
    for field in fields {
        let id = field.ident.as_ref().unwrap();
        let id_upper = id.to_string().to_uppercase();
        let idu = Ident::new(&id_upper, Span::call_site());
        ret.extend(quote! {
            pub const #idu: &str = stringify!(#id);
        });
    }
    ret
}

fn gen_fields_array(s: &Ident, fields: &StructFields) -> proc_macro2::TokenStream {
    let mut ret = quote!();
    for field in fields {
        let id = field.ident.as_ref().unwrap();
        let id_upper = id.to_string().to_uppercase();
        let idu = Ident::new(&id_upper, Span::call_site());
        ret.extend(quote! {
            #s::#idu,
        });
    }
    ret
}

fn get_fields_from_derive_input(d: &syn::DeriveInput) -> syn::Result<&StructFields> {
    if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = d.data{
        return Ok(named)
    }
    Err(syn::Error::new_spanned(d, "Must define on a Struct, not Enum".to_string()))
}

fn get_table_name(d: &syn::DeriveInput) -> Option<String> {
    let mut r = None;
    for attr in &d.attrs {
        if attr.path().is_ident("table") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value().unwrap();
                    let s: LitStr = value.parse().unwrap();
                    r = Some(s);
                }
                Ok(())
            }).unwrap();
        }
    }

    r.map(|v| v.value())
}

fn get_id_from_fields(fields: &StructFields) -> Option<&Field> {
    let mut res = None;
    for field in fields {
        for attr in &field.attrs {
            if attr.path().is_ident("table") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("id") {
                        res = Some(field);
                    }
                    Ok(())
                }).unwrap();
            }
        }
    }

    res
}
