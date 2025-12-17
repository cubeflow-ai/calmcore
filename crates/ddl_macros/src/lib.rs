use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat};

/// 协调者路由宏 - 自动生成路由逻辑
///
/// 使用方式：
/// ```
/// #[coordinator_route]
/// async fn create_table(self, _ctx: Context, schema: Schema, ...) -> Result<(), CoreError> {
///     // 这里直接写协调者的实现逻辑
///     let table_info = TableInfo { ... };
///     self.catalog.create_table(table_info).await?;
///     Ok(())
/// }
/// ```
///
/// 宏会自动生成：
/// 1. 如果是协调者节点 -> 执行方法体的逻辑
/// 2. 如果不是协调者 -> 通过 RPC 转发到协调者
#[proc_macro_attribute]
pub fn coordinator_route(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    generate_coordinator_route(input_fn)
}

/// 本地执行宏 - 标记方法为本地执行（不路由）
///
/// 使用方式：
/// ```
/// #[local_only]
/// async fn drop_partition_local(self, _ctx: Context, ...) -> Result<(), CoreError> {
///     // 本地执行逻辑
/// }
/// ```
#[proc_macro_attribute]
pub fn local_only(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    // 本地执行方法直接返回，只添加日志
    let sig = &input_fn.sig;
    let fn_name = &sig.ident;
    let fn_name_str = fn_name.to_string();
    let body = &input_fn.block;
    let vis = &input_fn.vis;
    let asyncness = &sig.asyncness;
    let inputs = &sig.inputs;
    let output = &sig.output;

    // 提取参数名用于日志
    let log_params = extract_log_params(inputs);

    let expanded = quote! {
        #vis #asyncness fn #fn_name(#inputs) #output {
            log::info!("🔧 [Local] Executing {} with params: {}", #fn_name_str, #log_params);
            #body
        }
    };

    TokenStream::from(expanded)
}

fn generate_coordinator_route(input_fn: ItemFn) -> TokenStream {
    let sig = &input_fn.sig;
    let fn_name = &sig.ident;
    let fn_name_str = fn_name.to_string();
    let body = &input_fn.block;
    let vis = &input_fn.vis;
    let asyncness = &sig.asyncness;
    let inputs = &sig.inputs;
    let output = &sig.output;

    // 提取参数（跳过 self 和 _ctx）用于转发
    let params: Vec<_> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    let ident = &pat_ident.ident;
                    if ident != "_ctx" {
                        return Some(ident);
                    }
                }
            }
            None
        })
        .collect();

    // 提取参数用于日志
    let log_params = extract_log_params(inputs);

    let expanded = quote! {
        #vis #asyncness fn #fn_name(#inputs) #output {
            let trace_info = format!("[{}]", #fn_name_str);

            if self.am_i_coord_node() {
                log::info!("📋 [CoordNode] {} executing with params: {}", trace_info, #log_params);
                // 协调者直接执行方法体
                #body
            } else {
                log::info!("📤 [Node] {} forwarding to coordinator with params: {}", trace_info, #log_params);
                // 非协调者通过 RPC 转发
                self.coord_client()
                    .await?
                    .#fn_name(tarpc::context::current(), #(#params),*)
                    .await
                    .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
            }
        }
    };

    TokenStream::from(expanded)
}

fn extract_log_params(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    let params: Vec<_> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    let ident = &pat_ident.ident;
                    let ident_str = ident.to_string();
                    if ident_str != "self" && ident_str != "_ctx" {
                        return Some(quote! {
                            format!("{}={:?}", #ident_str, #ident)
                        });
                    }
                }
            }
            None
        })
        .collect();

    if params.is_empty() {
        quote! { "no params" }
    } else {
        quote! {
            vec![#(#params),*].join(", ")
        }
    }
}
