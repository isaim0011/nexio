//! Procedural macros for the `nexio` runtime: `#[nexio::main]` and `#[nexio::test]`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Lit, Meta};

enum ShardMode {
    None,
    Auto,
    Fixed(syn::Expr),
}

fn parse_main_args(args: TokenStream) -> syn::Result<ShardMode> {
    if args.is_empty() {
        return Ok(ShardMode::None);
    }
    let meta: Meta = syn::parse(args)?;
    match meta {
        Meta::NameValue(nv) if nv.path.is_ident("shards") => {
            if let syn::Expr::Lit(expr_lit) = &nv.value {
                if let Lit::Str(s) = &expr_lit.lit {
                    if s.value() == "auto" {
                        return Ok(ShardMode::Auto);
                    }
                }
            }
            Ok(ShardMode::Fixed(nv.value))
        }
        Meta::Path(p) if p.is_ident("auto") => Ok(ShardMode::Auto),
        _ => Err(syn::Error::new_spanned(
            meta,
            "expected `#[nexio::main]`, `#[nexio::main(shards = N)]`, or `#[nexio::main(shards = \"auto\")]`",
        )),
    }
}

/// Marks an `async fn main` to be executed on the `nexio` runtime.
///
/// # Examples
/// ```ignore
/// #[nexio::main]
/// async fn main() {
///     println!("hello from nexio");
/// }
/// ```
///
/// Multi-core sharded mode:
/// ```ignore
/// #[nexio::main(shards = "auto")]
/// async fn main() {
///     // runs on every CPU core with independent reactors
/// }
/// ```
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    let mode = match parse_main_args(args) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(item as ItemFn);

    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input.sig.fn_token,
            "#[nexio::main] requires an `async fn`",
        )
        .to_compile_error()
        .into();
    }

    let attrs = &input.attrs;
    let vis = &input.vis;
    let mut sig = input.sig.clone();
    sig.asyncness = None;
    let block = &input.block;

    let body = match mode {
        ShardMode::None => quote! {
            ::nexio::block_on(async move #block)
        },
        ShardMode::Auto => quote! {
            ::nexio::run_sharded_auto(|| async move #block);
        },
        ShardMode::Fixed(n) => quote! {
            ::nexio::run_sharded(#n, || async move #block);
        },
    };

    quote! {
        #(#attrs)*
        #vis #sig {
            #body
        }
    }
    .into()
}

/// Marks an `async fn` test to be executed on the `nexio` runtime.
///
/// # Examples
/// ```ignore
/// #[nexio::test]
/// async fn my_test() {
///     nexio::time::sleep(std::time::Duration::from_millis(1)).await;
/// }
/// ```
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[nexio::test] does not take arguments",
        )
        .to_compile_error()
        .into();
    }
    let input = parse_macro_input!(item as ItemFn);

    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input.sig.fn_token,
            "#[nexio::test] requires an `async fn`",
        )
        .to_compile_error()
        .into();
    }

    let attrs = &input.attrs;
    let vis = &input.vis;
    let mut sig = input.sig.clone();
    sig.asyncness = None;
    let block = &input.block;

    quote! {
        #[::core::prelude::v1::test]
        #(#attrs)*
        #vis #sig {
            ::nexio::block_on(async move #block)
        }
    }
    .into()
}
