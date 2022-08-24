extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse2, parse_macro_input, ItemFn, LitStr};

/// A decorator is applied to the target function's body
/// and wraps it with the tracing `child_span` function.
///
/// # Panics
/// - failed to parse the decorated function
/// - failed to parse the an argument of the decorator (the span name)
/// - failed to build a new function body (internal error, should never happen)
#[proc_macro_attribute]
pub fn otm_child_span(attr: TokenStream, item: TokenStream) -> TokenStream {
    let span_name = parse_macro_input!(attr as LitStr).value();
    let mut item_fn = parse_macro_input!(item as ItemFn);
    let body = item_fn.block;
    let new_body = quote! {
        {
            child_span(stringify!(#span_name), || {
                #body
            })
        }
    };
    item_fn.block = parse2(new_body).unwrap();
    item_fn.into_token_stream().into()
}
