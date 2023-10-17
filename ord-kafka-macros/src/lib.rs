use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Trace a function with a given operation name. Example usage:
/// `#[trace]`, the span name will be the function's name.
/// Alternatively, `#[trace("my_span_name")]` will use the given span name
#[proc_macro_attribute]
pub fn trace(args: TokenStream, input: TokenStream) -> TokenStream {
  let func = parse_macro_input!(input as ItemFn);
  let func_name = &func.sig.ident;
  let inputs = &func.sig.inputs;
  let output = &func.sig.output;
  let block = &func.block;

  let name = if args.is_empty() {
    func_name.to_string()
  } else {
    parse_macro_input!(args as syn::LitStr).value()
  };

  let expanded = quote! {
      fn #func_name(#inputs) #output {
          let tracer = opentelemetry::global::tracer("ord-kafka");
          tracer.in_span(#name, |_| {
              #block
          })
      }
  };

  TokenStream::from(expanded)
}
