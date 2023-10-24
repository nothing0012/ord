use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitStr};

fn extract_span_name(arg: TokenStream) -> Option<LitStr> {
  let parsed: Result<LitStr, _> = syn::parse(arg);
  parsed.ok()
}

#[proc_macro_attribute]
pub fn trace(args: TokenStream, input: TokenStream) -> TokenStream {
  let function = parse_macro_input!(input as ItemFn);
  let maybe_span_name = extract_span_name(args);

  let fn_name = &function.sig.ident;
  let fn_vis = &function.vis;
  let fn_block = &function.block;
  let fn_inputs = &function.sig.inputs;
  let fn_output = &function.sig.output;
  let fn_generics = &function.sig.generics;
  let asyncness = &function.sig.asyncness;

  let span_name = if let Some(msg) = maybe_span_name {
    msg.value()
  } else {
    fn_name.to_string()
  };

  let output = quote! {
      #fn_vis #asyncness fn #fn_name #fn_generics(#fn_inputs) #fn_output {
          use opentelemetry::trace::Span;
          use opentelemetry::trace::Tracer;
          use opentelemetry::trace::TraceContextExt;

          let tracer = opentelemetry::global::tracer("ord-kafka");
          let span = tracer.start(#span_name);
          let cx = opentelemetry::Context::current_with_span(span);
          let _guard = cx.clone().attach();

          let result = {
            #fn_block
          };

          result
      }
  };

  output.into()
}
