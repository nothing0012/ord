use axum::{extract::MatchedPath, http::Request, middleware::Next, response::Response};
use opentelemetry::{
  global,
  trace::{Span, Tracer},
  Key,
};

pub(crate) async fn tracing_layer<B>(request: Request<B>, next: Next<B>) -> Response {
  let tracer = global::tracer("ord-kafka");
  let cx = opentelemetry::Context::current();
  let route = request
    .extensions()
    .get::<MatchedPath>()
    .unwrap()
    .as_str()
    .to_string();

  let mut span = tracer.start_with_context(route.clone(), &cx);
  span.set_attribute(Key::new("http.method").string(request.method().as_str().to_string()));
  span.set_attribute(Key::new("http.target").string(request.uri().path().to_string()));
  span.set_attribute(Key::new("http.route").string(route));

  let response = next.run(request).await;

  // Set http response
  span.set_attribute(Key::new("http.status_code").i64(i64::from(response.status().as_u16())));
  span.end();

  response
}
