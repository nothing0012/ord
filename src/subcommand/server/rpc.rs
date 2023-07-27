use super::*;
use axum_jrpc::{
  error::{JsonRpcError, JsonRpcErrorReason},
  JrpcResult, JsonRpcExtractor, JsonRpcResponse,
};
use serde_json::Value;

pub(super) async fn handler(
  Extension(_page_config): Extension<Arc<PageConfig>>,
  Extension(index): Extension<Arc<Index>>,
  value: JsonRpcExtractor,
) -> JrpcResult {
  match value.method.as_str() {
    "getHealth" => get_health(value).await,
    "getSatRanges" => get_sat_ranges(value, index).await,
    method => Ok(value.method_not_found(method)),
  }
}

fn invalid_params(answer_id: i64, message: String) -> JrpcResult {
  Err(JsonRpcResponse::error(
    answer_id,
    JsonRpcError::new(JsonRpcErrorReason::InvalidParams, message, Value::default()),
  ))
}

async fn get_health(value: JsonRpcExtractor) -> JrpcResult {
  let answer_id = value.get_answer_id();
  Ok(JsonRpcResponse::success(answer_id, "OK"))
}

async fn get_sat_ranges(value: JsonRpcExtractor, index: Arc<Index>) -> JrpcResult {
  #[derive(Deserialize)]
  struct Req {
    outputs: Vec<String>,
  }

  #[derive(Serialize)]
  struct SatRange {
    output: String,
    start: u64,
    end: u64,
  }

  #[derive(Serialize)]
  struct Res {
    sat_ranges: Vec<SatRange>,
  }

  let answer_id = value.get_answer_id();
  if index.has_sat_index().is_err() {
    return Ok(JsonRpcResponse::success(
      answer_id,
      Res { sat_ranges: vec![] },
    ));
  }

  let req: Req = value.parse_params()?;
  let mut res = Res { sat_ranges: vec![] };

  for output in req.outputs {
    let outpoint = match OutPoint::from_str(output.as_str()) {
      Ok(outpoint) => outpoint,
      Err(err) => return invalid_params(answer_id, err.to_string()),
    };
    let list = match index.list(outpoint) {
      Ok(list) => list,
      Err(err) => return invalid_params(answer_id, err.to_string()),
    };
    if let Some(list) = list {
      match list {
        List::Spent => {}
        List::Unspent(ranges) => {
          for range in ranges {
            res.sat_ranges.push(SatRange {
              output: output.clone(),
              start: range.0,
              end: range.1,
            });
          }
        }
      }
    }
  }

  Ok(JsonRpcResponse::success(answer_id, res))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_invalid_params() {
    let result = invalid_params(123, "Invalid input".to_string());
    assert!(result.is_err());
    let error = result.err().unwrap();
    assert_eq!(error.id, 123);
  }

  #[tokio::test]
  async fn test_get_health() {
    let value = JsonRpcExtractor {
      method: "getHealth".to_string(),
      parsed: Value::default(),
      id: 0,
    };
    let result = get_health(value).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.id, 0);
  }
}