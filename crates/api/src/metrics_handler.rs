use axum::{http::StatusCode, response::IntoResponse};
use prometheus::{Encoder, TextEncoder};

pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        buffer,
    )
}
