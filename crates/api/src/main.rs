mod handlers;
mod metrics_handler;
mod sql_handlers;
mod state;

use std::net::SocketAddr;

use axum::{
    routing::{get, post},
    Router,
};
use metrics_handler::metrics_handler;
use sql_handlers::{sql_execute, sql_plan};
use state::AppState;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let checkpoint_dir = std::env::var("CHECKPOINT_DIR").unwrap_or_else(|_| "./checkpoints".into());
    let state = AppState::new(checkpoint_dir.into());

    let app = Router::new()
        .route("/pipelines", post(handlers::create_pipeline).get(handlers::list_pipelines))
        .route("/pipelines/:name", get(handlers::get_pipeline).delete(handlers::delete_pipeline))
        .route("/pipelines/:name/start", post(handlers::start_pipeline))
        .route("/pipelines/:name/stop", post(handlers::stop_pipeline))
        .route("/health", get(handlers::health))
        .route("/metrics", get(metrics_handler))
        .route("/sql/plan", post(sql_plan))
        .route("/sql/execute", post(sql_execute))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    info!("IVM control plane running on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
