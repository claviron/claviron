use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use claviron_metrics::MetricsProvider;
use claviron_runtime::AppState;

pub fn routes(_state: &AppState) -> Router<AppState> {
    Router::new().route("/metrics", get(metrics_endpoint))
}

async fn metrics_endpoint(State(state): State<AppState>) -> impl IntoResponse {
    let Some(provider) = state.registry_ref().resolve::<MetricsProvider>()
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(http::header::CONTENT_TYPE, "text/plain")],
            "MetricsProvider is not registered".to_string(),
        );
    };

    match provider.render_prometheus(&state) {
        Ok(output) => (
            StatusCode::OK,
            [(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            output,
        ),
        Err(err) => {
            tracing::error!("failed to render metrics: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(http::header::CONTENT_TYPE, "text/plain")],
                format!("Error rendering metrics: {err}"),
            )
        }
    }
}
