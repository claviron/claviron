use super::*;
use claviron_http::policy::HttpPolicyOverride;
use claviron_php_embed::{PhpApplicationAdapter, PhpApplicationService};

struct Application(PhpApplicationAdapter);

pub(super) fn prepare(state: &AppState, runtime: PhpRuntime) -> Arc<dyn DispatchStep> {
    let adapter = runtime.application.as_ref().and_then(|name| {
        let service = state.registry_ref().resolve::<PhpApplicationService>()?;
        let adapter = match PhpApplicationAdapter::new(service, name).environment(state, &runtime) {
            Ok(adapter) => adapter,
            Err(error) => {
                tracing::error!(application = %name, %error, "PHP application request environment could not be prepared");
                return None;
            }
        };
        Some(adapter.route_policy(HttpPolicyOverride {
            requests: runtime.requests, responses: runtime.responses,
        }))
    });
    match adapter {
        Some(adapter) => Arc::new(Application(adapter)),
        None => {
            tracing::error!(application = ?runtime.application, "PHP application route unavailable; matched requests will return 503");
            Arc::new(UnavailableApplication)
        }
    }
}

impl DispatchStep for Application {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let adapter = self.0.clone();
        DispatchFuture::new(async move {
            match adapter.handle(request).await {
                Ok(response) => ServiceResult::Handled(response),
                Err(never) => match never {},
            }
        })
    }
}
