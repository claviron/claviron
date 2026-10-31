use super::*;
use claviron_php_embed::{PhpScriptAdapter, PhpScriptService};

struct Script(PhpScriptAdapter);

pub(super) fn prepare(
    state: &AppState,
    runtime: Arc<PhpRuntime>,
) -> Option<Arc<dyn DispatchStep>> {
    let Some(service) = state.registry_ref().resolve::<PhpScriptService>()
    else {
        tracing::error!("PhpScriptService not registered");
        return None;
    };
    Some(Arc::new(Script(PhpScriptAdapter::new(service, runtime))))
}

impl DispatchStep for Script {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchFuture {
        let adapter = self.0.clone();
        DispatchFuture::new(async move {
            match adapter.try_handle(request).await {
                Ok(ServiceResult::Handled(response)) => {
                    ServiceResult::Handled(response)
                }
                Ok(ServiceResult::Unhandled(request)) => {
                    // The native resolver returned before polling the body.
                    ServiceResult::Unhandled(request)
                }
                Err(error) => ServiceResult::Handled(gateway_error(
                    error.status_code(),
                    error,
                )),
            }
        })
    }
}
