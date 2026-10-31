use super::*;
use claviron_php_fcgi::{PhpFcgiService, PhpFpmAdapter};

struct Fcgi(PhpFpmAdapter);

pub(super) fn prepare(
    state: &AppState,
    runtime: Arc<PhpRuntime>,
) -> Option<Arc<dyn DispatchStep>> {
    let Some(service) = state.registry_ref().resolve::<PhpFcgiService>() else {
        tracing::error!("PhpFcgiService not registered");
        return None;
    };
    Some(Arc::new(Fcgi(PhpFpmAdapter::new(service, runtime))))
}

impl DispatchStep for Fcgi {
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
