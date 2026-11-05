use super::*;
use crate::{PhpApplicationAdapter, PhpApplicationService};

struct Application(PhpApplicationAdapter);

pub(super) fn prepare(
    state: &AppState,
    runtime: PhpRuntime,
) -> Arc<dyn BackendHandler> {
    let adapter = runtime.application.as_ref().and_then(|name| {
        let service = state.registry_ref().resolve::<PhpApplicationService>()?;
        let adapter = match PhpApplicationAdapter::new(service, name).environment(state, &runtime) {
            Ok(adapter) => adapter,
            Err(error) => {
                tracing::error!(application = %name, %error, "PHP application request environment could not be prepared");
                return None;
            }
        };
        Some(adapter)
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
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        let adapter = self.0.clone();
        DispatchCall::new(async move {
            match adapter.handle(request).await {
                Ok(response) => ServiceResult::Handled(response),
                Err(never) => match never {},
            }
        })
    }
}

impl BackendHandler for Application {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::Request,
            reply: BackendReply::Owner,
        }
    }
}
