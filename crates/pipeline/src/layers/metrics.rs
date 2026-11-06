use claviron_runtime::activity::{ACTIVITY, Exchange};

use super::*;

#[derive(Default)]
pub(crate) struct MetricsState<S> {
    inner: S,
    exchange: Option<Exchange>,
}

impl<S> MetricsState<S> {
    pub(crate) fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

/// Count an exchange until the response head is ready or the request is dropped.
pub(crate) struct MetricsLayer;

impl<S> HostLayer<MetricsState<S>> for MetricsLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<MetricsState<S>>,
    ) -> crate::Result<()> {
        layer.before(|_, state| {
            state.exchange = Some(ACTIVITY.begin());
            ControlFlow::Continue(())
        });
        Ok(())
    }
}
