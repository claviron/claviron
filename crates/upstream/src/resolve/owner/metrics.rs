//! Prepared owner bindings hold query counters; readers do not count.
use claviron_orbit::counter::Counter;

use super::*;
use crate::resolve::AddressAnswer;
use crate::resolve::metrics::{QueryCounts, Store};

impl ProfileResolver {
    pub(crate) fn attach_metrics(
        &mut self,
        store: &Store,
        table: Option<Arc<Counter>>,
    ) -> Result<(), PublicationError> {
        for binding in &mut self.bindings {
            binding.counters = Some(store.prepare(
                &self.profile,
                &binding.name.label,
                table.clone(),
            )?);
        }
        Ok(())
    }
    pub(super) fn record_query(
        &self,
        index: usize,
        answer: &Result<AddressAnswer, claviron_resolver::ResolveError>,
    ) {
        if let Some(counts) = &self.bindings[index].counters {
            counts.record(answer);
        }
    }
    pub(crate) fn query_counts(
        &self,
        label: &str,
    ) -> Result<QueryCounts, PublicationError> {
        self.bindings
            .iter()
            .find(|b| b.name.label == label)
            .and_then(|b| b.counters.as_ref())
            .map_or(Ok(QueryCounts::default()), |counts| counts.snapshot())
    }
}
