//! Bounded initial truth and event-assisted election waiting.
use super::*;
use crate::resolve::{Boot, ResolutionChanged, lookup};

impl ProfileResolver {
    /// One total budget covers election waiting and every unresolved name.
    /// Existing publications are read without querying or acquiring ownership.
    /// A provider must complete this before adopting its serving generation.
    /// A changed plan also needs the caller's old-owner handoff: this operation
    /// never revokes a live tenure belonging to the previous configuration.
    pub async fn boot(
        &self,
        events: &ProcessEventBus,
        context: &RunContext,
        shutdown: &CancellationToken,
    ) -> Result<Vec<PublishedResolution>, ResolverError> {
        tokio::select! {
            biased;
            _ = context.cancelled() => Err(ResolverError::Cancelled),
            _ = shutdown.cancelled() => Err(ResolverError::Cancelled),
            result = tokio::time::timeout(self.budget, self.boot_inner(events)) => {
                result.map_err(|_| ResolverError::BootTimeout {
                    profile: self.profile.clone(),
                    names: self.bindings.iter().map(|binding| {
                        format!("{} ({})", binding.name.label, binding.name.source.name())
                    }).collect::<Vec<_>>().join(", ")
                })?
            }
        }
    }
    fn required(
        &self,
        binding: &Binding,
        error: claviron_resolver::ResolveError,
    ) -> ResolverError {
        ResolverError::Required {
            profile: self.profile.clone(),
            name: format!(
                "{} ({})",
                binding.name.label,
                binding.name.source.name()
            ),
            error,
        }
    }
    async fn boot_inner(
        &self,
        events: &ProcessEventBus,
    ) -> Result<Vec<PublishedResolution>, ResolverError> {
        // Subscribe before reading/electing. A contending boot must observe an
        // answer committed inside its budget, even when retry_interval is longer.
        let mut changed = events.subscribe::<ResolutionChanged>();
        loop {
            let current = self.current()?;
            for (binding, publication) in self.bindings.iter().zip(&current) {
                if let Some(publication) = publication
                    && self.config.boot == Boot::Require
                    && self
                        .restore(binding, publication)?
                        .active()
                        .next()
                        .is_none()
                {
                    return Err(self.required(
                        binding,
                        claviron_resolver::ResolveError::new(
                            claviron_resolver::ResolveErrorKind::NoRecords,
                            "authoritative answer has no active endpoints",
                        ),
                    ));
                }
            }
            if current.iter().all(Option::is_some) {
                return Ok(current
                    .into_iter()
                    .map(|current| current.expect("checked complete"))
                    .collect());
            }
            let Some(mut tenure) = self.acquire()? else {
                self.wait_boot_change(&mut changed).await;
                continue;
            };
            for (index, binding) in self.bindings.iter().enumerate() {
                // A preceding owner could commit between our read and acquire.
                if binding.entry.read()?.is_some() {
                    continue;
                }
                let mut set = ResolvedSet::new(
                    binding.name.first,
                    self.config.max_addresses,
                )?;
                let Some(answer) = self
                    .during(
                        &mut tenure,
                        lookup(
                            self.resolver.as_ref(),
                            &binding.name.source,
                            &self.config,
                            self.budget,
                        ),
                    )
                    .await?
                else {
                    break;
                };
                #[cfg(feature = "metrics")]
                self.record_query(index, &answer);
                let warning = match answer {
                    Ok(answer) => {
                        set.apply(answer)?;
                        None
                    }
                    Err(error) if self.config.boot == Boot::Require => {
                        return Err(self.required(binding, error));
                    }
                    Err(error) => {
                        let (_, warn) =
                            set.fail(error.kind, self.config.on_failure)?;
                        warn.then_some(error)
                    }
                };
                let publication = self.publish(&mut tenure, index, 0, &set)?;
                if let Some(publication) = publication {
                    self.announce(binding, publication, warning, events);
                } else {
                    break;
                }
            }
            // Drop exact ownership before retrying a partial/conflicting boot.
            drop(tenure);
            let current = self.current()?;
            if current.iter().any(Option::is_none) {
                self.wait_boot_change(&mut changed).await;
            }
        }
    }

    async fn wait_boot_change(
        &self,
        changed: &mut tokio::sync::broadcast::Receiver<ResolutionChanged>,
    ) {
        use tokio::sync::broadcast::error::RecvError;
        tokio::select! {
            _ = tokio::time::sleep(self.config.retry_interval) => {},
            _ = async {
                loop {
                    match changed.recv().await {
                        Ok(event) if event.profile == self.profile => return,
                        Ok(_) => {},
                        Err(RecvError::Lagged(_)) => return,
                        Err(RecvError::Closed) => std::future::pending::<()>().await,
                    }
                }
            } => {}
        }
    }
}
