//! Configured cold refresh loop; no request-path queries or spawned tasks.
use super::*;
use crate::resolve::lookup;

impl ProfileResolver {
    pub(super) fn announce(
        &self,
        binding: &Binding,
        publication: PublishedResolution,
        warning: Option<claviron_resolver::ResolveError>,
        events: &ProcessEventBus,
    ) {
        let active = publication
            .snapshot
            .restore()
            .map(|set| set.active().count())
            .unwrap_or_default();
        tracing::debug!(
            profile = %self.profile,
            name = %binding.name.label,
            revision = publication.revision,
            generation = ?publication.snapshot.generation(),
            active,
            "upstream resolution published"
        );
        events.emit(binding.entry.event(&publication));
        if let Some(error) = warning {
            tracing::warn!(profile = %self.profile, name = %binding.name.label,
                %error, "upstream resolution is stale; retaining failure policy");
        }
    }

    /// Cancellation drops the pending query and exact lease in this future.
    /// It does not claim to join the resolver driver's shared protocol work.
    pub async fn run(
        &self,
        events: &ProcessEventBus,
        context: &RunContext,
        shutdown: &CancellationToken,
    ) -> Result<(), ResolverError> {
        tokio::select! {
            biased;
            _ = context.cancelled() => Ok(()),
            _ = shutdown.cancelled() => Ok(()),
            result = self.run_inner(events) => result,
        }
    }
    fn next(
        &self,
        set: &ResolvedSet,
    ) -> tokio::time::Instant {
        let delay = if set.stale() {
            self.config.retry_interval
        } else if !self.config.interval.is_zero() {
            self.config.interval
        } else {
            set.valid_until().map_or(self.config.retry_interval, |expiry| {
                let remaining =
                    expiry.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.config.retry_interval
                } else {
                    remaining
                }
            })
        };
        tokio::time::Instant::now() + delay
    }
    async fn run_inner(
        &self,
        events: &ProcessEventBus,
    ) -> Result<(), ResolverError> {
        loop {
            let Some(mut tenure) = self.acquire()? else {
                tokio::time::sleep(self.config.retry_interval).await;
                continue;
            };
            let mut due = self
                .current()?
                .iter()
                .map(|current| match current {
                    Some(current) => {
                        let set = current.snapshot.restore()?;
                        if set.stale()
                            || set
                                .valid_until()
                                .is_some_and(|expiry| expiry > Instant::now())
                        {
                            return Ok(self.next(&set));
                        }
                        Ok(tokio::time::Instant::now())
                    }
                    _ => Ok(tokio::time::Instant::now()),
                })
                .collect::<Result<Vec<_>, PublicationError>>()?;
            'owned: loop {
                let mut retiring = false;
                for (index, binding) in self.bindings.iter().enumerate() {
                    // Re-read before a query: joined retirement publications may
                    // have advanced the revision while this owner was sleeping.
                    let current = binding.entry.read()?;
                    let mut revision =
                        current.as_ref().map_or(0, |current| current.revision);
                    let mut set = match &current {
                        Some(current) => self.restore(binding, current)?,
                        None => ResolvedSet::new(
                            binding.name.first,
                            self.config.max_addresses,
                        )?,
                    };
                    let generation = set.generation();
                    // Snapshot receipts are not evidence. Only native IO join
                    // or exact kernel death can release a physically retired
                    // assignment; publication revalidates that proof under flock.
                    for receipt in set.retiring().collect::<Vec<_>>() {
                        if binding.entry.retirement_ready(receipt)? {
                            set.finish_retirement(receipt)?;
                        }
                    }
                    if set.generation() != generation {
                        let publication =
                            self.publish(&mut tenure, index, revision, &set)?;
                        let Some(publication) = publication else {
                            if tenure.lease.is_none() {
                                return Ok(());
                            }
                            break 'owned;
                        };
                        revision = publication.revision;
                        self.announce(binding, publication, None, events);
                        // Joining IO does not restart the DNS query cadence or
                        // manufacture a fresh TTL, and emits no health verdict.
                    }
                    if due[index] > tokio::time::Instant::now() {
                        retiring |= set.retiring().next().is_some();
                        continue;
                    }
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
                        break 'owned;
                    };
                    #[cfg(feature = "metrics")]
                    self.record_query(index, &answer);
                    let warning = match answer {
                        Ok(answer) => {
                            set.apply(answer)?;
                            None
                        }
                        Err(error) => {
                            let (_, warn) =
                                set.fail(error.kind, self.config.on_failure)?;
                            warn.then_some(error)
                        }
                    };
                    let published =
                        self.publish(&mut tenure, index, revision, &set)?;
                    if let Some(published) = published {
                        self.announce(binding, published, warning, events);
                        due[index] = self.next(&set);
                        retiring |= set.retiring().next().is_some();
                    } else {
                        // A concurrent retirement transaction may have won the
                        // revision; retry from its current truth, never overwrite.
                        // Local supersession must exit rather than reclaim a
                        // newer generation's private ownership.
                        if tenure.lease.is_none() {
                            return Ok(());
                        }
                        break 'owned;
                    }
                }
                let mut next =
                    *due.iter().min().expect("prepared named bindings");
                if retiring {
                    // Existing retry_interval also bounds cold receipt checks.
                    // This is not a drain deadline or another DNS refresh timer;
                    // blocked owners keep their cells for as long as necessary.
                    next = next.min(
                        tokio::time::Instant::now()
                            + self.config.retry_interval,
                    );
                }
                if self
                    .during(&mut tenure, tokio::time::sleep_until(next))
                    .await?
                    .is_none()
                {
                    break 'owned;
                }
            }
            drop(tenure);
            tokio::time::sleep(self.config.retry_interval).await;
        }
    }
}
