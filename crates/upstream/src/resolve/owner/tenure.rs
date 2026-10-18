//! Exact profile tenure retained and renewed across queries and sleeps.
use super::*;

impl ProfileResolver {
    /// All boot/query/retirement commits share the same exact lease and revision
    /// boundary. A failed commit never substitutes private local ownership.
    pub(super) fn publish(
        &self,
        tenure: &mut Tenure,
        index: usize,
        revision: u64,
        set: &ResolvedSet,
    ) -> Result<Option<PublishedResolution>, ResolverError> {
        let snapshot = set.snapshot()?;
        Ok(match &mut tenure.lease {
            Some(lease) => tenure.writers[index]
                .publish(revision, snapshot, lease, self.ttl)?,
            None => tenure.writers[index].publish_local(revision, snapshot)?,
        })
    }
    pub(super) fn acquire(&self) -> Result<Option<Tenure>, ResolverError> {
        let mut lease = match &self.authority {
            ResolutionAuthority::Local { .. } => None,
            ResolutionAuthority::Fleet { locks, owner, .. } => {
                match locks
                    .try_acquire(self.bindings[0].entry.duty(), owner, self.ttl)
                    .map_err(PublicationError::from)?
                {
                    LockAcquire::Occupied(_) => return Ok(None),
                    LockAcquire::Acquired(lease) => {
                        Some(Lock::new(locks.clone(), lease))
                    }
                }
            }
        };
        let mut writers = Vec::with_capacity(self.bindings.len());
        for binding in &self.bindings {
            let writer = match &mut lease {
                Some(lease) => {
                    let Some(writer) = binding.entry.claim(lease, self.ttl)?
                    else {
                        return Ok(None);
                    };
                    writer
                }
                None => binding.entry.claim_local()?,
            };
            writers.push(writer);
        }
        Ok(Some(Tenure { lease, writers }))
    }
    pub(super) async fn during<T>(
        &self,
        tenure: &mut Tenure,
        future: impl Future<Output = T>,
    ) -> Result<Option<T>, ResolverError> {
        let Some(lease) = &mut tenure.lease else {
            return Ok(Some(future.await));
        };
        if !lease.renew(self.ttl).map_err(PublicationError::from)? {
            return Ok(None);
        }
        // Half the derived lease lifetime is a renewal safety bound, not a
        // second operational refresh cadence. Delayed ticks never catch up.
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + self.budget,
            self.budget,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                _ = ticker.tick() => {
                    if !lease.renew(self.ttl).map_err(PublicationError::from)? { return Ok(None); }
                }
                output = &mut future => {
                    return Ok(lease.renew(self.ttl).map_err(PublicationError::from)?.then_some(output));
                }
            }
        }
    }
}
