# Design notes

Claviron's design choices, ownership boundaries and guarantees. These are living
contracts, not protocol standards or chronological decision records.

## Application and transport

- [BYPASS_PIPELINE](BYPASS_PIPELINE.md): shape routing around the application;
  compose native Rust with existing request owners without bypassing admission.
- [HOST_BINDING](HOST_BINDING.md): a TLS connection belongs to its accepted
  host, not every application covered by its certificate.
- [EARLY_DATA_POLICY](EARLY_DATA_POLICY.md): transport acceptance of 0-RTT
  does not grant application permission to execute replayable work.
- [CONNECTION_HIJACK](CONNECTION_HIJACK.md): PHP and native Rust borrow the same
  PostgreSQL session instead of splitting one unit of work across connections.

## Lifetime and ownership

- [LIFETIME_POLICY](LIFETIME_POLICY.md): prepare work at compilation, boot or
  publication when possible; keep request-time safety checks with their evidence.
- [GRACEFUL_OWNERSHIP](GRACEFUL_OWNERSHIP.md): accepted work owns its graceful
  lifetime; an absent deadline grants no implicit termination authority.
- [HOT_CLIENT_POOL](HOT_CLIENT_POOL.md): retain usable protocol clients and
  maintain explicit warm capacity rather than repeating setup per operation.
- [WORKER_ADOPTION](WORKER_ADOPTION.md): preserve surviving workers through an
  explicit identity-checked ownership transfer.
- [INCARNATION_MASKING](INCARNATION_MASKING.md): keep logical roles stable while
  the runtime distinguishes and replaces their exact process owners.

## Same-OS cooperation

- [FLEETAWARE](FLEETAWARE.md): sibling processes cooperating on one OS, not a
  cloud cluster or automatic distributed singleton.
- [ORBIT_CELL](ORBIT_CELL.md): declare typed shared values; bind them before
  application work and release them through explicit ownership.
- [ORBIT_LINK](ORBIT_LINK.md): named application traffic over bounded inboxes
  and asynchronous SHM streams, with process-local resource ownership.

## Environment

- [PRODUCTION_ENVIRONMENT](PRODUCTION_ENVIRONMENT.md): explicit environment
  selection and configuration ownership, separate from process-env hygiene.
