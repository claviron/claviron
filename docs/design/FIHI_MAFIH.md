# Fihi Ma Fihi

*In it is what is in it.*

Design identifier: `FIHI_MAFIH`.

Claviron is a composable runtime architecture. An application gives it a role
by selecting libraries, capabilities and service owners. The same foundation
can host an Edge, an application runner, a client tool, a task service or a VPN
service. It can also bring several of those roles into one managed process.

## Composition defines the role

The repository is a set of libraries, exposed through the public Claviron
facade. Crates own implementation boundaries; Cargo features select available
capabilities. Registration assembles the application's components, and
preparation turns its declarations into executable plans.

These choices operate at different lifetimes. Feature selection defines what a
binary can contain. Boot and publication define what that binary will run and
where its capabilities apply. Their cost and ownership follow
[LIFETIME_POLICY](LIFETIME_POLICY.md).

| Composition | Role |
| --- | --- |
| Runtime and application-owned components | An embedded runtime or a small service. |
| Managed lifecycle and outbound clients | A client tool or load generator, with no inbound HTTP listener required. |
| Application owner and selected ingress | A Rust or language application runner. |
| Listeners, host selection and prepared policies/backends | An HTTP Edge. |
| Application services and optional worker supervision | A task host, VPN service or combined application server. |

These roles can use the same lifecycle, transports and application contracts.
An Edge and a Runner adapt the same listener machinery to their own ownership
boundaries. A managed client can use the same outbound backend that an Edge
uses to forward a request. Its position in the interaction changes; the
underlying contract remains recognizable.

## Deployment follows explicit boundaries

A composition can keep services together or place them in separate processes.
This supports a modular monolith, independently managed services, and mixtures
of both. Each process can itself be a small Claviron composition with its own
lifecycle and selected capabilities.

A process boundary requires an explicit communication and ownership contract:
what crosses it, who admits work, who owns its body or session, and how
cancellation and shutdown propagate. Orbit Link provides same-host application
communication; HTTP upstreams provide another boundary. Moving an in-process
component out of process requires the appropriate adapter and deployment work.
These boundaries preserve the application's meaning while changing how its
parts are hosted.

## Design obligations

- **Compose existing owners.** A new role should reuse the relevant lifecycle,
  transport and execution owners. Add an adapter at the boundary that changes;
  avoid a second implementation of the same listener, supervisor or backend.
- **Keep dependencies directional.** Transport owns connection execution;
  Edge and Runner supply their adapters. Host policy belongs to the Edge that
  prepares it. A reusable primitive must not acquire the whole serving
  composition merely because one consumer uses it there.
- **Pay where the capability is selected.** Optional dependencies belong behind
  their feature boundary. Within a compiled capability, omitted host policies
  leave no action in that host's prepared pipeline. Shared contracts do not
  require every composition to construct every service.
- **Preserve semantics across forms.** Reusing a component carries its admission,
  body/session, cancellation and graceful ownership contracts with it. Shared
  interfaces do not make live resources or process ownership interchangeable.
- **Make the assembled form explicit.** Features, registrations and prepared
  configuration must explain which role runs, which resources it owns and which
  work it exposes. Each composition is independently accountable for its costs.

## Example: a managed benchmark client

The transport harness assembles one Claviron composition as a client and another
as a Runner application. Both use the normal supervisor lifecycle. The client
invokes a prepared Link backend or upstream service directly, without an Edge
frontend; the Runner receives through the corresponding production ingress.
This makes a benchmark tool from the same libraries that make a serving system.
The observation point changes without replacing the measured implementation.

The detailed contracts remain with their owners:
[HOST_PIPELINE](HOST_PIPELINE.md), [EDGE_BACKENDS](EDGE_BACKENDS.md),
[ORBIT_LINK](ORBIT_LINK.md) and [GRACEFUL_OWNERSHIP](GRACEFUL_OWNERSHIP.md).

Title reference: [Fihi Ma Fihi](https://en.wikipedia.org/wiki/Fihi_Ma_Fihi).
The architectural interpretation above is Claviron's own.
