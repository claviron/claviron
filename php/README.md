# Claviron PHP packages

This directory contains the source for Claviron's Composer packages. Each
package has its own manifest, dependencies and stable release tags.

| Package | Purpose |
| --- | --- |
| [claviron/sapi](sapi/README.md) | Native API declarations, fallbacks and runtime logging |
| [claviron/symfony](symfony/README.md) | Persistent Symfony HttpKernel application adapter |

`scripts/composer.sh` exports committed package sources to their individual
distribution repositories. Package versions are independent of the Rust
release. Runtime-internal scripts embedded in native binaries remain in their
own Rust crates and are excluded from these Composer distributions.
