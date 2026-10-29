# PHP installation

These scripts own the shared ZTS PHP toolchain and FPM service helpers.
The PHP builder supports Linux and FreeBSD. The macOS helper manages an
existing ZTS PHP installation through launchd; it does not build PHP.

Run commands from the Claviron repository. The PHP builder, module installer
and FPM installers print help without arguments. Building, installing extensions
and installing service files are separate operations. Choose a supported PHP
release explicitly when upgrading.

## Build profiles

| Profile | Compiler flags | PHP debug / assertions | Default prefix | Default source |
| --- | --- | --- | --- | --- |
| `release` (default) | `-O2 -g0` | Disabled | `/opt/php-zts-embed` | `$HOME/src/php-src` |
| `debug` | `-O0 -g` | Enabled | `/opt/php-zts-embed-debug` | `$HOME/src/php-src-debug` |

```sh
bash scripts/install/php/php-zts-install.sh dry-run
PHP_VERSION=8.5.12 bash scripts/install/php/php-zts-install.sh install

PHP_BUILD_PROFILE=debug PHP_VERSION=8.5.12 \
  bash scripts/install/php/php-zts-install.sh install
```

`CC` and `CXX` select the compiler. The profile flags are appended to inherited
`CFLAGS` and `CXXFLAGS`, so include-only flags cannot remove optimization.
Use `CPPFLAGS` for dependency include directories and `LDFLAGS` for linker
options. `PHP_INSTALL_DIR`, `PHP_SRC_DIR`, `MAKE_JOBS` and `SKIP_APT=1` select
installation paths, build parallelism and preinstalled dependencies.

The builder records compiler identity, flags, profile and configure options.
Changes discard old object files before reconfiguration; an unchanged retry
after a failed compilation reuses completed objects. `install --configure`
forces a clean reconfiguration. The installed `PHP_DEBUG` and ZTS settings are
verified. An existing installation with a different debug profile is rejected.

Debug builds use separate source and installation directories and do not
register their library with the system dynamic linker. Select their matching
`PHP`, `PHP_CONFIG` and library search path explicitly in a development
application; do not mix release and debug extension binaries.

## Extensions and Xdebug

PHP's debug build adds engine diagnostics. Xdebug is a separately compiled
extension for PHP debugging, profiling and related tools; using Xdebug does
not require a PHP debug build. Neither default installer command installs it.
See the [Xdebug installation documentation](https://xdebug.org/docs/install).

```sh
bash scripts/install/php/php-zts-install-modules.sh install
bash scripts/install/php/php-zts-install-modules.sh install-xdebug

PHP_BUILD_PROFILE=debug \
  bash scripts/install/php/php-zts-install-modules.sh install-xdebug
```

The module installer uses the matching profile and prefix, checks the PHP ABI,
and keeps Xdebug in the CLI scan directory. Embedded requests do not load it.
PECL builds use an invocation-owned temporary directory. Module installation
does not remove other builds' temporary files.

## FPM service helpers

Prepare the selected installation's `php.ini`, `php-fpm.conf`, pools and FPM
INI scan directory before installing a service helper. Preserve existing
socket paths and pool identities when replacing a service.

Linux with systemd:

```sh
sudo env PHP_VERSION=8.5.12 \
  bash scripts/install/php/php-zts-fpm-install.sh install
```

This validates the selected runtime and configuration, then installs service
files and the CLI launcher. It does not enable, start, stop or restart FPM.

FreeBSD:

```sh
sudo env PHP_VERSION=8.5.12 \
  sh scripts/install/php/php-zts-fpm-install-freebsd.sh install
```

This reads `php_zts_fpm` settings from the native rc configuration, validates
the runtime, and preserves an existing rc.d service file. It does not change
rc settings or restart FPM. The shared PHP builder selects `gmake` and the
FreeBSD compiler and dependency paths. The generated service uses graceful
`SIGQUIT` for stopping. Its log directory ownership is configured through
`php_zts_fpm_log_owner` and `php_zts_fpm_log_group` (defaults: `root`, `wheel`).

macOS, for an existing operator-selected PHP prefix:

```sh
PHP_INSTALL_DIR=/path/to/php-prefix \
  bash scripts/install/php/php-zts-ctl.sh install
```

This is an explicit service action: it renders the LaunchAgent paths and
starts FPM. Use `--help` to inspect its management commands first.

`re2c-install-almalinux.sh` installs the lexer generator when the distribution
version is insufficient. `composer-install.sh install` verifies Composer's
official installer signature and installs its PHAR without rebuilding PHP.
