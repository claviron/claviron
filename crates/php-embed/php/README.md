# Claviron SAPI PHP package

`claviron/sapi` provides the PHP-side companion to Claviron's embedded SAPI:
native API declarations and fail-fast fallbacks, the `Claviron\` PHP types,
and `Claviron\Monolog\ClavironHandler` for runtime log channels.

```sh
composer require claviron/sapi
```

Composer installs PHP files only. It does not install or compile the native
Claviron host. Native definitions win when the matching host is present;
fallbacks do not emulate runtime services. Use `claviron_runtime_available()`
for engine presence and `claviron_runtime_configured()` for prepared host
bindings, not `function_exists()` after the fallbacks have been loaded.

The framework-independent package does not depend on Laravel. Laravel drivers
and the persistent application adapter are supplied by `claviron/laravel`.

Source changes belong in the [Claviron repository](https://github.com/claviron/claviron/tree/main/crates/php-embed/php).
The standalone Composer repository is a generated distribution, not a second
development source. Packagist versions follow its release tags.

## License

MIT.
