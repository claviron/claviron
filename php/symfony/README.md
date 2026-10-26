# Claviron Symfony integration

Install `claviron/symfony` and copy `stubs/application.php` to the application
root. Point the Claviron application's `script_path` at that entry. The stub
creates the application's `App\Kernel` with its fixed environment and calls
`Claviron\Symfony\ApplicationRunner::run()`.

The runner accepts a `KernelInterface & TerminableInterface`, boots it once,
handles buffered requests, responds before Symfony termination, and shuts the
kernel down when native receive ends. Symfony's kernel owns its request/service
reset lifecycle. Claviron owns execution budgets and cancellation; the adapter
does not call `set_time_limit()` or introduce another server or polling loop.

Applications that need link presence can opt in by calling
`ApplicationRunner::run($kernel, links: true)` and registering the supplied
subscriber. After boot, the runner subscribes to the four Claviron
local connection/departure topics and dispatches typed Symfony events:
`EdgeConnected`, `ApplicationConnected`, `EdgeLeft`, and `ApplicationLeft` under
`Claviron\Symfony\Link\Events`. To log them, register the supplied subscriber
in the application's Symfony service configuration. For a PHP-configured
kernel, add this beside the other service registrations:

```php
$services->set(\Claviron\Symfony\Link\LinkSubscriber::class);
```

Application subscribers may listen to the same classes. Connection events report
only opposite-side lanes directly observed by this process, never other peers'
introductions. No fleet query/reply exchange occurs. The fleet field names the
Link segment, role names its transport and an unknown PID stays null. The left
events report the final lane for a name departing. They share Claviron's
topic names and fields with the Laravel link events. The native event loop is
already owned by `ApplicationRunner`; no extra worker or poller is needed.
The link bridge runs in the persistent application mode. Script/SAPI and FCGI
requests use the ordinary Symfony HTTP entry; they do not keep an event
receiver alive between requests and do not join an Orbit application link.
Their request handling remains available through the normal Symfony routes;
link presence is meaningful only for the application mode.

This adapter preserves the sample's raw-body request envelope. It does
not emulate PHP superglobals or parse multipart uploads. Streamed/file
responses and other non-HTTP messages fail explicitly. It adds
no demo counters or boot IDs to responses. Framework-neutral native fallbacks
come from the `claviron/sapi` dependency; Rust crate names are unchanged.
