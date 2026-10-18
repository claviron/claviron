# Claviron Laravel integration

This package replaces Laravel's server-side Pusher HTTP call with Claviron's
native broadcast primitive. Browser clients continue to use Laravel Echo and
the Pusher WebSocket protocol.

```php
// config/broadcasting.php
'default' => env('BROADCAST_CONNECTION', 'claviron'),

'connections' => [
    'claviron' => [
        'driver' => 'claviron',
        'key' => env('PUSHER_APP_KEY'),
        'secret' => env('PUSHER_APP_SECRET'),
    ],
],
```

The normal `/broadcasting/auth` route and `routes/channels.php` declarations
remain authoritative for private and presence channels. No Pusher PHP SDK or
HTTP publisher is used by this connection.

Fleet-global channel occupancy transitions are ordinary typed Laravel events:

```php
use Claviron\Laravel\Broadcasting\Events\ChannelOccupied;
use Claviron\Laravel\Broadcasting\Events\ChannelVacated;
use Illuminate\Support\Facades\Event;

Event::listen(ChannelOccupied::class, function (ChannelOccupied $event): void {
    // $event->appId, $event->channel, $event->time (CarbonImmutable)
    // Presence channels also carry $event->userId and $event->userInfo.
});

Event::listen(ChannelVacated::class, function (ChannelVacated $event): void {
    // The last fleet subscriber has left this app's channel.
});
```

The package subscribes the persistent application loop to both bus topics.
`appId` keeps applications separate even when they use the same channel name.

## Lumenize console

The Rust `claviron-laravel` package also builds a `lumenize` binary. Its default
behavior is to execute the application's own Artisan entry:

```sh
cargo build -p claviron-laravel --bin lumenize
```

Install it on your Cargo binary path from the library checkout:

```sh
cargo install --path crates/laravel --bin lumenize --locked
```

Then run `lumenize list` from the application root. The executable name is
`lumenize`; its Rust package is `claviron-laravel`.

Run that executable from your Laravel application root, for example
`/path/to/lumenize list`. It executes `./artisan` in the native PHP engine;
your autoloader, bootstrap, profiles and custom preparation remain owned by
that local entry. No package runner replaces it, no HTTP server or TaskService
starts, and the entry is not modified. Command arguments, terminal input/output
and the command's exit status are preserved. If `./artisan` is missing, the
launcher reports that no Laravel application was found here and exits with
failure. It does not require or automatically install a Composer package.

This entry does not yet attach to a live fleet or prepare native DB/cache
services. Use ordinary PHP drivers for detached commands; explicit Claviron
drivers need a host with those capabilities prepared. `--live` is not implemented.

The isolated external console check takes a Composer autoloader and the built
binary: `php tests/artisan.php /path/to/vendor/autoload.php /path/to/lumenize`.

### Console configuration

The auto-discovered `Claviron\Laravel\Providers\LumenizeProvider` supplies
defaults only when Laravel is running in console mode. Applications disabling
package discovery register that class in their own provider list.

Without an application config file, `config('lumenize.config_path')` returns
`lumenize.yaml`. Optionally publish the defaults:

```sh
lumenize vendor:publish --tag=lumenize-config
```

Override them in your application's `config/lumenize.php`:

```php
return [
    'config_path' => 'config/example.yaml',
];
```

The path is relative to the application's working directory. Application values
and cached Laravel configuration remain authoritative. The provider declares
configuration only; it does not yet read that YAML, prepare native services or
attach to Orbit. Ordinary `php artisan` can also publish this configuration.

## Application events

Think in directions relative to this PHP application:

| Direction | Application code | Claviron's role |
| --- | --- | --- |
| Local | Dispatch and listen with Laravel's ordinary event API | Nothing crosses the application boundary unless this class is explicitly relayed or is a `FleetEvent`. |
| Incoming | Register a `FleetEvent` class with `EventBridge::subscribe()`, then use an ordinary Laravel listener | Claviron receives `topic` and `payload`, decodes the registered class, and dispatches it locally. Subscribe before the work whose event matters; there is no replay. |
| Outgoing | Dispatch a `FleetEvent` or call `EventBridge::publish()` | Claviron publishes its stable topic and payload to the application event bus. |

`Claviron\Runtime\Event` is the incoming native envelope. Laravel code normally
handles the decoded event class, not the envelope or the Orbit link. A link
presence subscriber is optional application behavior; Edge already logs
transport reachability without any PHP subscription.

An ordinary Laravel event can also travel from one PHP worker to another. In
that case, register its class with `relay()` before Laravel's listener chain
is registered:

```php
use Claviron\Laravel\EventBridge;
use App\Events\DemoEvent;

app(EventBridge::class)->relay([DemoEvent::class]);
```

Dispatch remains ordinary Laravel code. The bridge converts the event into the
existing Claviron `ApplicationEvent`: the runtime (`php`), named application,
Laravel class name, envelope ID, and serialized object travel in one fleet
frame. Rust rejects another runtime or application before PHP sees the frame.
One application worker claims the envelope and runs Laravel's ordinary
listener chain; a `ShouldQueue` listener therefore enqueues one job for the
application's queue pool. The envelope ID remains transport context and is not
injected into the Laravel event.

Events shared with Rust, JavaScript, or Python use a stable topic and the
`FleetEvent` contract. `relay()` is for serialized PHP classes and does not
make that serialization a cross-language contract. Incoming delivery never
publishes the same event back to the bus.

## Sessions

The `claviron` session driver stores Laravel's serialized session payload in a
Claviron cache store. Give sessions their own named cache store so application
cache flushes cannot remove them:

```php
// config/cache.php
'claviron_sessions' => [
    'driver' => 'claviron',
    'runtime_store' => 'sessions',
    'prefix' => 'my-app-session-',
],

// config/session.php
'driver' => env('SESSION_DRIVER', 'claviron'),
'store' => env('SESSION_STORE', 'claviron_sessions'),
```

The runtime store may compose Orbit L1 with Redis L2. Laravel's session
blocking uses the same cache repository's Orbit lock implementation when
`session.block_store` selects this store.

An explicit `session.store` is preserved. If it is absent, null or empty,
the provider uses Laravel's configured `cache.default`; the driver name
`claviron` does not require a cache store named `claviron`.

### Bind data to an existing native session

For a route already protected by the host's native auth policy, opt in:

```php
Route::middleware('native.session:api')->group(function () {
    Route::post('/example', function (Illuminate\Http\Request $request) {
        $request->session()->flash('notice', 'Saved');

        return response()->noContent();
    });
});
```

Register `SessionServiceProvider` and select `session.driver=claviron`.
The alias is optional route middleware, not a global API/session boot hook.
The required argument (`api` above) is a data namespace, **not a guard selector**;
the host's vhost/route guard must already decide who this request belongs to.
Different namespaces remain separate even for the same native session ID.

`StartNativeSession` uses the verified native session ID and remaining credential
lifetime, honors `session.store` or the configured cache default, and emits no
Laravel session cookie. Missing/expired native sessions are rejected. Applications
can subclass `allowsGuest()` for transient login forms; guest data is never saved.
Subclasses can retain their own native CSRF/audit integration without reimplementing
the identity-to-cache binding. This does not introduce another login or auth session.

Flash retains ordinary Laravel aging and the configured Laravel serialization.
The default PHP-serialized payload is not directly shared with Rust. Laravel's
optional `session.serialization=json` can provide a readable representation,
but coordinating Rust writes/flash consumption is a separate application concern.
