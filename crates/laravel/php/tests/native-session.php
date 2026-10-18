<?php

declare(strict_types=1);

// Real Laravel session/cache managers; no application boot, runtime or network.
require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

use Claviron\Laravel\Http\Middleware\StartNativeSession;
use Claviron\Laravel\Providers\SessionServiceProvider;
use Illuminate\Auth\AuthenticationException;
use Illuminate\Cache\CacheManager;
use Illuminate\Config\Repository;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Application;
use Illuminate\Http\Request;
use Illuminate\Routing\Router;
use Illuminate\Session\SessionManager;
use Symfony\Component\HttpFoundation\Response;

$native = null;
$reads = 0;
function claviron_auth(): ?array
{
    ++$GLOBALS['reads'];

    return $GLOBALS['native'];
}
function verifyNativeSession(bool $condition, string $message): void
{
    if (!$condition) { throw new LogicException($message); }
}

$app = new Application(sys_get_temp_dir());
$app->instance('config', new Repository([
    'cache' => ['default' => 'shared', 'stores' => [
        'shared' => ['driver' => 'array'],
        'selected' => ['driver' => 'array'],
    ]],
    'session' => [
        'driver' => 'claviron', 'store' => null, 'cookie' => 'example_session',
        'lifetime' => 10, 'lottery' => [0, 100], 'encrypt' => false,
    ],
]));
$cache = new CacheManager($app);
$manager = new SessionManager($app);
$router = new Router(new Dispatcher($app), $app);
$app->instance('cache', $cache);
$app->instance('session', $manager);
$app->instance('router', $router);
new SessionServiceProvider($app)->boot($cache);
verifyNativeSession($router->getMiddleware()['native.session'] === StartNativeSession::class, 'Opt-in alias missing.');
$middleware = new StartNativeSession($manager);
$request = static fn () => Request::create('https://app.example.test/data', 'POST', cookies: [
    'example_session' => str_repeat('a', 40),
]);
$native = ['session_id' => '00000000-0000-4000-8000-000000000001', 'expires_at' => time() + 120];
$nativeId = $native['session_id'];
$key = sha1('api:php:'.$nativeId);
$response = $middleware->handle($request(), static function (Request $request) use ($key): Response {
    verifyNativeSession($request->hasSession(), 'Native session not attached to the request.');
    verifyNativeSession($request->session()->getId() === $key, 'Client cookie replaced the verified native ID.');
    $request->session()->flash('notice', 'Saved');
    $request->session()->put('value', 'api-data');

    return new Response('ok');
}, 'api');
verifyNativeSession($reads === 1, 'Native identity was resolved more than once.');
verifyNativeSession($response->headers->getCookies() === [], 'A second session cookie was emitted.');
verifyNativeSession(is_string($cache->store()->get($key)), 'Laravel data was not persisted in the configured cache.');

$middleware->handle($request(), static function (Request $request): Response {
    verifyNativeSession($request->session()->get('notice') === 'Saved', 'Flash did not survive the next request.');

    return new Response('ok');
}, 'api');
$middleware->handle($request(), static function (Request $request): Response {
    verifyNativeSession($request->session()->get('notice') === null, 'Flash was not aged by Laravel.');

    return new Response('ok');
}, 'api');

$middleware->handle($request(), static function (Request $request): Response {
    verifyNativeSession($request->session()->get('value') === null, 'Data crossed native session namespaces.');
    $request->session()->put('value', 'ops-data');

    return new Response('ok');
}, 'ops');
verifyNativeSession(unserialize($cache->store()->get($key), ['allowed_classes' => false])['value'] === 'api-data', 'Another namespace overwrote API data.');

$app['config']->set('session.store', 'selected');
$middleware->handle($request(), static function (Request $request): Response {
    verifyNativeSession($request->session()->get('value') === null, 'Explicit cache store was ignored.');
    $request->session()->put('value', 'selected-data');
    $request->session()->migrate();

    return new Response('ok');
}, 'api');
verifyNativeSession(unserialize($cache->store('selected')->get($key), ['allowed_classes' => false])['value'] === 'selected-data', 'Session regeneration detached data from the native ID.');

foreach ([null, ['session_id' => $nativeId, 'expires_at' => time() - 1], ['session_id' => 'invalid', 'expires_at' => time() + 120]] as $identity) {
    $native = $identity;
    try {
        $middleware->handle($request(), static fn () => new Response('unexpected'), 'api');
        throw new LogicException('Anonymous, expired or invalid native session was accepted.');
    } catch (AuthenticationException) {
    }
}
$native = null;
$guest = new class($manager) extends StartNativeSession {
    protected function allowsGuest(Request $request): bool { return true; }
};
$before = $cache->store('selected')->getStore()->all();
$guest->handle($request(), static function (Request $request): Response {
    $request->session()->put('guest', 'not-persistent');

    return new Response('ok');
}, 'login');
verifyNativeSession($cache->store('selected')->getStore()->all() === $before, 'Guest data was persisted.');

$native = ['session_id' => '00000000-0000-4000-8000-000000000002', 'expires_at' => time() + 120];
$middleware->handle($request(), static function (Request $request): Response {
    verifyNativeSession($request->session()->get('value') === null, 'Reusable middleware leaked another session.');

    return new Response('ok');
}, 'api');

echo "Native session binding, flash, namespaces, explicit cache and transient guests verified.\n";
