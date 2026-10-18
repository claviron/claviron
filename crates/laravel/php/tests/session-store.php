<?php

declare(strict_types=1);

// Real Laravel managers over in-memory stores; no app boot or native runtime.
require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

use Claviron\Laravel\Providers\SessionServiceProvider;
use Illuminate\Cache\CacheManager;
use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;
use Illuminate\Session\SessionManager;

$app = new Application(sys_get_temp_dir());
$app->instance('config', new Repository([
    'cache' => [
        'default' => 'shared',
        'stores' => [
            'shared' => ['driver' => 'array'],
            'selected' => ['driver' => 'array'],
            'replacement' => ['driver' => 'array'],
        ],
    ],
    'session' => [
        'driver' => 'claviron',
        'store' => null,
        'lifetime' => 10,
        'cookie' => 'example_session',
    ],
]));
$cache = new CacheManager($app);
$sessions = new SessionManager($app);
$app->instance('session', $sessions);
new SessionServiceProvider($app)->boot($cache);

foreach ([null, '', 'selected'] as $selection) {
    $app['config']->set('session.store', $selection);
    $sessions->forgetDrivers();
    $store = $selection ?: 'shared';
    $cache->store($store)->put('source', $store, 60);
    $handler = $sessions->driver()->getHandler();
    if ($handler->read('source') !== $store) {
        throw new LogicException('Session did not select the configured cache store');
    }
    $handler->write('example-id', 'session-data');
    if ($handler->read('example-id') !== 'session-data') {
        throw new LogicException('Session payload did not round-trip');
    }
    $handler->destroy('example-id');
    if ($handler->read('example-id') !== '') {
        throw new LogicException('Session deletion did not remove the payload');
    }
}

$app['config']->offsetUnset('session.store');
$app['config']->set('cache.default', 'replacement');
$sessions->forgetDrivers();
$cache->store('replacement')->put('source', 'new-default', 60);
if ($sessions->driver()->getHandler()->read('source') !== 'new-default') {
    throw new LogicException('Session ignored the current configured default');
}

echo "Configured default and explicit session cache selection verified.\n";
