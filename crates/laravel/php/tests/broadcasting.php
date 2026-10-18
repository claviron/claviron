<?php

declare(strict_types=1);

require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

use Claviron\Laravel\Broadcasting\Broadcaster;
use Claviron\Laravel\Broadcasting\UnconfiguredBroadcaster;
use Claviron\Laravel\Providers\BroadcastingProvider;
use Illuminate\Broadcasting\BroadcastManager;
use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;

$app = new Application(sys_get_temp_dir());
$app->instance('config', new Repository([
    'broadcasting' => [
        'default' => 'claviron',
        'connections' => [
            'claviron' => ['driver' => 'claviron', 'key' => null, 'secret' => null],
        ],
    ],
]));
$manager = new BroadcastManager($app);
(new BroadcastingProvider($app))->boot($manager);

$manager->channel('example', static fn (): bool => true);
$unconfigured = $manager->connection();
if (! $unconfigured instanceof UnconfiguredBroadcaster) {
    throw new LogicException('Missing credentials created the configured broadcaster.');
}

foreach ([
    static fn () => $unconfigured->broadcast(['example'], 'event'),
    static fn () => $unconfigured->auth(null),
] as $operation) {
    try {
        $operation();
    } catch (LogicException $error) {
        if ($error->getMessage() === 'Claviron broadcasting requires a Pusher key and secret.') {
            continue;
        }
        throw $error;
    }
    throw new LogicException('An unconfigured broadcaster accepted a runtime operation.');
}

$app['config']->set('broadcasting.connections.claviron.key', 'example-key');
$app['config']->set('broadcasting.connections.claviron.secret', 'example-secret');
$manager->purge('claviron');
if (! $manager->connection() instanceof Broadcaster) {
    throw new LogicException('Configured credentials did not create the real broadcaster.');
}

echo "Broadcast driver selection passed.\n";
