<?php

declare(strict_types=1);

// Real configuration/provider contracts without booting an application or host.
require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

use Claviron\Laravel\Providers\LumenizeProvider;
use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;
use Illuminate\Support\ServiceProvider;

$check = static function (bool $condition, string $message): void {
    if (! $condition) {
        throw new LogicException($message);
    }
};
$application = static function (bool $console, array $config = [], bool $cached = false): Application {
    $app = new class(sys_get_temp_dir(), $console, $cached) extends Application {
        public function __construct(string $path, private bool $console, private bool $cached)
        {
            parent::__construct($path);
        }

        public function runningInConsole(): bool
        {
            return $this->console;
        }

        public function configurationIsCached(): bool
        {
            return $this->cached;
        }
    };
    $app->instance('config', new Repository($config));

    return $app;
};
$publishes = ServiceProvider::$publishes;
$groups = ServiceProvider::$publishGroups;

try {
    ServiceProvider::$publishes = [];
    ServiceProvider::$publishGroups = [];

    $http = $application(false);
    $provider = new LumenizeProvider($http);
    $provider->register();
    $provider->boot();
    $check(! $http['config']->has('lumenize'), 'HTTP boot received console defaults');
    $check(ServiceProvider::pathsToPublish(LumenizeProvider::class) === [], 'HTTP boot registered console publishing');

    $console = $application(true);
    $provider = new LumenizeProvider($console);
    $provider->register();
    $provider->boot();
    $check($console['config']->get('lumenize.config_path') === 'lumenize.yaml', 'Missing config did not receive the default document path');
    $check(ServiceProvider::pathsToPublish(LumenizeProvider::class, 'lumenize-config') === [
        dirname(__DIR__).'/config/lumenize.php' => $console->configPath('lumenize.php'),
    ], 'Publish target is not the application config directory');

    $custom = $application(true, ['lumenize' => ['config_path' => 'config/example.yaml', 'application_option' => true]]);
    (new LumenizeProvider($custom))->register();
    $check($custom['config']->get('lumenize.config_path') === 'config/example.yaml', 'Application path was overwritten');
    $check($custom['config']->get('lumenize.application_option') === true, 'Application configuration was discarded');

    $cached = $application(true, ['lumenize' => ['config_path' => 'config/cached.yaml']], true);
    (new LumenizeProvider($cached))->register();
    $check($cached['config']->all() === ['lumenize' => ['config_path' => 'config/cached.yaml']], 'Cached configuration was changed');

    echo "Lumenize provider passed: console defaults, application overrides, publishing, config cache and HTTP isolation.\n";
} finally {
    ServiceProvider::$publishes = $publishes;
    ServiceProvider::$publishGroups = $groups;
}
