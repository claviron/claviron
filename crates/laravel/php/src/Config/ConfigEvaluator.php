<?php

declare(strict_types=1);

namespace Claviron\Laravel\Config;

use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;
use Illuminate\Support\Env;
use Illuminate\Support\Facades\Facade;

/** Context for a Laravel-authored document that Claviron, not Laravel, reads. */
final class ConfigEvaluator
{
    /** @param array<string, string> $environment */
    public static function prepare(string $basePath, array $environment): void
    {
        // Env's normal boolean/null/default handling remains Laravel's. Inputs
        // are the host's pinned snapshot, never another dotenv read or putenv.
        $_ENV = $environment;
        $_SERVER = $environment;
        Env::disablePutenv();

        $app = new Application($basePath);
        $app->instance('config', new Repository);
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($app);

        // No bootstrap/app.php, LoadConfiguration, service-provider boot,
        // database connection or Octane receive loop. app()/base_path() and
        // autoloaded helpers are available; booted services are not promised.
    }
}
