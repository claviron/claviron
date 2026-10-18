<?php

declare(strict_types=1);

// Only the native boundary is replaced. No application boot, fleet or DB I/O.
$prepared = false;
function claviron_runtime_available(): bool { return true; }
function claviron_runtime_configured(): bool { return $GLOBALS['prepared']; }
function claviron_cache_store(?string $name = null): never { throw new RuntimeException('native cache selected'); }
function claviron_postgres_connection(string $name): never { throw new RuntimeException('native PostgreSQL selected'); }

require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

$app = new \Illuminate\Foundation\Application(sys_get_temp_dir());
$app->instance('config', new \Illuminate\Config\Repository([
    'cache' => ['default' => 'claviron', 'stores' => [
        'claviron' => ['driver' => 'claviron', 'fallback' => 'array'],
        'array' => ['driver' => 'array'],
    ]],
    'database' => ['connections' => [
        'pgsql' => ['driver' => 'pgsql', 'claviron' => true, 'database' => 'example'],
        'mysql' => ['driver' => 'mysql', 'claviron' => true, 'database' => 'example'],
    ]],
]));
$factory = new \Illuminate\Database\Connectors\ConnectionFactory($app);
$app->instance('db.factory', $factory);
$cache = new \Illuminate\Cache\CacheManager($app);
$database = new \Illuminate\Database\DatabaseManager($app, $factory);
(new \Claviron\Laravel\Providers\CacheServiceProvider($app))->boot($cache);
(new \Claviron\Laravel\Providers\DatabaseServiceProvider($app))->boot($database);

if (! $cache->store()->getStore() instanceof \Illuminate\Cache\ArrayStore) {
    throw new LogicException('Detached engine ignored the declared cache fallback');
}
foreach (['pgsql' => \Illuminate\Database\PostgresConnection::class, 'mysql' => \Illuminate\Database\MySqlConnection::class] as $driver => $class) {
    // Resolving a plain Laravel connection keeps PDO lazy; no connection is opened.
    if ($database->connection($driver)::class !== $class) {
        throw new LogicException('Detached engine selected a native database adapter');
    }
    $database->purge($driver);
}

$prepared = true;
$cache->forgetDriver('claviron');
foreach ([
    'native cache selected' => fn () => $cache->store(),
    'native PostgreSQL selected' => fn () => $database->connection('pgsql'),
    // Unlike symbol presence, the MySQL adapter also requires native functions.
    'The Claviron MySQL runtime is unavailable; enable the mysql feature.' => fn () => $database->connection('mysql'),
] as $message => $operation) {
    try { $operation(); } catch (RuntimeException $error) {
        if ($error->getMessage() === $message) { continue; }
        throw $error;
    }
    throw new LogicException('Prepared runtime silently fell back: '.$message);
}
echo "Runtime selection passed: declared fallback/PDO when detached, native adapters when prepared.\n";
