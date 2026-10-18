<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Database\PostgresConnection;
use Claviron\Laravel\Database\MySqlConnection;
use Illuminate\Database\Connection;
use Illuminate\Database\Connectors\ConnectionFactory;
use Illuminate\Database\DatabaseManager;
use Illuminate\Support\Arr;
use Illuminate\Support\ServiceProvider;
use InvalidArgumentException;

/**
 * Takes over any `pgsql` connection whose config says `'claviron' => true`
 * (the runtime's `default` pool) or `'claviron' => 'pool-name'`.
 *
 * The decision is made once, when the connection is first resolved: inside
 * the runtime the connection is served from the named Claviron pool; anywhere
 * the host functions do not exist — a shell, a test process — the very same
 * profile is handed back to Laravel's factory and becomes the plain PDO
 * connection it always was. The native functions may still exist in a
 * Claviron-built PHP CLI binary, so availability is determined by the runtime
 * registration rather than by symbol presence alone. Nothing about this runs
 * per request.
 */
final class DatabaseServiceProvider extends ServiceProvider
{
    public function boot(DatabaseManager $database): void
    {
        $database->extend('mysql', function (array $config, string $name): Connection {
            $pool = $config['claviron'] ?? null;
            if ($pool === null || $pool === false) {
                return $this->factory()->make($config, $name);
            }
            if (isset($config['read']) || isset($config['write'])) {
                throw new InvalidArgumentException(
                    "Database connection [{$name}] cannot combine `claviron` with a read/write split.",
                );
            }
            if (!function_exists('claviron_runtime_configured') || !claviron_runtime_configured()) {
                return $this->factory()->make($config, $name);
            }

            // MySqlConnection fails closed if the host omitted its mysql
            // feature, even when Composer loaded the fallback function stubs.
            // Connection options belong to db.yaml, not PDO's init commands.
            return new MySqlConnection(
                is_string($pool) ? $pool : 'default',
                (string) ($config['database'] ?? ''),
                (string) ($config['prefix'] ?? ''),
                Arr::add(Arr::add($config, 'prefix', ''), 'name', $name),
            );
        });
        $database->extend('pgsql', function (array $config, string $name): Connection {
            $pool = $config['claviron'] ?? null;

            if ($pool === null || $pool === false) {
                return $this->factory()->make($config, $name);
            }

            if (isset($config['read']) || isset($config['write'])) {
                throw new InvalidArgumentException(
                    "Database connection [{$name}] cannot combine `claviron` with a read/write split.",
                );
            }

            if (!function_exists('claviron_postgres_connection')
                || !function_exists('claviron_runtime_configured')
                || !claviron_runtime_configured()) {
                return $this->factory()->make($config, $name);
            }

            // The pool's URL owns the connection options. `search_path` in
            // the profile is for the PDO path; on this one the connection
            // answers it from the server, so it must not shadow that.
            unset($config['search_path']);

            return new PostgresConnection(
                is_string($pool) ? $pool : 'default',
                (string) ($config['database'] ?? ''),
                (string) ($config['prefix'] ?? ''),
                // `name` and `prefix` are what ConnectionFactory::parseConfig()
                // adds, and `extend()` bypasses that factory entirely. Without
                // `name`, Connection::getName() is null, so every model
                // hydrated from this connection is stamped
                // setConnection(null) by Builder::newModelInstance() and
                // silently falls back to the *default* connection on its next
                // write — outside this connection's transaction.
                Arr::add(Arr::add($config, 'prefix', ''), 'name', $name),
            );
        });
    }

    private function factory(): ConnectionFactory
    {
        return $this->app['db.factory'];
    }
}
