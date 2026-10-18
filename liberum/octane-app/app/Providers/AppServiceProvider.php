<?php

namespace App\Providers;

use App\Http\Controllers\OrbitalCellController;
use Claviron\Laravel\Database\PostgresConnection as ClavironPostgresConnection;
use Illuminate\Console\Events\CommandStarting;
use Illuminate\Database\Events\ConnectionEstablished;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use Laravel\Octane\Events\WorkerStarting;
use Laravel\Octane\Events\WorkerStopping;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Schema changes run on the plain-PDO connection, never on the native
     * Claviron one: `claviron` is the request hot path, and the host functions
     * it needs do not exist in a CLI process anyway.
     */
    private const SCHEMA_CONNECTION = 'pgsql';

    private bool $creatingSchema = false;

    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->app->singleton(OrbitalCellController::class);
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        $this->registerOrbitalCellLifecycle();
        $this->createSchemaOnConnect();
        $this->bootstrapSchemaBeforeMigrations();
    }

    private function registerOrbitalCellLifecycle(): void
    {
        if (! function_exists('claviron_runtime_available') || ! claviron_runtime_available()) {
            return;
        }

        // Resolve on the base application before Octane clones request sandboxes.
        Event::listen(WorkerStarting::class, static function (WorkerStarting $event): void {
            $event->app->make(OrbitalCellController::class);
        });

        Event::listen(WorkerStopping::class, static function (WorkerStopping $event): void {
            if ($event->app->resolved(OrbitalCellController::class)) {
                $event->app->make(OrbitalCellController::class)->release();
            }
        });
    }

    /**
     * The schema is created the moment the schema connection opens.
     *
     * A search_path naming a missing schema is accepted by PostgreSQL, but
     * the first unqualified CREATE then fails with "no schema has
     * been selected to create in". Doing it here fixes it at the root: every
     * entry point that opens this connection — migrate, tinker, a queue
     * worker — finds the schema in place before its first statement.
     *
     * Not the native connection: DDL has no business on the request hot path,
     * even though both connections now authenticate as the same role.
     *
     * The catalog is consulted first rather than leaning on `IF NOT EXISTS`:
     * PostgreSQL checks CREATE on the *database* before it checks whether the
     * schema is already there, so the bare statement would fail for the
     * runtime role on every single connect once the schema exists.
     */
    private function createSchemaOnConnect(): void
    {
        Event::listen(function (ConnectionEstablished $event): void {
            if ($event->connectionName !== self::SCHEMA_CONNECTION
                || $event->connection instanceof ClavironPostgresConnection
                || $this->creatingSchema) {
                return;
            }

            $this->creatingSchema = true;

            try {
                $schema = $event->connection->getConfig('search_path');

                if (! is_string($schema) || $schema === '' || str_contains($schema, ',')) {
                    return;
                }

                $present = $event->connection->selectOne(
                    'select 1 as present from pg_namespace where nspname = ?', [$schema]
                );

                if ($present !== null) {
                    return;
                }

                $event->connection->unprepared(
                    'create schema if not exists "'.str_replace('"', '""', $schema).'"'
                );
            } finally {
                $this->creatingSchema = false;
            }
        });
    }

    /**
     * Every migration command bootstraps the schema it needs first.
     *
     * Grants, not schema creation: `db:bootstrap` also sets ALTER DEFAULT
     * PRIVILEGES, which only applies to tables created *after* it runs — so it
     * has to happen before the migrations, not after.
     */
    private function bootstrapSchemaBeforeMigrations(): void
    {
        if (! $this->app->runningInConsole()) {
            return;
        }

        Event::listen(function (CommandStarting $event): void {
            $migrationCommands = [
                'migrate', 'migrate:fresh', 'migrate:install', 'migrate:refresh',
                'migrate:reset', 'migrate:rollback', 'migrate:status',
            ];

            if (! in_array($event->command, $migrationCommands, true)) {
                return;
            }

            Artisan::call('db:bootstrap', ['--connection' => self::SCHEMA_CONNECTION]);
        });
    }
}
