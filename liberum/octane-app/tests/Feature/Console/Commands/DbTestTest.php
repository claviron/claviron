<?php

declare(strict_types=1);

use Illuminate\Database\PostgresConnection;
use Illuminate\Support\Facades\DB;

if (! function_exists('claviron_runtime_available')) {
    function claviron_runtime_available(): bool
    {
        return false;
    }
}

if (! function_exists('claviron_postgres_connection')) {
    function claviron_postgres_connection(): never
    {
        throw new RuntimeException('Native PostgreSQL must not be selected outside Claviron.');
    }
}

it('reports a successful smoke test without leaving its temporary table behind', function (): void {
    config(['database.connections.sqlite.database' => ':memory:']);
    DB::purge('sqlite');

    $this->artisan('db:test', ['--connection' => 'sqlite'])
        ->expectsOutputToContain('Database smoke test passed.')
        ->assertSuccessful();

    $temporaryTables = DB::connection('sqlite')->selectOne(
        "select count(*) as aggregate from sqlite_temp_master where name like 'claviron_db_test_%'",
    );

    expect((int) $temporaryTables->aggregate)->toBe(0);
});

it('fails clearly when the requested connection does not exist', function (): void {
    $this->artisan('db:test', ['--connection' => 'missing'])
        ->expectsOutputToContain('Unable to resolve database connection [missing]')
        ->assertFailed();
});

it('uses the PDO connection when native symbols exist without an embed runtime', function (): void {
    if (function_exists('claviron_runtime_available') && claviron_runtime_available()) {
        $this->markTestSkipped('The Claviron PHP CLI boundary is unavailable.');
    }

    config(['database.connections.claviron-cli-probe' => [
        ...config('database.connections.pgsql'),
        'claviron' => 'octane',
    ]]);

    $connection = DB::connection('claviron-cli-probe');

    expect($connection)->toBeInstanceOf(PostgresConnection::class)
        ->not->toBeInstanceOf(Lumenize\Database\PostgresConnection::class);
});
