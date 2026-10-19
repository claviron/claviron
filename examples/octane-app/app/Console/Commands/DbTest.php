<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Attributes\Description;
use Illuminate\Console\Attributes\Signature;
use Illuminate\Console\Command;
use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use RuntimeException;
use Throwable;

#[Signature('db:test {--connection= : Database connection to exercise}')]
#[Description('Exercise the configured database connection without changing persistent data')]
final class DbTest extends Command
{
    public function handle(): int
    {
        $connectionName = (string) ($this->option('connection') ?: config('database.default'));

        try {
            $connection = DB::connection($connectionName);
        } catch (Throwable $exception) {
            $this->error("Unable to resolve database connection [{$connectionName}]: {$exception->getMessage()}");

            return self::FAILURE;
        }

        $runtimeAvailable = function_exists('claviron_runtime_available')
            && claviron_runtime_available();

        $this->table(['Runtime', 'Value'], [
            ['PHP binary', PHP_BINARY],
            ['PHP version', PHP_VERSION],
            ['Execution', $runtimeAvailable ? 'Claviron embed' : 'PHP CLI'],
            ['Connection', $connectionName],
            ['Driver', $connection->getDriverName()],
            ['Connection class', $connection::class],
        ]);

        $probes = [
            'connect and select' => fn (): string => (string) $connection
                ->selectOne('select 1 as value')->value,
            ...($connection->getDriverName() === 'pgsql' ? [
                'current schema' => fn (): string => $this->assertCurrentSchema($connection),
            ] : []),
            'bound parameters' => fn (): string => (string) $connection
                ->selectOne('select ? as value', ['bound-value'])->value,
            'transaction commit' => fn (): string => (string) $connection->transaction(
                fn (): int => (int) $connection->selectOne('select 42 as value')->value,
            ),
            'transaction rollback' => fn (): string => $this->exerciseRollback($connection),
            'temporary DML and cursor' => fn (): string => $this->exerciseTemporaryTable($connection),
            'connection after probes' => fn (): string => (string) $connection
                ->selectOne('select 1 as value')->value,
        ];

        $rows = [];
        $failed = false;

        foreach ($probes as $name => $probe) {
            $startedAt = hrtime(true);

            try {
                $result = $probe();
                $status = 'PASS';
            } catch (Throwable $exception) {
                $failed = true;
                $status = 'FAIL';
                $result = $exception::class.': '.$exception->getMessage();
            }

            $rows[] = [
                $name,
                $status,
                number_format((hrtime(true) - $startedAt) / 1_000_000, 2).' ms',
                $result,
            ];
        }

        $this->table(['Probe', 'Status', 'Duration', 'Result'], $rows);

        if ($failed) {
            $this->error('Database smoke test failed.');

            return self::FAILURE;
        }

        $this->info('Database smoke test passed.');

        return self::SUCCESS;
    }

    private function assertCurrentSchema(Connection $connection): string
    {
        $actual = (string) $connection->selectOne('select current_schema() as schema')->schema;
        $configured = $connection->getConfig('search_path') ?? $connection->getConfig('schema');
        $schemas = is_array($configured) ? $configured : preg_split('/[\s,"\']+/', (string) $configured);
        $expected = trim((string) ($schemas[0] ?? ''), '\'"');

        if ($expected !== '' && $actual !== $expected) {
            throw new RuntimeException("expected schema {$expected}, got {$actual}");
        }

        return $actual;
    }

    private function exerciseRollback(Connection $connection): string
    {
        try {
            $connection->transaction(function () use ($connection): never {
                $connection->selectOne('select 1 as value');

                throw new RuntimeException('intentional rollback');
            });
        } catch (RuntimeException $exception) {
            if ($exception->getMessage() === 'intentional rollback') {
                return 'rolled back';
            }

            throw $exception;
        }

        throw new RuntimeException('rollback probe did not throw');
    }

    private function exerciseTemporaryTable(Connection $connection): string
    {
        $table = 'claviron_db_test_'.getmypid();

        $connection->statement("drop table if exists {$table}");

        try {
            $connection->statement(
                "create temporary table {$table} (id integer primary key, value varchar(64) not null)",
            );
            $connection->insert(
                "insert into {$table} (id, value) values (?, ?)",
                [1, 'created'],
            );
            $connection->update(
                "update {$table} set value = ? where id = ?",
                ['updated', 1],
            );

            $rows = iterator_to_array(
                $connection->cursor("select id, value from {$table} order by id"),
            );

            if (count($rows) !== 1 || (int) $rows[0]->id !== 1 || $rows[0]->value !== 'updated') {
                throw new RuntimeException('temporary table returned an unexpected row');
            }

            if ($connection->delete("delete from {$table} where id = ?", [1]) !== 1) {
                throw new RuntimeException('temporary table delete affected an unexpected row count');
            }

            return 'insert, update, cursor, delete';
        } finally {
            $connection->statement("drop table if exists {$table}");
        }
    }
}
