<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;

/**
 * Creates the schema the application lives in, and gives the runtime role
 * standing access to whatever migrations create later.
 *
 * The runtime role is the same role that runs the migrations, so the grants
 * are usually a no-op; they stay because they are what makes the schema
 * readable when the two are ever separated again.
 *
 * This cannot be a migration: Laravel creates its own `migrations` table
 * before running any of them, and with the application schema alone on the
 * search_path that fails with "no schema has been selected to create in"
 * while the schema is missing.
 *
 * Idempotent — safe to run before every migrate.
 */
final class DatabaseBootstrap extends Command
{
    protected $signature = 'db:bootstrap
        {--connection=pgsql : Connection that owns schema changes}
        {--schema= : Schema to create; defaults to the connection\'s search_path}
        {--role=claviron : Runtime role that reads and writes the tables}';

    protected $description = 'Create the application schema and grant the runtime role access to it';

    public function handle(): int
    {
        $connection = DB::connection($this->option('connection'));
        $schemaName = $this->schemaName($connection);

        if ($schemaName === null) {
            $this->error('no single schema on the connection; pass --schema');

            return self::FAILURE;
        }

        $schema = $this->quoteIdentifier($schemaName);
        $role = $this->quoteIdentifier((string) $this->option('role'));

        // The catalog is consulted first: PostgreSQL checks CREATE on the
        // *database* before it checks whether the schema already exists, so a
        // bare `create schema if not exists` fails for a role that only owns
        // its own schema.
        $present = $connection->selectOne(
            'select 1 as present from pg_namespace where nspname = ?',
            [$schemaName],
        );

        if ($present === null) {
            $connection->statement("create schema if not exists {$schema}");
        }

        $this->info("schema {$schema} ready");

        $roleExists = $connection->selectOne(
            'select 1 as present from pg_roles where rolname = ?',
            [(string) $this->option('role')],
        );

        if ($roleExists === null) {
            $this->warn("role {$role} does not exist; skipping grants");

            return self::SUCCESS;
        }

        foreach ([
            "grant usage on schema {$schema} to {$role}",
            // Applies to tables this connection creates from now on.
            "alter default privileges in schema {$schema} grant select, insert, update, delete on tables to {$role}",
            "alter default privileges in schema {$schema} grant usage, select on sequences to {$role}",
            // Applies to anything already there.
            "grant select, insert, update, delete on all tables in schema {$schema} to {$role}",
            "grant usage, select on all sequences in schema {$schema} to {$role}",
        ] as $statement) {
            $connection->statement($statement);
        }

        $this->info("granted {$role} access to {$schema}");

        return self::SUCCESS;
    }

    /**
     * The schema to create: the option when given, otherwise the connection's
     * own search_path, so config/database.php stays the single source of truth.
     * A multi-schema path is ambiguous and rejected rather than guessed at.
     */
    private function schemaName(Connection $connection): ?string
    {
        $schema = (string) ($this->option('schema') ?? '');

        if ($schema === '') {
            $searchPath = $connection->getConfig('search_path');
            $schema = is_string($searchPath) ? trim($searchPath) : '';
        }

        if ($schema === '' || str_contains($schema, ',')) {
            return null;
        }

        return $schema;
    }

    /**
     * Identifiers come from options, so they are quoted rather than trusted.
     */
    private function quoteIdentifier(string $name): string
    {
        return '"'.str_replace('"', '""', $name).'"';
    }
}
