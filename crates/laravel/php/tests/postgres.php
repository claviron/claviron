<?php

declare(strict_types=1);

namespace {
    // php tests/postgres.php /path/to/a/consumer/vendor/autoload.php
    require $argv[1];
}

namespace Claviron\Runtime {
    if (!class_exists(PostgresConnection::class, false)) {
        final class PostgresConnection {}
    }
}

namespace Claviron\Laravel\Database {
    use Illuminate\Database\Connection;
    use Illuminate\Database\DatabaseTransactionsManager;
    use RuntimeException;

    final class Backend
    {
        public static bool $transaction = false;
        public static array $calls = [];
    }

    function claviron_postgres_execute($handle, $sql, $parameters = []): array
    {
        Backend::$calls[] = [$sql, $parameters];

        return ['columns' => [['name' => 'value']], 'rows' => [[1]], 'affected_rows' => 0];
    }

    function claviron_postgres_begin($handle): bool
    {
        if (Backend::$transaction) {
            throw new RuntimeException('a PostgreSQL transaction is already open on this connection');
        }

        Backend::$calls[] = ['BEGIN'];

        return Backend::$transaction = true;
    }

    function claviron_postgres_commit($handle): bool
    {
        Backend::$calls[] = ['COMMIT'];
        Backend::$transaction = false;

        return true;
    }

    function claviron_postgres_rollback($handle): bool
    {
        Backend::$calls[] = ['ROLLBACK'];
        Backend::$transaction = false;

        return true;
    }

    function claviron_postgres_in_transaction($handle): bool
    {
        return Backend::$transaction;
    }

    function claviron_postgres_disconnect($handle): bool
    {
        Backend::$calls[] = ['DISCONNECT'];
        Backend::$transaction = false;

        return true;
    }

    function check(bool $condition, string $message): void
    {
        if (!$condition) {
            throw new RuntimeException($message);
        }
    }

    // Substitute only the native boundary. Keep Laravel's actual query,
    // reconnect, savepoint and afterCommit pipelines.
    $reflection = new \ReflectionClass(PostgresConnection::class);
    $db = $reflection->newInstanceWithoutConstructor();
    $reflection->getProperty('nativeConnection')->setValue(
        $db, (new \ReflectionClass(\Claviron\Runtime\PostgresConnection::class))->newInstanceWithoutConstructor(),
    );
    $sentinel = fn () => throw new RuntimeException('PDO must never be accessed');
    (new \ReflectionMethod(Connection::class, '__construct'))->invoke(
        $db, $sentinel, 'fixture', '', ['name' => 'pgsql', 'driver' => 'pgsql', 'search_path' => 'public'],
    );
    $db->setTransactionManager(new DatabaseTransactionsManager());
    $reconnections = 0;
    $db->setReconnector(function ($connection) use (&$reconnections, $sentinel): void {
        ++$reconnections;
        // DatabaseManager::refreshPdoConnections does this, resetting the
        // Laravel nesting count without touching an already-open native lease.
        $connection->setPdo($sentinel)->setReadPdo(null);
    });

    // An Octane operation disconnects; the next payment constructor begins
    // before its first query. That query must not erase its transaction level.
    $db->disconnect();
    $db->beginTransaction();
    $db->statement('SELECT pg_advisory_xact_lock(?, ?)', [1, 101]);
    check($db->transactionLevel() === 1 && Backend::$transaction, 'First query after disconnect erased the native transaction level');
    check($reconnections === 0, 'Native PostgreSQL entered the PDO reconnect path');

    $callbacks = 0;
    $db->beginTransaction();
    $db->afterCommit(function () use (&$callbacks): void { ++$callbacks; });
    $db->commit();
    check($callbacks === 0 && $db->transactionLevel() === 1, 'Inner commit ended the outer cashier transaction');
    $db->commit();
    check($callbacks === 1 && !Backend::$transaction, 'Outer commit or callback contract changed');

    $db->beginTransaction();
    $db->beginTransaction();
    $db->rollBack(1);
    check($db->transactionLevel() === 1 && Backend::$transaction, 'Savepoint rollback ended the native transaction');
    $db->rollBack();
    check($db->transactionLevel() === 0 && !Backend::$transaction, 'Outer rollback retained transaction state');

    $db->beginTransaction();
    $db->afterCommit(function () use (&$callbacks): void { ++$callbacks; });
    $db->disconnect();
    $db->disconnect(); // also safe when the native request boundary ran first
    check($db->transactionLevel() === 0 && !Backend::$transaction, 'Disconnect retained an abandoned transaction');
    $db->beginTransaction();
    $db->selectOne('SELECT 1');
    $db->commit();
    check($callbacks === 1, 'Disconnect retained abandoned afterCommit callbacks');
    check($reconnections === 0, 'Repeated operations lost the no-PDO sentinel');

    $db->beginTransaction();
    Backend::$transaction = false; // native end_request rolled back leftovers
    check($db->transactionLevel() === 0, 'Native request cleanup left stale PHP bookkeeping');
    $db->disconnect();

    echo "PASS: PostgreSQL disconnect, next-query transaction ownership, savepoints, callbacks and repeated operations\n";
}
