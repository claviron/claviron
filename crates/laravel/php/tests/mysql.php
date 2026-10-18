<?php

declare(strict_types=1);

namespace {
    // php tests/mysql.php /path/to/a/consumer/vendor/autoload.php
    require $argv[1];
}

namespace Claviron\Runtime {
    if (!class_exists(MySqlConnection::class, false)) {
        final class MySqlConnection {}
    }
}

namespace Claviron\Laravel\Database {
    use Illuminate\Database\Connection;
    use Illuminate\Database\DatabaseTransactionsManager;
    use Illuminate\Database\UniqueConstraintViolationException;
    use RuntimeException;

    final class Backend
    {
        public static bool $transaction = false;
        public static array $calls = [];
        public static ?string $failure = null;
        public static bool $failCommit = false;
    }

    function claviron_mysql_execute($handle, $sql, $parameters = []): array
    {
        Backend::$calls[] = [$sql, $parameters];
        if ($failure = Backend::$failure) {
            Backend::$failure = null;
            throw new RuntimeException($failure);
        }
        return [
            'columns' => [['name' => 'credit'], ['name' => 'content']],
            'rows' => [['123456789012345678.123456', "\x00\xff"]],
            'affected_rows' => 2, 'insert_id' => '41',
        ];
    }
    function claviron_mysql_begin($handle): bool
    {
        Backend::$calls[] = ['BEGIN'];
        return Backend::$transaction = true;
    }
    function claviron_mysql_commit($handle): bool
    {
        Backend::$calls[] = ['COMMIT'];
        Backend::$transaction = false;
        if (Backend::$failCommit) {
            throw new RuntimeException('Lost connection to MySQL server during query');
        }
        return true;
    }
    function claviron_mysql_rollback($handle): bool
    {
        Backend::$calls[] = ['ROLLBACK'];
        Backend::$transaction = false;
        return true;
    }
    function claviron_mysql_in_transaction($handle): bool { return Backend::$transaction; }
    function claviron_mysql_disconnect($handle): bool
    {
        Backend::$transaction = false;
        Backend::$calls[] = ['DISCONNECT'];
        return true;
    }

    function check(bool $condition, string $message): void
    {
        if (!$condition) { throw new RuntimeException($message); }
    }

    // The test replaces only the native boundary, not Laravel's transaction
    // manager, query pipeline, grammar, processor or exception conversion.
    $reflection = new \ReflectionClass(MySqlConnection::class);
    $db = $reflection->newInstanceWithoutConstructor();
    $reflection->getProperty('nativeConnection')->setValue($db, (new \ReflectionClass(\Claviron\Runtime\MySqlConnection::class))->newInstanceWithoutConstructor());
    (new \ReflectionMethod(Connection::class, '__construct'))->invoke(
        $db, fn () => throw new RuntimeException('PDO must never be accessed'),
        'fixture', '', ['name' => 'mysql', 'driver' => 'mysql', 'charset' => 'utf8mb4'],
    );
    $db->setTransactionManager(new DatabaseTransactionsManager());
    check($db->getName() === 'mysql', 'Eloquent must retain its connection name');

    $row = $db->selectOne('SELECT ?, "?"', ["\x00\xff"]);
    check($row->credit === '123456789012345678.123456', 'DECIMAL must remain text');
    check($row->content === "\x00\xff", 'BLOB must preserve bytes');
    check(Backend::$calls[0] === ['SELECT ?, "?"', ["\x00\xff"]], 'MySQL placeholders/binary binding changed');
    check($db->table('entries')->insertGetId(['credit' => '0.000001']) === 41, 'insertGetId bypassed native insert ID');

    Backend::$calls = [];
    $callbacks = 0;
    $db->beginTransaction();
    $db->beginTransaction();
    $db->afterCommit(function () use (&$callbacks) { $callbacks++; });
    $db->commit();
    check($callbacks === 0 && $db->transactionLevel() === 1, 'inner commit committed the cashier transaction');
    $db->commit();
    check($callbacks === 1 && Backend::$calls === [['BEGIN'], ['SAVEPOINT trans2', []], ['COMMIT']], 'outer commit contract');

    Backend::$calls = [];
    $db->beginTransaction();
    $db->beginTransaction();
    $db->rollBack(1);
    check($db->transactionLevel() === 1 && Backend::$calls[2][0] === 'ROLLBACK TO SAVEPOINT trans2', 'nested rollback');
    $db->rollBack();
    check(!Backend::$transaction && $db->transactionLevel() === 0, 'outer rollback');

    $runs = 0;
    try {
        $db->transaction(function () use (&$runs) { $runs++; throw new RuntimeException('business rule'); }, 3);
    } catch (RuntimeException $error) {
        check($error->getMessage() === 'business rule', 'original business failure lost');
    }
    check($runs === 1 && !Backend::$transaction, 'business errors must not retry');

    $runs = 0;
    $db->transaction(function ($db) use (&$runs) {
        if (++$runs === 1) {
            Backend::$failure = 'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found';
        }
        $db->statement('UPDATE entries SET credit = ?', ['0.000001']);
    }, 2);
    check($runs === 2, 'server-reported deadlock must retain Laravel retry semantics');

    Backend::$failure = 'SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry';
    try {
        $db->statement('INSERT INTO entries VALUES (?)', [1]);
        throw new RuntimeException('duplicate key must throw');
    } catch (UniqueConstraintViolationException $error) {
        check($error->errorInfo[1] === 1062, 'MySQL errno lost');
    }

    $runs = 0;
    Backend::$failCommit = true;
    try {
        $db->transaction(function () use (&$runs) { $runs++; }, 3);
        throw new RuntimeException('ambiguous commit must throw');
    } catch (QueryException $error) {
        check(str_contains($error->getMessage(), 'Lost connection'), 'commit failure was masked');
    }
    check($runs === 1, 'ambiguous COMMIT must not replay the cashier operation');
    Backend::$failCommit = false;

    $db->beginTransaction();
    Backend::$transaction = false; // runtime ended an abandoned work unit
    check($db->transactionLevel() === 0, 'stale Octane transaction count survived');
    check(str_contains($db->escape("a'\\b"), "61275c62"), 'escape depends on server backslash mode');
    $db->beginTransaction();
    $db->disconnect();
    check(!$db->transactionLevel() && $db->getLastInsertId() === null, 'disconnect retained request state');
    check($db->selectOne('SELECT 1')->credit === '123456789012345678.123456', 'disconnected adapter tried to reconnect PDO');
    echo "PASS: MySQL Laravel adapter, native IDs/types, nested transactions, callbacks, retries and ambiguous commit\n";
}
