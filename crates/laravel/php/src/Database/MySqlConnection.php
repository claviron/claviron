<?php

declare(strict_types=1);

namespace Claviron\Laravel\Database;

use Closure;
use Illuminate\Database\MySqlConnection as IlluminateMySqlConnection;
use LogicException;
use PDO;
use RuntimeException;
use Throwable;

/** Laravel's MySQL grammar and Eloquent API over the runtime-owned pool. */
final class MySqlConnection extends IlluminateMySqlConnection
{
    private \Claviron\Runtime\MySqlConnection $nativeConnection;

    private ?string $serverVersion = null;

    public function __construct(string $pool, string $database = '', string $prefix = '', array $config = [])
    {
        if (!function_exists('claviron_mysql_connection')
            || !(new \ReflectionFunction('claviron_mysql_connection'))->isInternal()
            || !function_exists('claviron_runtime_available')
            || !claviron_runtime_available()) {
            throw new RuntimeException('The Claviron MySQL runtime is unavailable; enable the mysql feature.');
        }

        $this->nativeConnection = claviron_mysql_connection($pool);
        parent::__construct(
            static fn (): never => throw new LogicException('Claviron MySQL has no PDO handle.'),
            $database, $prefix, $config,
        );
    }

    public function select($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        if ($fetchUsing !== [] && $fetchUsing !== [PDO::FETCH_OBJ]) {
            throw new LogicException('Custom PDO fetch modes are unavailable on Claviron MySQL.');
        }

        return $this->run($query, $bindings, function ($query, $bindings): array {
            if ($this->pretending()) {
                return [];
            }

            $result = $this->executeNative($query, $bindings);
            $names = array_column($result['columns'], 'name');

            return array_map(static function (array $values) use ($names): object {
                $row = new \stdClass();
                foreach ($names as $index => $name) {
                    $row->{$name} = $values[$index] ?? null;
                }

                return $row;
            }, $result['rows']);
        });
    }

    public function selectResultSets($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        throw new LogicException('Multiple MySQL result sets are not supported by the native adapter.');
    }

    /** Buffered, like PDO MySQL's default; not a server-side streaming cursor. */
    public function cursor($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        yield from $this->select($query, $bindings, $useReadPdo, $fetchUsing);
    }

    public function insert($query, $bindings = [], $sequence = null)
    {
        return $this->run($query, $bindings, function ($query, $bindings): bool {
            if ($this->pretending()) {
                return true;
            }
            $result = $this->executeNative($query, $bindings);
            $this->lastInsertId = $result['insert_id'];
            $this->recordsHaveBeenModified();

            return true;
        });
    }

    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings): bool {
            if (!$this->pretending()) {
                $this->executeNative($query, $bindings);
                $this->recordsHaveBeenModified();
            }

            return true;
        });
    }

    public function affectingStatement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings): int {
            if ($this->pretending()) {
                return 0;
            }
            $affected = (int) $this->executeNative($query, $bindings)['affected_rows'];
            $this->recordsHaveBeenModified($affected > 0);

            return $affected;
        });
    }

    public function unprepared($query)
    {
        return $this->run($query, [], function ($query): bool {
            if (!$this->pretending()) {
                self::native(fn () => claviron_mysql_unprepared($this->nativeConnection, $query));
                $this->recordsHaveBeenModified();
            }

            return true;
        });
    }

    public function beginTransaction()
    {
        foreach ($this->beforeStartingTransaction as $callback) {
            $callback($this);
        }
        if ($this->transactionLevel() === 0) {
            self::native(fn () => claviron_mysql_begin($this->nativeConnection));
        } else {
            $this->executeNative('SAVEPOINT trans'.($this->transactions + 1), []);
        }
        $this->transactions++;
        $this->transactionsManager?->begin($this->getName(), $this->transactions);
        $this->fireConnectionEvent('beganTransaction');
    }

    public function transaction(Closure $callback, $attempts = 1)
    {
        for ($attempt = 1; $attempt <= $attempts; $attempt++) {
            $this->beginTransaction();
            try {
                $result = $callback($this);
            } catch (Throwable $exception) {
                $this->handleTransactionException($exception, $attempt, $attempts);
                continue;
            }
            try {
                $this->commit();
            } catch (Throwable $exception) {
                $this->handleCommitTransactionException($exception, $attempt, $attempts);
                continue;
            }

            return $result;
        }

        throw new LogicException('Claviron MySQL transaction attempts exhausted.');
    }

    protected function handleCommitTransactionException(Throwable $e, $currentAttempt, $maxAttempts)
    {
        // Only a server-reported concurrency failure can be retried. An I/O
        // failure during COMMIT has an unknown outcome and must propagate.
        $this->transactions = max(0, $this->transactions - 1);
        if ($this->causedByConcurrencyError($e) && $currentAttempt < $maxAttempts) {
            if (claviron_mysql_in_transaction($this->nativeConnection)) {
                self::native(fn () => claviron_mysql_rollback($this->nativeConnection));
            }
            return;
        }
        if ($this->causedByLostConnection($e)) {
            $this->transactions = 0;
        }
        throw $e;
    }

    public function commit()
    {
        if ($this->transactions === 1) {
            $this->fireConnectionEvent('committing');
            self::native(fn () => claviron_mysql_commit($this->nativeConnection));
        }
        [$level, $this->transactions] = [$this->transactions, max(0, $this->transactions - 1)];
        $this->transactionsManager?->commit($this->getName(), $level, $this->transactions);
        $this->fireConnectionEvent('committed');
    }

    public function rollBack($toLevel = null)
    {
        $toLevel ??= $this->transactions - 1;
        if ($toLevel < 0 || $toLevel >= $this->transactions) {
            return;
        }
        if (claviron_mysql_in_transaction($this->nativeConnection)) {
            if ($toLevel === 0) {
                self::native(fn () => claviron_mysql_rollback($this->nativeConnection));
            } else {
                $this->executeNative('ROLLBACK TO SAVEPOINT trans'.($toLevel + 1), []);
            }
        } else {
            $toLevel = 0;
        }
        $this->transactions = $toLevel;
        $this->transactionsManager?->rollback($this->getName(), $this->transactions);
        $this->fireConnectionEvent('rollingBack');
    }

    public function transactionLevel()
    {
        if ($this->transactions > 0 && !claviron_mysql_in_transaction($this->nativeConnection)) {
            $this->transactions = 0;
        }

        return $this->transactions;
    }

    public function getPdo()
    {
        throw new LogicException('Claviron MySQL intentionally exposes no PDO handle.');
    }

    public function disconnect()
    {
        self::native(fn () => claviron_mysql_disconnect($this->nativeConnection));
        $this->transactions = 0;
        $this->lastInsertId = null;
        $this->transactionsManager?->rollback($this->getName(), 0);
        // Keep the parent's no-PDO sentinel: this object names a pool, and
        // the next statement obtains a fresh native lease without a reconnector.
    }

    public function getServerVersion(): string
    {
        $version = $this->version();
        return preg_match('/(?:5\.5\.5-)?(\d+\.\d+\.\d+)/', $version, $match) ? $match[1] : $version;
    }

    public function isMaria()
    {
        return str_contains($this->version(), 'MariaDB');
    }

    protected function escapeString($value)
    {
        // A hex literal does not depend on NO_BACKSLASH_ESCAPES. Charset is
        // restricted to a SQL identifier, never arbitrary config interpolation.
        $charset = $this->getConfig('charset') ?? 'utf8mb4';
        if (!preg_match('/\A[a-zA-Z0-9_]+\z/', $charset)) {
            throw new LogicException('Invalid MySQL charset.');
        }
        return "CONVERT(X'".bin2hex($value)."' USING {$charset})";
    }

    private function version(): string
    {
        return $this->serverVersion ??= (string) $this->executeNative('SELECT VERSION()', [])['rows'][0][0];
    }

    private function executeNative(string $query, array $bindings): array
    {
        return self::native(fn () => claviron_mysql_execute(
            $this->nativeConnection, $query, $this->prepareBindings($bindings),
        ));
    }

    private static function native(Closure $operation): mixed
    {
        try {
            return $operation();
        } catch (Throwable $failure) {
            throw QueryException::from($failure);
        }
    }
}
