<?php

declare(strict_types=1);

namespace Claviron\Laravel\Database;

use Closure;
use DateTimeInterface;
use Illuminate\Database\PostgresConnection as IlluminatePostgresConnection;
use LogicException;
use PDO;
use RuntimeException;
use Throwable;

final class PostgresConnection extends IlluminatePostgresConnection
{
    private \Claviron\Runtime\PostgresConnection $nativeConnection;

    /**
     * Server settings this connection answers from, read once and never again.
     *
     * @var array{search_path: string, server_version: string, standard_conforming_strings: string}|null
     */
    private ?array $settings = null;

    /**
     * Rows per `FETCH` when the connection config does not say. Large enough
     * that the round trips disappear against the work of a job, small enough
     * that one batch is not the memory problem the cursor exists to avoid.
     */
    private const CURSOR_BATCH = 1000;

    public function __construct(string $pool, string $database = '', string $prefix = '', array $config = [])
    {
        if (!function_exists('claviron_postgres_connection')
            || !function_exists('claviron_runtime_available')
            || !claviron_runtime_available()) {
            throw new RuntimeException('The Claviron PostgreSQL runtime is unavailable.');
        }

        $this->nativeConnection = claviron_postgres_connection($pool);
        parent::__construct(
            static fn (): never => throw new LogicException('Claviron PostgreSQL has no PDO handle.'),
            $database,
            $prefix,
            $config,
        );
    }

    /**
     * This connection is the request hot path, not a PDO shim. Schema work
     * belongs on the plain PDO connection that `php artisan` uses, so nothing
     * below is on the serving path — every one of these answers is resolved on
     * first ask and cached for the life of the connection, and the whole set
     * costs a single pooled round trip when something finally asks.
     *
     * `search_path` is the one that matters beyond convenience. Laravel reads
     * it from the connection config, and ours is not there: it lives in the
     * runtime pool's URL, so the runtime stays the single owner of connection
     * options. Left unanswered, `PostgresBuilder::getCurrentSchemaListing()`
     * falls back to `public` — which is how `migrate:fresh` on this connection
     * would aim `dropAllTables()` at the wrong schema.
     *
     * Only the `search_path` key is resolved here, and `getConfig()` is the
     * hottest accessor on this class: `getName()` goes through it on every
     * model hydration.
     */
    public function getConfig($option = null)
    {
        if (($option === 'search_path' || $option === null)
            && !array_key_exists('search_path', $this->config)) {
            $this->config['search_path'] = $this->settings()['search_path'];
        }

        return parent::getConfig($option);
    }

    /**
     * PDO answers this from `PDO::ATTR_SERVER_VERSION`, and
     * `PostgresGrammar::compileColumns()` asks for it on every column
     * introspection. The setting carries a build suffix ("18.6 (Debian …)")
     * that PDO does not report, so only the leading version is kept.
     */
    public function getServerVersion(): string
    {
        $reported = $this->settings()['server_version'];

        return preg_match('/^\d+(?:\.\d+)*/', $reported, $matches) === 1
            ? $matches[0]
            : $reported;
    }

    /**
     * Single result set, because one native execute returns exactly one.
     */
    public function selectResultSets($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        return [$this->select($query, $bindings, $useReadPdo, $fetchUsing)];
    }

    /**
     * `PDO::quote()` for PostgreSQL without the handle: double the single
     * quotes, and fall back to an `E''` literal when the server still treats a
     * backslash as an escape.
     *
     * Reached through `Connection::escape()`, which callers like
     * `Builder::toRawSql()` use; the caller has already rejected non-UTF-8
     * input and handled null, bool, int and binary before this point.
     */
    protected function escapeString($value)
    {
        $escaped = str_replace("'", "''", $value);

        if ($this->settings()['standard_conforming_strings'] === 'on') {
            return "'".$escaped."'";
        }

        return "E'".str_replace('\\', '\\\\', $escaped)."'";
    }

    /**
     * One round trip for every setting this class can be asked about, rather
     * than one per question, and never repeated.
     *
     * Deliberately not `selectOne()`: `getConfig()` can be reached before this
     * connection's `run()` pipeline has its grammar, post processor and event
     * dispatcher, and a settings read has no business in the query log.
     *
     * @return array{search_path: string, server_version: string, standard_conforming_strings: string}
     */
    private function settings(): array
    {
        if ($this->settings !== null) {
            return $this->settings;
        }

        $result = $this->executeNative(
            "select current_setting('search_path') as search_path, "
            ."current_setting('server_version') as server_version, "
            ."current_setting('standard_conforming_strings') as standard_conforming_strings",
            [],
        );

        $row = $result['rows'][0] ?? [];

        return $this->settings = [
            'search_path' => (string) ($row[0] ?? 'public'),
            'server_version' => (string) ($row[1] ?? ''),
            'standard_conforming_strings' => (string) ($row[2] ?? 'on'),
        ];
    }

    public function select($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        if ($fetchUsing !== [] && $fetchUsing !== [PDO::FETCH_OBJ]) {
            throw new LogicException('Custom PDO fetch modes are unavailable on Claviron PostgreSQL.');
        }

        return $this->run($query, $bindings, function ($query, $bindings): array {
            if ($this->pretending()) {
                return [];
            }

            return self::hydrate($this->executeNative($query, $bindings));
        });
    }

    /**
     * Streams the result set instead of materialising it.
     *
     * The rows stay on PostgreSQL behind a server-side cursor and arrive a
     * batch at a time, so a job walking a large table holds one batch rather
     * than the table. This used to loop over `select()`, which meant `cursor()`
     * and `get()` had exactly the same memory cost and only looked different.
     *
     * Lazy by construction: because this method yields, nothing runs — not even
     * the `DECLARE` — until the first row is asked for, and the cursor is closed
     * on the way out however the iteration ends.
     */
    public function cursor($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        if ($fetchUsing !== [] && $fetchUsing !== [PDO::FETCH_OBJ]) {
            throw new LogicException('Custom PDO fetch modes are unavailable on Claviron PostgreSQL.');
        }

        $cursor = $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return null;
            }

            try {
                return claviron_postgres_cursor(
                    $this->nativeConnection,
                    self::numberPlaceholders($query),
                    array_map(self::encodeBinding(...), $bindings),
                    (int) ($this->getConfig('cursor_batch') ?? self::CURSOR_BATCH),
                );
            } catch (Throwable $failure) {
                throw QueryException::from($failure);
            }
        });

        if ($cursor === null) {
            return;
        }

        try {
            while (true) {
                $batch = claviron_postgres_cursor_fetch($cursor);

                if ($batch['rows'] === []) {
                    return;
                }

                // Yielded one at a time rather than `yield from`, so the keys
                // keep counting across batches instead of restarting at each.
                foreach (self::hydrate($batch) as $row) {
                    yield $row;
                }
            }
        } finally {
            claviron_postgres_cursor_close($cursor);
        }
    }

    /**
     * Turns one native result into the `stdClass` rows Eloquent expects.
     *
     * Column order is preserved and names are not collapsed, which is why the
     * native result keeps columns and values apart in the first place.
     *
     * @param  array{columns: list<array{name: string}>, rows: list<list<mixed>>}  $result
     * @return list<\stdClass>
     */
    private static function hydrate(array $result): array
    {
        $names = array_column($result['columns'], 'name');

        return array_map(static function (array $values) use ($names): object {
            $row = new \stdClass();
            foreach ($names as $index => $name) {
                $row->{$name} = $values[$index] ?? null;
            }

            return $row;
        }, $result['rows']);
    }

    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings): bool {
            if ($this->pretending()) {
                return true;
            }

            $this->executeNative($query, $bindings);
            $this->recordsHaveBeenModified();

            return true;
        });
    }

    public function affectingStatement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings): int {
            if ($this->pretending()) {
                return 0;
            }

            $result = $this->executeNative($query, $bindings);
            $affected = (int) $result['affected_rows'];
            $this->recordsHaveBeenModified($affected > 0);

            return $affected;
        });
    }

    /**
     * `PDO::exec()`: the simple query protocol, so one string carrying several
     * statements — a migration's function body and the trigger that uses it —
     * runs here exactly as it runs over PDO. Everything else on this class is
     * a prepared statement, and PostgreSQL prepares one command at a time.
     */
    public function unprepared($query)
    {
        return $this->run($query, [], function ($query): bool {
            if ($this->pretending()) {
                return true;
            }

            try {
                claviron_postgres_unprepared($this->nativeConnection, $query);
            } catch (Throwable $failure) {
                throw QueryException::from($failure);
            }

            $this->recordsHaveBeenModified();

            return true;
        });
    }

    public function beginTransaction()
    {
        foreach ($this->beforeStartingTransaction as $callback) {
            $callback($this);
        }

        if ($this->transactionLevel() === 0) {
            claviron_postgres_begin($this->nativeConnection);
        } else {
            $this->executeNative('SAVEPOINT trans'.($this->transactions + 1), []);
        }

        $this->transactions++;
        $this->transactionsManager?->begin($this->getName(), $this->transactions);
        $this->fireConnectionEvent('beganTransaction');
    }

    /**
     * Laravel's own transaction loop cannot be inherited: it commits through
     * `getPdo()->commit()`, and this connection has no handle to give it. The
     * shape is reproduced instead, and every decision about a failure is
     * delegated to `handleTransactionException()` — so a retry happens on a
     * deadlock or a serialization failure and on nothing else, which is what
     * makes the SQLSTATE worth carrying.
     *
     * Retrying on any `Throwable`, as this did before, re-ran the closure and
     * its side effects for ordinary business errors.
     */
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

        throw new LogicException('Claviron PostgreSQL transaction attempts exhausted.');
    }

    /**
     * Laravel's own version reaches for `getPdo()->inTransaction()` before
     * rolling back a failed commit. The decisions are identical; only the
     * question of whether a transaction is still open is asked of the runtime
     * instead of a PDO handle.
     */
    protected function handleCommitTransactionException(Throwable $e, $currentAttempt, $maxAttempts)
    {
        $this->transactions = max(0, $this->transactions - 1);

        if ($this->causedByConcurrencyError($e) && $currentAttempt < $maxAttempts) {
            if (claviron_postgres_in_transaction($this->nativeConnection)) {
                claviron_postgres_rollback($this->nativeConnection);
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
            claviron_postgres_commit($this->nativeConnection);
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

        if ($toLevel === 0) {
            claviron_postgres_rollback($this->nativeConnection);
        } else {
            $this->executeNative('ROLLBACK TO SAVEPOINT trans'.($toLevel + 1), []);
        }

        $this->transactions = $toLevel;
        $this->transactionsManager?->rollback($this->getName(), $this->transactions);
        $this->fireConnectionEvent('rollingBack');
    }

    /**
     * The runtime ends any transaction the request did not, so a level left on
     * this object by an earlier request is stale bookkeeping rather than an
     * open transaction. Asking the connection is how the two views are
     * reconciled — and it is why one abandoned transaction can no longer take
     * the rest of the worker's requests down with it.
     */
    public function transactionLevel()
    {
        if ($this->transactions > 0 && !claviron_postgres_in_transaction($this->nativeConnection)) {
            $this->transactions = 0;
        }

        return $this->transactions;
    }

    public function getPdo()
    {
        throw new LogicException('Claviron PostgreSQL intentionally exposes no PDO handle.');
    }

    public function disconnect()
    {
        claviron_postgres_disconnect($this->nativeConnection);
        $this->transactions = 0;
        $this->transactionsManager?->rollback($this->getName(), 0);
        // Preserve the no-PDO sentinel. Parent::disconnect() sets it to null;
        // the next query then reconnects PDO and resets the nesting count even
        // when a payment handler has already begun a new native transaction.
        // The native handle names a pool and lazily obtains its next lease.
    }

    /**
     * One path, because the runtime leases one connection to the request: a
     * statement inside a transaction and one outside it go to the same place.
     */
    private function executeNative(string $query, array $bindings): array
    {
        try {
            return claviron_postgres_execute(
                $this->nativeConnection,
                self::numberPlaceholders($query),
                array_map(self::encodeBinding(...), $bindings),
            );
        } catch (Throwable $failure) {
            throw QueryException::from($failure);
        }
    }

    private static function encodeBinding(mixed $value): mixed
    {
        if ($value instanceof DateTimeInterface) {
            return ['type' => 'string', 'value' => $value->format('Y-m-d\TH:i:s.uP')];
        }
        if (is_bool($value)) {
            return ['type' => 'boolean', 'value' => $value];
        }
        if (is_int($value)) {
            return ['type' => 'i64', 'value' => $value];
        }
        if (is_float($value)) {
            return ['type' => 'f64', 'value' => $value];
        }
        if (is_array($value) && !isset($value['type'])) {
            return ['type' => 'json', 'value' => $value];
        }

        return $value;
    }

    /**
     * Rewrites Laravel's `?` placeholders as PostgreSQL's `$1`, `$2`, …
     *
     * The scan exists because `?` is not only a placeholder in PostgreSQL: it
     * is also the JSON key-existence operator (`?`, `?|`, `?&`), and it can sit
     * inside a string, an identifier, a comment or a dollar-quoted body, where
     * numbering it corrupts the statement instead of parameterising it.
     */
    private static function numberPlaceholders(string $sql): string
    {
        $output = '';
        $number = 0;
        $length = strlen($sql);

        for ($index = 0; $index < $length; $index++) {
            $char = $sql[$index];

            if ($char === "'" || $char === '"') {
                $end = self::scanQuoted($sql, $index, $char);
            } elseif ($char === '-' && ($sql[$index + 1] ?? null) === '-') {
                $newline = strpos($sql, "\n", $index);
                $end = $newline === false ? $length : $newline + 1;
            } elseif ($char === '/' && ($sql[$index + 1] ?? null) === '*') {
                $end = self::scanBlockComment($sql, $index);
            } elseif ($char === '$' && ($tagEnd = self::dollarTagEnd($sql, $index)) !== null) {
                $end = self::scanDollarQuoted($sql, $index, $tagEnd);
            } elseif ($char === '?') {
                // PDO unescapes a doubled question mark to a literal one, which
                // is how PostgreSQL's JSON operators survive a driver that
                // spells placeholders `?`. Laravel doubles them on the way in
                // (PostgresGrammar::whereBasic), so undoubling here is not an
                // extension — it is the other half of that contract.
                if (($sql[$index + 1] ?? null) === '?') {
                    $output .= '?';
                    $index++;

                    continue;
                }

                $output .= '$'.(++$number);

                continue;
            } else {
                $output .= $char;

                continue;
            }

            $output .= substr($sql, $index, $end - $index);
            $index = $end - 1;
        }

        return $output;
    }

    /**
     * Index just past the closing quote of a string or quoted identifier.
     *
     * A doubled quote escapes itself. A backslash escapes the next character
     * only inside an `E''` string, where PostgreSQL still reads C-style
     * escapes whatever `standard_conforming_strings` says.
     */
    private static function scanQuoted(string $sql, int $start, string $quote): int
    {
        $length = strlen($sql);
        $escapes = $quote === "'" && self::precededByEscapeMarker($sql, $start);

        for ($index = $start + 1; $index < $length; $index++) {
            $char = $sql[$index];

            if ($escapes && $char === '\\') {
                $index++;

                continue;
            }

            if ($char !== $quote) {
                continue;
            }

            if (($sql[$index + 1] ?? null) === $quote) {
                $index++;

                continue;
            }

            return $index + 1;
        }

        return $length;
    }

    /**
     * Whether the quote at $index opens an `E''` escape string rather than an
     * ordinary one. The `E` has to be a token of its own, not the tail of an
     * identifier such as `some_value'…`.
     */
    private static function precededByEscapeMarker(string $sql, int $index): bool
    {
        $previous = $index > 0 ? $sql[$index - 1] : null;

        if ($previous !== 'E' && $previous !== 'e') {
            return false;
        }

        $before = $sql[$index - 2] ?? ' ';

        return !ctype_alnum($before) && $before !== '_';
    }

    /**
     * Index just past the end of a block comment. PostgreSQL nests them, so
     * the first `*&#47;` does not necessarily close the one that opened here.
     */
    private static function scanBlockComment(string $sql, int $start): int
    {
        $length = strlen($sql);
        $depth = 0;

        for ($index = $start; $index < $length; $index++) {
            $pair = substr($sql, $index, 2);

            if ($pair === '/*') {
                $depth++;
                $index++;

                continue;
            }

            if ($pair === '*/') {
                $index++;

                if (--$depth === 0) {
                    return $index + 1;
                }
            }
        }

        return $length;
    }

    /**
     * Index just past a dollar-quote opener (`$$` or `$tag$`), or null when
     * this `$` is not one — `$1` is a positional parameter, and a tag may not
     * start with a digit.
     */
    private static function dollarTagEnd(string $sql, int $start): ?int
    {
        $length = strlen($sql);
        $index = $start + 1;

        while ($index < $length && (ctype_alnum($sql[$index]) || $sql[$index] === '_')) {
            if ($index === $start + 1 && ctype_digit($sql[$index])) {
                return null;
            }

            $index++;
        }

        return ($sql[$index] ?? null) === '$' ? $index + 1 : null;
    }

    /**
     * Index just past the matching close of a dollar-quoted body. The body is
     * opaque: only the exact opening tag ends it.
     */
    private static function scanDollarQuoted(string $sql, int $start, int $tagEnd): int
    {
        $tag = substr($sql, $start, $tagEnd - $start);
        $close = strpos($sql, $tag, $tagEnd);

        return $close === false ? strlen($sql) : $close + strlen($tag);
    }
}
