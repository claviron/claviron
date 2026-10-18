<?php

declare(strict_types=1);


/**
 * Claviron embed API declarations with fail-fast fallbacks, not a polyfill.
 * Safe to require in ordinary PHP and in Claviron: native definitions win.
 * Runtime presence does not imply service readiness or optional feature support.
 * After loading this file, function_exists() also sees fallbacks; use
 * claviron_runtime_available() to distinguish ordinary PHP from Claviron embed.
 * Downstream compiled-in extensions own their own stubs.
 */

if (!function_exists('claviron_runtime_available')) {
    /**
     * @return bool
     */
    function claviron_runtime_available(): bool
    {
        return false;
    }
}

if (!function_exists('claviron_runtime_configured')) {
    /** Whether the host runtime has prepared its native service bindings. */
    function claviron_runtime_configured(): bool
    {
        return false;
    }
}

if (!function_exists('claviron_app_name')) {
    /**
     * Name of the embedding Claviron application, not a named PHP program.
     * @return string
     */
    function claviron_app_name(): string
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_environment')) {
    /**
     * Environment the Claviron process runs under: `production`, `local`, or
     * the one its launch named. Not Laravel's APP_ENV.
     * @return string
     */
    function claviron_environment(): string
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_config')) {
    /**
     * Read an exported namespace.key from Claviron's validated configuration.
     * Only missing keys return $default; an explicitly configured null stays null.
     */
    function claviron_config(string $key, mixed $default = null): mixed
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_application_name')) {
    /** Named language application currently executing, if any. */
    function claviron_application_name(): ?string
    {
        return null;
    }
}

if (!function_exists('claviron_auth')) {
    /**
     * Current verified HTTP identity; expires_at is credential expiry in Unix seconds.
     * Internal attributes may contain password hashes; never return this array as an API response.
     * @return array{subject: string, session_id: ?string, expires_at: int, attributes: array<string, scalar|null>}|null
     */
    function claviron_auth(): ?array
    {
        throw new \RuntimeException('claviron_auth() requires native Claviron auth + php_embed support.');
    }
}





if (!function_exists('claviron_is_cancelled')) {
    /**
     * @return bool
     */
    function claviron_is_cancelled(): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_is_on_http')) {
    /**
     * Whether this execution is serving an HTTP request right now: true in a
     * request and while an application handles a received one, false for a
     * task and for an application between messages.
     */
    function claviron_is_on_http(): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_sapi_stats')) {
    function claviron_sapi_stats(): ?array
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_application_receive')) {
    /** @return array|\Claviron\Runtime\Event|null */
    function claviron_application_receive(): array|\Claviron\Runtime\Event|null
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_application_respond')) {
    function claviron_application_respond(array $response): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_application_stream')) {
    /** Sends a reply's head (status, headers); the body follows through claviron_application_write(). */
    function claviron_application_stream(array $head): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_application_write')) {
    /** One piece of a streamed reply; false when the client is gone and the handler should stop. */
    function claviron_application_write(string $chunk): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_application_end')) {
    /** Ends a streamed reply, and with it the request. */
    function claviron_application_end(): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_budget_begin')) {
    /** Bounds the work until claviron_budget_end() with the engine's own budget; PHP raises the fatal. */
    function claviron_budget_begin(int $seconds): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_budget_end')) {
    function claviron_budget_end(): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_events_available')) {
    /** Whether the host has prepared its event bus, not just loaded the engine. */
    function claviron_events_available(): bool
    {
        return false;
    }
}

if (!function_exists('claviron_event_subscribe')) {
    function claviron_event_subscribe(string $topic): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_event_subscribe_many')) {
    /** @param list<string> $topics */
    function claviron_event_subscribe_many(array $topics): int
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_event_unsubscribe')) {
    function claviron_event_unsubscribe(string $topic): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_event_publish')) {
    /** @param null|bool|int|float|string|array $payload */
    function claviron_event_publish(string $topic, mixed $payload): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_event_publish_shared')) {
    /** Publish an explicitly registered cross-runtime event. */
    function claviron_event_publish_shared(string $topic, mixed $payload): void
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and Orbit events.');
    }
}

if (!function_exists('claviron_event_receive')) {
    function claviron_event_receive(int $timeout_ms = 0): ?\Claviron\Runtime\Event
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_task_output_write')) {
    function claviron_task_output_write(string $stream, string $data): int
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_task_output_flush')) {
    function claviron_task_output_flush(string $stream): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_task_receive')) {
    /** Wait for either subscribed input; null means task cancellation, not idle. */
    function claviron_task_receive(): \Claviron\Runtime\Event|\Claviron\Runtime\Invocation|null
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and a PHP runtime Task.');
    }
}

if (!function_exists('orbit_invoke_subscribe')) {
    function orbit_invoke_subscribe(string $operation): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_invoke_unsubscribe')) {
    function orbit_invoke_unsubscribe(string $operation): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_invoke_receive')) {
    function orbit_invoke_receive(int $timeout_ms = 0): ?\Claviron\Runtime\Invocation
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_invoke')) {
    /** @param null|bool|int|float|string|array $payload */
    function orbit_invoke(string $operation, mixed $payload): string
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_cache_store')) {
    function claviron_cache_store(?string $name = null): \Claviron\Runtime\CacheHandle
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_cache_get')) {
    function claviron_cache_get(\Claviron\Runtime\CacheHandle $cache, string $key): mixed
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_cache_put')) {
    function claviron_cache_put(\Claviron\Runtime\CacheHandle $cache, string $key, mixed $value, ?int $seconds = null): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_cache_forget')) {
    function claviron_cache_forget(\Claviron\Runtime\CacheHandle $cache, string $key): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_cache_clear')) {
    function claviron_cache_clear(\Claviron\Runtime\CacheHandle $cache): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}


if (!function_exists('claviron_mysql_connection')) {
    function claviron_mysql_connection(?string $name = null): \Claviron\Runtime\MySqlConnection
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_execute')) {
    /**
     * Parameters may be PHP scalars or typed arrays such as
     * ['type' => 'i32', 'value' => 42], ['type' => 'bytes', 'value' => $bytes],
     * and ['type' => 'json', 'value' => $value].
     *
     * @return array{columns: list<array{name: string, type: string}>, rows: list<list<mixed>>, affected_rows: int|string, insert_id: string}
     */
    function claviron_mysql_execute(
        \Claviron\Runtime\MySqlConnection $connection,
        string $sql,
        array $parameters = [],
    ): array {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_unprepared')) {
    /** PDO::exec(): raw SQL, several statements allowed, returns the rows affected. */
    function claviron_mysql_unprepared(
        \Claviron\Runtime\MySqlConnection $connection,
        string $sql,
    ): int {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_begin')) {
    /** PDO::beginTransaction(): the transaction is state on the connection, not a handle. */
    function claviron_mysql_begin(
        \Claviron\Runtime\MySqlConnection $connection,
        ?string $isolation_level = null,
        ?bool $read_only = null,
    ): bool {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_commit')) {
    function claviron_mysql_commit(\Claviron\Runtime\MySqlConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_disconnect')) {
    function claviron_mysql_disconnect(\Claviron\Runtime\MySqlConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_rollback')) {
    function claviron_mysql_rollback(\Claviron\Runtime\MySqlConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_mysql_in_transaction')) {
    /** Whether the request's connection has a transaction open, as the connection itself knows it. */
    function claviron_mysql_in_transaction(\Claviron\Runtime\MySqlConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native MySQL support.');
    }
}

if (!function_exists('claviron_sqlite_connection')) {
    function claviron_sqlite_connection(?string $name = null): \Claviron\Runtime\SqliteConnection
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_execute')) {
    /**
     * Parameters may be PHP scalars or typed arrays such as
     * ['type' => 'i32', 'value' => 42], ['type' => 'bytes', 'value' => $bytes],
     * and ['type' => 'json', 'value' => $value].
     *
     * @return array{columns: list<array{name: string, type: string}>, rows: list<list<mixed>>, affected_rows: int|string, insert_id: string}
     */
    function claviron_sqlite_execute(
        \Claviron\Runtime\SqliteConnection $connection,
        string $sql,
        array $parameters = [],
    ): array {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_unprepared')) {
    /** PDO::exec(): raw SQL, several statements allowed, returns the rows affected. */
    function claviron_sqlite_unprepared(
        \Claviron\Runtime\SqliteConnection $connection,
        string $sql,
    ): int {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_begin')) {
    /** PDO::beginTransaction(): the transaction is state on the connection, not a handle. */
    function claviron_sqlite_begin(
        \Claviron\Runtime\SqliteConnection $connection,
        string $mode = 'DEFERRED',
    ): bool {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_commit')) {
    function claviron_sqlite_commit(\Claviron\Runtime\SqliteConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_disconnect')) {
    function claviron_sqlite_disconnect(\Claviron\Runtime\SqliteConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_rollback')) {
    function claviron_sqlite_rollback(\Claviron\Runtime\SqliteConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_sqlite_in_transaction')) {
    /** Whether the request's connection has a transaction open, as the connection itself knows it. */
    function claviron_sqlite_in_transaction(\Claviron\Runtime\SqliteConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native SQLite support.');
    }
}

if (!function_exists('claviron_postgres_connection')) {
    function claviron_postgres_connection(?string $name = null): \Claviron\Runtime\PostgresConnection
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_execute')) {
    /**
     * Parameters may be PHP scalars or typed arrays such as
     * ['type' => 'i32', 'value' => 42], ['type' => 'bytes', 'value' => $bytes],
     * and ['type' => 'json', 'value' => $value].
     *
     * @return array{columns: list<array{name: string, type: string}>, rows: list<list<mixed>>, affected_rows: int|string}
     */
    function claviron_postgres_execute(
        \Claviron\Runtime\PostgresConnection $connection,
        string $sql,
        array $parameters = [],
    ): array {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_unprepared')) {
    /** PDO::exec(): raw SQL, several statements allowed, returns the rows affected. */
    function claviron_postgres_unprepared(
        \Claviron\Runtime\PostgresConnection $connection,
        string $sql,
    ): int {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_begin')) {
    /** PDO::beginTransaction(): the transaction is state on the connection, not a handle. */
    function claviron_postgres_begin(
        \Claviron\Runtime\PostgresConnection $connection,
        ?string $isolation_level = null,
        ?bool $read_only = null,
    ): bool {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_commit')) {
    function claviron_postgres_commit(\Claviron\Runtime\PostgresConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_rollback')) {
    function claviron_postgres_rollback(\Claviron\Runtime\PostgresConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_in_transaction')) {
    /** Whether the request's connection has a transaction open, as the connection itself knows it. */
    function claviron_postgres_in_transaction(\Claviron\Runtime\PostgresConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_disconnect')) {
    /** Release this thread's native lease, rolling back any unfinished transaction. */
    function claviron_postgres_disconnect(\Claviron\Runtime\PostgresConnection $connection): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_cursor')) {
    /** DECLARE on the request's connection; rows stay on the server and arrive $batch at a time. */
    function claviron_postgres_cursor(
        \Claviron\Runtime\PostgresConnection $connection,
        string $sql,
        array $parameters = [],
        int $batch = 1000,
    ): \Claviron\Runtime\PostgresCursor {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_cursor_fetch')) {
    /** @return array{columns: list<array{name: string, type: string}>, rows: list<list<mixed>>} empty rows once exhausted */
    function claviron_postgres_cursor_fetch(\Claviron\Runtime\PostgresCursor $cursor): array
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('claviron_postgres_cursor_close')) {
    function claviron_postgres_cursor_close(\Claviron\Runtime\PostgresCursor $cursor): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and native PostgreSQL support.');
    }
}

if (!function_exists('orbit_counter_get')) {
    function orbit_counter_get(string $key): ?int
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_counter_inc')) {
    /** @return int|false */
    function orbit_counter_inc(string $key, int $by = 1): mixed
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_counter_dec')) {
    /** @return int|false */
    function orbit_counter_dec(string $key, int $by = 1): mixed
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_counter_reset')) {
    function orbit_counter_reset(string $key): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_lock_acquire')) {
    function orbit_lock_acquire(string $key, string $owner, int $ttl_ms): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_lock_release')) {
    function orbit_lock_release(string $key, string $owner): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_lock_owner')) {
    function orbit_lock_owner(string $key): ?string
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('orbit_lock_force_release')) {
    function orbit_lock_force_release(string $key): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_log_channel')) {
    /**
     * The id of a log channel logging.yaml declares; null when it declares none
     * of that name. $skip lists class prefixes never taken as a record's source.
     *
     * @param list<string> $skip
     */
    function claviron_log_channel(string $name, array $skip = []): ?int
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}

if (!function_exists('claviron_log')) {
    /**
     * Publish one record to a channel: $level is a PSR-3 level name, $context
     * becomes fields, $target defaults to the calling Class::method.
     *
     * @param array<array-key, mixed> $context
     */
    function claviron_log(int $channel, string $level, string $message, array $context = [], ?string $target = null): bool
    {
        throw new \RuntimeException(__FUNCTION__ . '() requires the Claviron embed runtime and its native implementation.');
    }
}
