<?php

declare(strict_types=1);

namespace Claviron\Runtime;

/**
 * Immutable event delivered by the Claviron embed runtime.
 *
 * @property-read null|bool|int|float|string|array $payload
 */
if (!class_exists(Event::class, false)) {
    final readonly class Event
    {
        private function __construct(
            public string $id,
            public string $topic,
            public mixed $payload,
        ) {}
    }
}

/**
 * Immutable invocation delivered by orbit_invoke_receive().
 *
 * @property-read null|bool|int|float|string|array $payload
 */
if (!class_exists(Invocation::class, false)) {
    final readonly class Invocation
    {
        private function __construct(
            public string $id,
            public string $operation,
            public mixed $payload,
            public int $submitted_at,
        ) {}
    }
}

/**
 * A typed value that lives in the fleet's shared memory and travels by id.
 *
 * `$n = Orbital::int(0); $n++;` updates the same cell from every process
 * that holds a handle; `serialize($n)` carries only the id, and the handle
 * that comes back reads whatever the cell holds then. Arithmetic on an int
 * or float cell and `.=` on a string cell are atomic and return the same
 * handle; comparisons and casts read the current value.
 */
if (!class_exists(Orbital::class, false)) {
    final class Orbital
    {
        private function __construct() {}

        public static function int(int $value = 0): self { throw new \RuntimeException('requires the Claviron runtime'); }
        public static function float(float $value = 0.0): self { throw new \RuntimeException('requires the Claviron runtime'); }
        public static function bool(bool $value = false): self { throw new \RuntimeException('requires the Claviron runtime'); }
        public static function string(string $value = ''): self { throw new \RuntimeException('requires the Claviron runtime'); }
        /** Reattach to a cell by the id another handle printed or serialized. */
        public static function open(string $id): self { throw new \RuntimeException('requires the Claviron runtime'); }

        public function id(): string { throw new \RuntimeException('requires the Claviron runtime'); }
        public function get(): int|float|bool|string { throw new \RuntimeException('requires the Claviron runtime'); }
        public function set(int|float|bool|string $value): void { throw new \RuntimeException('requires the Claviron runtime'); }
        /** Atomic add for int and float cells; returns the value after. */
        public function add(int|float $by): int|float { throw new \RuntimeException('requires the Claviron runtime'); }
        /** Atomic append for string cells; refused whole when it would not fit. */
        public function append(string $text): string { throw new \RuntimeException('requires the Claviron runtime'); }
        /** Give the cell back; every handle to it goes stale. */
        /** The write count; take it before get() and wait() misses nothing. */
        public function version(): int { throw new \RuntimeException('requires the Claviron runtime'); }
        /** Block until written since $since, by any process; the count now. Throws once released. */
        public function wait(int $since): int { throw new \RuntimeException('requires the Claviron runtime'); }
        public function release(): void { throw new \RuntimeException('requires the Claviron runtime'); }
    }
}

if (!class_exists(CacheHandle::class, false)) {
    /** Opaque native cache handle; only claviron_cache_store() creates one. */
    final class CacheHandle
    {
        private function __construct() {}
        private function __clone() {}

        public function __serialize(): array
        {
            throw new \RuntimeException('CacheHandle requires the Claviron embed runtime and native cache support.');
        }

        public function __unserialize(array $data): void
        {
            throw new \RuntimeException('CacheHandle requires the Claviron embed runtime and native cache support.');
        }
    }
}

if (!class_exists(PostgresConnection::class, false)) {
    /** Opaque named-pool handle; only claviron_postgres_connection() creates one. */
    final class PostgresConnection
    {
        private function __construct() {}
        private function __clone() {}
    }
}

if (!class_exists(PostgresCursor::class, false)) {
    /** Opaque server-side cursor on the request's connection; only claviron_postgres_cursor() creates one. */
    final class PostgresCursor
    {
        private function __construct() {}
        private function __clone() {}
    }
}

if (!class_exists(SqliteConnection::class, false)) {
    /** Opaque named-pool handle; only claviron_sqlite_connection() creates one. */
    final class SqliteConnection
    {
        private function __construct() {}
        private function __clone() {}
    }
}

if (!class_exists(MySqlConnection::class, false)) {
    /** Opaque named-pool handle; only claviron_mysql_connection() creates one. */
    final class MySqlConnection
    {
        private function __construct() {}
        private function __clone() {}
    }
}
