<?php

declare(strict_types=1);

namespace Claviron\Laravel\Cache;

use Illuminate\Contracts\Cache\Lock as LockContract;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Store as StoreContract;
use RuntimeException;

/**
 * Laravel's cache contract over a named Claviron runtime store.
 *
 * Values live in the runtime store named by `runtime_store` in
 * `config/cache.php` — one of the stores in `cache.yaml`. Counters and locks
 * are not cache to the runtime: `increment()`/`decrement()` go to the Orbit
 * counter ring and `lock()` to the Orbit lock ring, each keyed with this
 * store's prefix. A value put under a key and a counter incremented under the
 * same key are therefore two different things, and `get()` answers from the
 * cache only.
 *
 * This class exists only inside the runtime; the provider builds a
 * `DetachedStore` anywhere the host functions are absent.
 *
 * `flush()` clears the whole runtime store, this prefix or not, and cannot
 * reach counters: give each application its own store in `cache.yaml`.
 */
final class Store implements StoreContract, LockProvider
{
    private \Claviron\Runtime\CacheHandle $cache;

    /**
     * Laravel's stores `serialize()` every value. This one keeps scalars and
     * arrays native, so a Rust reader of the same store sees numbers, strings
     * and JSON; only objects — a model out of `Cache::remember()` — are
     * serialized, behind a marker `get()` recognises.
     */
    private const OBJECT = "\0claviron:object\0";

    public function __construct(?string $runtimeStore, private readonly string $prefix)
    {
        if (!function_exists('claviron_cache_store')) {
            throw new RuntimeException('The Claviron cache runtime is unavailable.');
        }

        $this->cache = claviron_cache_store($runtimeStore);
    }

    public function get($key)
    {
        return self::decode(claviron_cache_get($this->cache, $this->prefix.$key));
    }

    public function many(array $keys)
    {
        $values = [];

        foreach ($keys as $key) {
            $values[$key] = $this->get($key);
        }

        return $values;
    }

    public function put($key, $value, $seconds)
    {
        return claviron_cache_put($this->cache, $this->prefix.$key, self::encode($value), (int) $seconds);
    }

    public function putMany(array $values, $seconds)
    {
        $stored = true;

        foreach ($values as $key => $value) {
            $stored = $this->put($key, $value, $seconds) && $stored;
        }

        return $stored;
    }

    public function forever($key, $value)
    {
        return claviron_cache_put($this->cache, $this->prefix.$key, self::encode($value), null);
    }

    /**
     * The runtime has no touch of its own: the value is read back and stored
     * again under the new lifetime. A key that is not there is not touched.
     */
    public function touch($key, $seconds)
    {
        $value = $this->get($key);

        if ($value === null) {
            return false;
        }

        return $this->put($key, $value, $seconds);
    }

    public function forget($key)
    {
        return claviron_cache_forget($this->cache, $this->prefix.$key);
    }

    public function flush()
    {
        return claviron_cache_clear($this->cache);
    }

    public function increment($key, $value = 1)
    {
        $this->requireRuntime('orbit_counter_inc', 'counters');

        return $this->counted(orbit_counter_inc($this->prefix.$key, (int) $value), $key);
    }

    public function decrement($key, $value = 1)
    {
        $this->requireRuntime('orbit_counter_dec', 'counters');

        return $this->counted(orbit_counter_dec($this->prefix.$key, (int) $value), $key);
    }

    public function lock($name, $seconds = 0, $owner = null): LockContract
    {
        $this->requireRuntime('orbit_lock_acquire', 'locks');

        return new Lock($this->prefix.$name, (int) $seconds, $owner);
    }

    public function restoreLock($name, $owner): LockContract
    {
        return $this->lock($name, 0, $owner);
    }

    public function getPrefix()
    {
        return $this->prefix;
    }

    private static function encode(mixed $value): mixed
    {
        return is_object($value) ? self::OBJECT.serialize($value) : $value;
    }

    private static function decode(mixed $value): mixed
    {
        if (is_string($value) && str_starts_with($value, self::OBJECT)) {
            return unserialize(substr($value, strlen(self::OBJECT)));
        }

        return $value;
    }

    private function counted(mixed $result, string $key): int
    {
        if (!is_int($result)) {
            throw new RuntimeException("Claviron counter update failed for [{$key}].");
        }

        return $result;
    }

    private function requireRuntime(string $function, string $what): void
    {
        if (!function_exists($function)) {
            throw new RuntimeException("Claviron cache {$what} require the Claviron runtime.");
        }
    }
}
