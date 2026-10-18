<?php

declare(strict_types=1);

namespace Claviron\Laravel\Cache;

use Illuminate\Contracts\Cache\Store as StoreContract;
use RuntimeException;

/**
 * The `claviron` cache store where the Claviron runtime is absent: a shell,
 * a test process.
 *
 * Reads miss, writes report `false`, forgetting and flushing nothing succeed.
 * Counters throw, because there is nothing honest to answer them with. This
 * class is deliberately not a `LockProvider`: Laravel's scheduler and queue
 * middleware ask the store for locks only when it offers them, and fall back
 * to `add()` otherwise, which here fails closed.
 */
final class DetachedStore implements StoreContract
{
    public function __construct(private readonly string $prefix) {}

    public function get($key)
    {
        return null;
    }

    public function many(array $keys)
    {
        return array_fill_keys($keys, null);
    }

    public function put($key, $value, $seconds)
    {
        return false;
    }

    public function putMany(array $values, $seconds)
    {
        return false;
    }

    public function forever($key, $value)
    {
        return false;
    }

    public function touch($key, $seconds)
    {
        return false;
    }

    public function forget($key)
    {
        return true;
    }

    public function flush()
    {
        return true;
    }

    public function increment($key, $value = 1)
    {
        throw new RuntimeException('Claviron cache counters require the Claviron runtime.');
    }

    public function decrement($key, $value = 1)
    {
        throw new RuntimeException('Claviron cache counters require the Claviron runtime.');
    }

    public function getPrefix()
    {
        return $this->prefix;
    }
}
