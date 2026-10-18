<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Cache\DetachedStore;
use Claviron\Laravel\Cache\Store;
use Illuminate\Cache\CacheManager;
use Illuminate\Cache\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;

/**
 * Registers the `claviron` cache driver:
 *
 *     'claviron' => ['driver' => 'claviron', 'runtime_store' => 'fleet'],
 *
 * `runtime_store` names a store in the runtime's `cache.yaml`; leaving it out
 * uses the runtime's default store.
 */
final class CacheServiceProvider extends ServiceProvider
{
    public function boot(CacheManager $cache): void
    {
        // CacheManager binds custom creators to itself. Static closures cannot
        // be rebound (PHP 8.5 warns and PHP 9 rejects it).
        $cache->extend('claviron', function (Application $app, array $config) use ($cache): Repository {
            $runtimeStore = isset($config['runtime_store']) ? (string) $config['runtime_store'] : null;
            $prefix = (string) ($config['prefix'] ?? $app['config']['cache.prefix'] ?? '');

            if (function_exists('claviron_cache_store')
                && function_exists('claviron_runtime_configured')
                && claviron_runtime_configured()) {
                return $cache->repository(new Store($runtimeStore, $prefix), $config);
            }

            $fallback = $config['fallback'] ?? null;
            if (is_string($fallback) && $fallback !== '') {
                if ($fallback === ($config['store'] ?? null)) {
                    throw new InvalidArgumentException(
                        "Claviron cache store [{$fallback}] cannot fall back to itself.",
                    );
                }

                return $cache->store($fallback);
            }

            // A shell or a test process. Said once per process, because the
            // manager keeps the store it built.
            $app->make(LoggerInterface::class)->warning(sprintf(
                'Claviron cache store [%s] is running without the runtime: reads miss and writes are dropped.',
                $runtimeStore ?? 'default',
            ));

            return $cache->repository(new DetachedStore($prefix), $config);
        });
    }
}
