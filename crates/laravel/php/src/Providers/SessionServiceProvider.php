<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Http\Middleware\StartNativeSession;
use Illuminate\Cache\CacheManager;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Session\CacheBasedSessionHandler;
use Illuminate\Session\SessionManager;
use Illuminate\Support\ServiceProvider;

/**
 * Registers Laravel sessions over a configured Claviron cache store.
 *
 * The cache store owns the runtime details: an Orbit-backed store can provide
 * fleet-local L1 state, Redis L2 persistence, TTLs, and Orbit session locks.
 */
final class SessionServiceProvider extends ServiceProvider
{
    public function boot(CacheManager $cache): void
    {
        /** @var SessionManager $sessions */
        $sessions = $this->app->make('session');

        $sessions->extend('claviron', static function (Application $app) use ($cache): CacheBasedSessionHandler {
            $store = $app['config']['session.store'] ?: null;
            $lifetime = (int) $app['config']['session.lifetime'];

            return new CacheBasedSessionHandler(clone $cache->store($store), $lifetime);
        });

        if ($this->app->bound('router')) {
            $this->app->make('router')->aliasMiddleware('native.session', StartNativeSession::class);
        }
    }
}
