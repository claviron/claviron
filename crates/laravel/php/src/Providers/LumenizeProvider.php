<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Illuminate\Support\ServiceProvider;

/** Console configuration only; does not start a runtime or attach to a fleet. */
final class LumenizeProvider extends ServiceProvider
{
    public function register(): void
    {
        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->mergeConfigFrom($this->configurationPath(), 'lumenize');
    }

    public function boot(): void
    {
        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->publishes([
            $this->configurationPath() => $this->app->configPath('lumenize.php'),
        ], 'lumenize-config');
    }

    private function configurationPath(): string
    {
        return dirname(__DIR__, 2).'/config/lumenize.php';
    }
}
