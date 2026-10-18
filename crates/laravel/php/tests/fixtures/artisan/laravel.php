<?php

declare(strict_types=1);

// Native runtime presence without a configured event bus must not subscribe.
return \Illuminate\Foundation\Application::configure(basePath: dirname(__DIR__))
    ->withProviders([
        \Claviron\Laravel\Providers\LumenizeProvider::class,
        \Claviron\Laravel\Providers\ScheduleServiceProvider::class,
        \Claviron\Laravel\Providers\BroadcastingProvider::class,
    ], withBootstrapProviders: false)
    ->withExceptions(static function (\Illuminate\Foundation\Configuration\Exceptions $exceptions): void {})
    ->create();
