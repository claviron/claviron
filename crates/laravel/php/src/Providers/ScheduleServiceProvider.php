<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Console\ErrandRunCommand;
use Claviron\Laravel\Console\InvokeServeCommand;
use Claviron\Laravel\Console\ScheduleCommand;
use Claviron\Laravel\Console\ScheduleRunCommand;
use Claviron\Laravel\EventBridge;
use Claviron\Laravel\Invocation\Invoke;
use Claviron\Laravel\Invocation\Registry;
use Claviron\Laravel\Invocation\Runner;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\ServiceProvider;

/**
 * Registers `claviron:schedule`, the in-process scheduler daemon.
 *
 * Registered unconditionally, unlike the queue adapter: this adds a command
 * rather than replacing a binding, so it changes nothing until a task asks for
 * it. Under a real CLI it is simply an extra command nobody runs.
 */
final class ScheduleServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        // One bridge per container. Registered here rather than in a provider of
        // its own because package discovery reads `installed.json`, which only
        // changes on install -- a new provider would be invisible until then.
        $this->app->singleton(EventBridge::class, fn ($app) => new EventBridge($app->make(Dispatcher::class)));
        $this->app->singleton(Invoke::class, fn ($app) => new Invoke($app->make(EventBridge::class), $app->make(Dispatcher::class)));
        // Each application container owns its selected declaration catalog.
        // Building it never subscribes HTTP, queue or scheduler executions.
        $this->app->singleton(Registry::class, fn ($app) => Registry::fromConfig(
            $app,
            $app['config']->get('claviron.invocations', []),
        ));
        $this->app->singleton(Runner::class, fn ($app) => new Runner($app, $app->make(Registry::class)));

        if ($this->app->runningInConsole()) {
            // Ours is registered after the framework's, so the name resolves
            // here. `schedule:run` keeps working by hand and under cron; it
            // only gains an exit the daemon can use.
            $this->commands([ErrandRunCommand::class, InvokeServeCommand::class, ScheduleCommand::class, ScheduleRunCommand::class]);
        }
    }

    /**
     * @throws BindingResolutionException
     */
    public function boot(): void
    {
        // Outbound: any `FleetEvent` dispatched in this process reaches the
        // bus. Wired in boot, where listeners belong; cheap when nothing
        // implements it -- one instanceof per dispatched event.
        if (EventBridge::available()) {
            $this->app->make(EventBridge::class)->listenForOutbound();
        }
    }
}
