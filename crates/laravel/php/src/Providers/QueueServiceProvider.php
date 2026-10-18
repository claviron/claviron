<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Console\WorkCommand;
use Claviron\Laravel\Queue\Worker;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Console\WorkCommand as IlluminateWorkCommand;
use Illuminate\Queue\Worker as IlluminateWorker;
use Illuminate\Support\ServiceProvider;

/**
 * Swaps Laravel's queue worker for one that stops without POSIX signals, and
 * only when Claviron is the host. Under a real CLI or FPM the stock worker is
 * correct and this provider stays out of the way, so the same application code
 * runs unchanged in both places.
 */
final class QueueServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        if (!function_exists('claviron_is_cancelled')) {
            return;
        }

        $this->makeQueuePollingCancellationAware();

        // Decorate rather than rebind. The callables Laravel builds for
        // `isDownForMaintenance` and `resetScope` are carried over from the
        // instance it already made, instead of copied: `resetScope` grows
        // across releases -- it has picked up scoped-instance forgetting,
        // facade clearing and peak-memory reset -- and a copy would quietly
        // fall behind.
        $this->app->extend('queue.worker', static function (IlluminateWorker $worker, $app): Worker {
            $adapted = new Worker(
                $app['queue'],
                $app['events'],
                $app[ExceptionHandler::class],
                static fn (): bool => false,
            );

            (function () use ($worker): void {
                $this->isDownForMaintenance = $worker->isDownForMaintenance;
                $this->resetScope = $worker->resetScope;
            })->call($adapted);

            return $adapted;
        });

        // Laravel's stock queue command renders an interactive terminal line
        // in fragments. Replace only the command used inside Claviron so file
        // and journal transports receive one complete, source-owned record.
        $this->app->extend(
            IlluminateWorkCommand::class,
            static fn (IlluminateWorkCommand $_command, $app): WorkCommand => new WorkCommand(
                $app['queue.worker'],
                $app['cache.store'],
            ),
        );
    }

    /**
     * Keep an idle Redis worker reachable by Claviron cancellation.
     *
     * Redis' blocking pop runs below the PHP VM, so neither
     * `claviron_is_cancelled()` nor the cancellation-aware `sleep()` intercept
     * can run until `block_for` expires. A non-blocking pop lets Laravel fall
     * through to its ordinary `--sleep` option; the embedded runtime can then
     * cut that sleep short as soon as the task is cancelled.
     *
     * This provider is inactive under ordinary PHP CLI/FPM, so their queue
     * polling policy remains entirely application-owned.
     */
    private function makeQueuePollingCancellationAware(): void
    {
        $config = $this->app['config'];
        $connections = $config->get('queue.connections', []);

        foreach ($connections as $name => $connection) {
            if (!is_array($connection) || ($connection['driver'] ?? null) !== 'redis') {
                continue;
            }

            $config->set("queue.connections.{$name}.block_for", null);
        }
    }
}
