<?php

declare(strict_types=1);

namespace Claviron\Laravel\Queue;

use Illuminate\Queue\Worker as IlluminateWorker;
use Illuminate\Queue\WorkerOptions;

/**
 * Laravel's queue worker with the two things a thread cannot do taken out.
 *
 * A `php:` task runs as a thread inside a Claviron worker *process* it does not
 * own. Everything else `Illuminate\Queue\Worker` does is inherited untouched --
 * `queue:restart`, maintenance mode, the WorkerStarting/Idle/Stopping events,
 * `resetScope`, `--max-jobs`, `--max-time`, the memory limit, `--rest`,
 * `--sleep` -- which is why this extends rather than reimplements the loop.
 *
 * Measured against the stock worker on a three-slot pool: drained in 1.06s with
 * every instance reporting completion, where the stock worker took 11s and
 * ended in a bailout with its job abandoned.
 */
final class Worker extends IlluminateWorker
{
    /**
     * No POSIX signals, for two separate reasons.
     *
     * `pcntl_signal()` installs handlers for the whole process, so this thread
     * would take over SIGTERM/SIGQUIT/SIGINT for the Claviron worker it lives
     * in -- the very signals its supervisor uses to stop it. `pcntl_alarm()`
     * arms one alarm for that same process, which the slots of a pool would
     * then fight over.
     *
     * One method closes all three call sites: Laravel guards
     * `listenForSignals()`, `registerTimeoutHandler()` and
     * `resetTimeoutHandler()` behind this single answer.
     *
     * The cost is `--timeout`: a runaway job has no per-job deadline here, and
     * nothing in PHP can give one back without the alarm.
     */
    protected function supportsAsyncSignals(): bool
    {
        return false;
    }

    /**
     * Deliver the stop that `listenForSignals()` would have delivered.
     *
     * Claviron cancels a task through its own channel, and the engine
     * intercepts `sleep()`, so an idle worker observes this mid-sleep rather
     * than at the end of one.
     *
     * Only `$shouldQuit` is set here. The stop itself -- its status, its
     * reason, the `WorkerStopping` event -- stays Laravel's.
     */
    protected function stopIfNecessary(
        WorkerOptions $options,
        $lastRestart,
        ...$arguments
    ) {
        if (function_exists('claviron_is_cancelled') && claviron_is_cancelled()) {
            $this->shouldQuit = true;
        }

        // Laravel versions add trailing stop-policy inputs. Preserve their
        // positions and defaults rather than interpreting them in the adapter.
        return parent::stopIfNecessary($options, $lastRestart, ...$arguments);
    }
}
