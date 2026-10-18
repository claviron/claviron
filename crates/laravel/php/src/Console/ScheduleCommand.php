<?php

declare(strict_types=1);

namespace Claviron\Laravel\Console;

use Claviron\Laravel\EventBridge;
use Claviron\Laravel\Events\InvocationCompleted;
use Claviron\Laravel\Events\InvocationFailed;
use Illuminate\Console\Command;
use Illuminate\Support\Carbon;

/**
 * The scheduler as a daemon, without a process per minute.
 *
 * `schedule:work` is Laravel's daemon form, but its body runs nothing: it
 * sleeps 100ms at a time and, on each new minute, spawns `php artisan
 * schedule:run` as a subprocess, then pumps that child's output to its own
 * stdout. Useful on a laptop, and unusable here -- an embedded SAPI has no CLI
 * binary to exec, and a child process would be outside this engine anyway, with
 * no Claviron functions, no fleet and no event bus.
 *
 * The alternative is not to reimplement the scheduler. `schedule:run` is
 * correct: it owns maintenance mode, `onOneServer`, `withoutOverlapping`,
 * sub-minute repetition, the task events and the output. It is only being
 * started the wrong way. So this runs it in-process, through the same
 * container, on a boot that happened once when the task started -- instead of
 * once per minute.
 *
 * Overlap is not guarded here for the same reason Laravel does not guard it in
 * `schedule:work`: it belongs to the event, as `withoutOverlapping()`. What is
 * different is that a run which outlives its minute cannot pile up behind a
 * spawner, because there is no spawner; the next minute simply waits.
 */
final class ScheduleCommand extends Command
{
    protected $signature = 'claviron:schedule';

    protected $description = 'Run the scheduler in-process, on one boot';

    public function handle(): int
    {
        // A daemon is not a request. php.ini's limit is shared with the web
        // model here, and the CLI SAPI zeroes it for exactly this shape.
        set_time_limit(0);

        $this->info(WorkerIdentity::message('Claviron scheduler running.'));

        // The minute this started in is already spent: cron would not fire for
        // it, and firing now would run an `everyMinute` event twice in it --
        // once on startup and once at the boundary. Wait for the next one, the
        // way both cron and `schedule:work` do.
        $last = Carbon::now()->startOfMinute();

        // The outcomes of what this scheduler sends out come back on the bus,
        // and this loop is the one place in the process that can pull them.
        // Subscribed before the first `schedule:run`, so nothing it invokes can
        // finish unheard.
        $bridge = $this->laravel->make(EventBridge::class);
        $bridge->subscribe([InvocationCompleted::class, InvocationFailed::class]);

        while (!$this->cancelled()) {
            $minute = Carbon::now()->startOfMinute();

            if (!$minute->equalTo($last)) {
                $last = $minute;
                $this->call('schedule:run');
            }

            // Typed and dispatched through Laravel: `Event::listen(InvocationCompleted::class, ...)`
            // in the application fires here, in the scheduler's own container.
            $bridge->pump();

            // A second of granularity on a minute schedule. `schedule:run`
            // decides what is due from the clock, not from when it was called,
            // so starting a moment late changes nothing.
            //
            // Cancellation inside `schedule:run` is not this loop's problem:
            // once any event is sub-minute that call holds the rest of the
            // minute, far longer than a `graceful_timeout`, so the exit has to
            // be in there. `Claviron\Laravel\Console\ScheduleRunCommand`
            // owns it.
            sleep(1);
        }

        $this->info(WorkerIdentity::message('Claviron scheduler stopped.'));

        return self::SUCCESS;
    }

    private function cancelled(): bool
    {
        return function_exists('claviron_is_cancelled') && claviron_is_cancelled();
    }
}
