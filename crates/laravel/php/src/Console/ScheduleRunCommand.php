<?php

declare(strict_types=1);

namespace Claviron\Laravel\Console;

use Illuminate\Console\Application as ConsoleApplication;
use Illuminate\Console\Events\ScheduledTaskFinished;
use Illuminate\Console\Events\ScheduledTaskStarting;
use Illuminate\Console\Scheduling\CallbackEvent;
use Illuminate\Console\Scheduling\Event;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Console\Scheduling\ScheduleRunCommand as IlluminateScheduleRunCommand;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Facades\Date;
use Illuminate\Support\Facades\Log;

/**
 * `schedule:run` with the two assumptions a daemon breaks put back.
 *
 * Both come from the same place: under system cron this command owns a process
 * that lives exactly one minute, so "this object", "this run" and "this minute"
 * are the same thing. Called repeatedly from a long-lived task they come apart.
 */
final class ScheduleRunCommand extends IlluminateScheduleRunCommand
{
    protected $signature = 'schedule:run {--whisper : Do not output message indicating that no jobs were ready to run}';

    /**
     * Stamp the start of *this* run, not of the process.
     *
     * The parent records it in its constructor, which under cron is the same
     * moment. Here the container hands back one instance for the life of the
     * task, so without this two things stop working silently: `repeatEvents()`
     * computes `$startedAt->endOfMinute()` and finds it already past, so
     * sub-minute events never repeat after the first minute; and
     * `serverShouldRun($event, $startedAt)` keys the `onOneServer` mutex by it,
     * so such an event holds one key forever and never runs again.
     */
    public function handle(
        Schedule $schedule,
        Dispatcher $dispatcher,
        Cache $cache,
        ExceptionHandler $handler
    ) {
        $this->startedAt = Date::now();

        return parent::handle($schedule, $dispatcher, $cache, $handler);
    }

    /**
     * Leave the sub-minute loop when the task is cancelled.
     *
     * `repeatEvents()` holds the rest of the minute once any event is
     * sub-minute, and its only exit is this question -- asked before every
     * event, on every 100ms pass. Under cron that is enough, because the
     * process dies with its minute either way. Under a daemon it is the
     * difference between stopping now and stopping in up to a minute, which is
     * longer than any sensible `graceful_timeout` and therefore ends in the
     * engine interrupting PHP mid-`usleep`.
     *
     * Laravel's own answer to the same question is `php artisan
     * schedule:interrupt`, a cache flag written by another process. We have a
     * better channel and no other process, so the flag is ours.
     */
    protected function shouldInterrupt()
    {
        if (function_exists('claviron_is_cancelled') && claviron_is_cancelled()) {
            return true;
        }

        return parent::shouldInterrupt();
    }

    /**
     * Hand every artisan command to Claviron instead of spawning PHP.
     *
     * A command event is a job boundary here by default. `runInBackground()`
     * is therefore unnecessary: Claviron owns admission and execution either
     * way. Callback events remain the explicit in-process form because a PHP
     * closure cannot cross the invocation boundary.
     */
    protected function runEvent($event)
    {
        $command = $this->errandCommand($event);

        if ($command === null) {
            parent::runEvent($event);

            return;
        }

        if (!$this->sendEvent($event, $command)) {
            // Publishing must never take the scheduler down. Preserve the
            // event's original Laravel behavior as the failure fallback.
            parent::runEvent($event);
        }
    }

    /**
     * The artisan command an event would send, or null if it cannot be sent.
     *
     * Null is never "we gave up": a closure has no command line to send, and
     * outside Claviron there is nowhere to send it. Both belong to Laravel.
     */
    private function errandCommand(Event $event): ?string
    {
        if ($event instanceof CallbackEvent) {
            return null;
        }

        if (!function_exists('claviron_runtime_available') || !claviron_runtime_available()) {
            // Said once per event so a scheduler that silently runs every
            // command as a subprocess explains itself in the log.
            Log::warning(sprintf(
                'schedule: [%s] runs as a subprocess: %s',
                (string) $event->command,
                function_exists('claviron_runtime_available') ? 'runtime reports unavailable' : 'no Claviron runtime functions in this process'
            ));

            return null;
        }

        // `$event->command` is the shell line Laravel built for a subprocess:
        // the php binary, the artisan binary, then the command. Only the last
        // part means anything to an already-booted container.
        $command = trim(str_replace(
            [ConsoleApplication::phpBinary(), ConsoleApplication::artisanBinary()],
            '',
            (string) $event->command
        ));

        return $command === '' ? null : $command;
    }

    /**
     * Publish the event and report its asynchronous handoff.
     *
     * There is no failure branch for capacity: the bus accepts, the bridge
     * admits when it can, and a burst waits there rather than here. The only
     * way this does not publish is a bus that is gone, which is a worker that
     * is gone.
     */
    private function sendEvent(Event $event, string $command): bool
    {
        $argv = ['errand:run', $command];

        // Always: the launched program closes the event through
        // `schedule:finish <id>`, which looks it up by exactly this name and
        // runs the after-callbacks -- they are not conditional on a lock.
        $argv[] = '--finish=' . $event->mutexName();

        // `withoutOverlapping()` travels as a flag, not as a lock: the
        // launched program takes an Orbit lock on its own command line, in
        // shared memory, and releases it in `finally`. The cache mutex Laravel
        // would have used is never created, so `schedule:finish`'s release of
        // it is a no-op.
        if ($event->withoutOverlapping) {
            $argv[] = '--lock';
            // Only a non-default lifetime travels; the default is the
            // program's own default, and the frame is 512 bytes.
            if ($event->expiresAt !== 1440) {
                $argv[] = '--lock-ttl=' . ($event->expiresAt * 60 * 1000);
            }
        }

        // `->sendOutputTo()` / `->appendOutputTo()`: the file Laravel would
        // have redirected the subprocess into. The launched program writes
        // its captured output there itself, so the event parameter is the
        // one the developer already knows.
        if ($event->output !== $event->getDefaultOutput()) {
            $argv[] = '--output=' . $event->output;
            if ($event->shouldAppendOutput) {
                $argv[] = '--append';
            }
        }

        $startedAt = microtime(true);

        $environment = ['APP_RUNNING_IN_CONSOLE' => 'true'];
        foreach (['APP_PROFILE', 'APP_TYPE', 'APP_NAME', 'PARTNER_CODE', 'OPERATOR_USERNAME'] as $name) {
            $value = getenv($name);
            if ($value !== false && $value !== '') {
                $environment[$name] = $value;
            }
        }

        try {
            $invocationId = orbit_invoke('claviron.php.program.invoke.v1', [
                // The operation selects the executor; the payload says what
                // kind of work the framework submitted.
                'kind' => 'job',
                // Absolute by contract: the bridge resolves nothing, and runs
                // the script from its own directory unless told otherwise.
                'script' => base_path('artisan'),
                'argv' => $argv,
                // The launched program has no CLI SAPI to tell Laravel it is
                // on a console -- `php_sapi_name()` is "claviron" for every
                // model here. Without this it boots the web providers, never
                // registers console commands, and exits 1 on "no commands in
                // the errand namespace". The same line every `php:` task in
                // tasks.yaml carries.
                'env' => $environment,
            ]);
        } catch (\Throwable $exception) {
            // Almost always the bounded invocation frame limit. Reported once
            // per occurrence, then the event runs the way it would have
            // without us.
            Log::warning(sprintf(
                'schedule: could not invoke [%s], running it as a subprocess: %s',
                $command,
                $exception->getMessage()
            ));

            return false;
        }

        // Reported here rather than by the launched program, and this is
        // parity rather than a shortcut: Laravel's own background path
        // dispatches both around the *spawn* too, because `Event::run()`
        // returns as soon as the process exists. The program reports
        // `ScheduledBackgroundTaskFinished` later, through `schedule:finish`
        // -- exactly as the background child does.
        $this->dispatcher->dispatch(new ScheduledTaskStarting($event));

        $worker = WorkerIdentity::label();
        $this->components->task(sprintf(
            '<fg=gray>%s</>%s Invoking [%s] <fg=gray>(%s)</>',
            Date::now()->format('Y-m-d H:i:s'),
            $worker === null ? '' : sprintf(' <fg=gray>%s</>', $worker),
            $command,
            $invocationId
        ), fn () => true);

        $this->dispatcher->dispatch(new ScheduledTaskFinished(
            $event,
            round(microtime(true) - $startedAt, 2)
        ));

        $this->eventsRan = true;

        return true;
    }
}
