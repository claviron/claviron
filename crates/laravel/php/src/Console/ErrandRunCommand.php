<?php

declare(strict_types=1);

namespace Claviron\Laravel\Console;

use Claviron\Laravel\Events\InvocationCompleted;
use Claviron\Laravel\Events\InvocationFailed;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Log;
use Symfony\Component\Console\Output\BufferedOutput;
use Throwable;

/**
 * One scheduled command, run the way Laravel's background child runs it.
 *
 * This is the program the invocation bridge boots: `artisan errand:run <command>`
 * on a fresh PHP thread, once. It owns what `schedule:finish` and the command
 * itself would have owned: the run, the log line, and the finish that runs the
 * after-callbacks, releases the overlap lock the scheduler took, and
 * dispatches `ScheduledBackgroundTaskFinished`.
 *
 * Kept as an artisan command rather than inlined into the bridge so the same
 * thing can be run by hand, from a subprocess, or from any other launcher, and
 * behave identically.
 */
final class ErrandRunCommand extends Command
{
    protected $signature = 'errand:run
        {line : The artisan command line to run -- `command` is taken by Symfony for the command name itself}
        {--finish= : Event id for schedule:finish, which runs the after-callbacks and releases the overlap lock the scheduler took}
        {--lock : Refuse to run while another invocation of the same command line holds the Orbit lock}
        {--lock-ttl= : Lock lifetime in milliseconds; defaults to three execution budgets, or a day without one}
        {--output= : File to write the command output to, as sendOutputTo() would}
        {--append : Append to --output instead of replacing it}';

    protected $description = 'Run one scheduled command as an invoked errand';

    protected $hidden = true;

    public function handle(): int
    {
        // Measured here, first thing, because this is the earliest moment the
        // invoked program exists as far as the framework is concerned. Orbit
        // stamps publication and the executor stamps admission, so queueing
        // and PHP boot are visible separately.
        $submittedAt = (int) (getenv('CLAVIRON_INVOKE_SUBMITTED_AT') ?: 0);
        $admittedAt = (int) (getenv('CLAVIRON_INVOKE_ADMITTED_AT') ?: 0);
        $now = (int) round(microtime(true) * 1000);
        $queued = $submittedAt > 0 && $admittedAt >= $submittedAt ? $admittedAt - $submittedAt : null;
        $boot = $admittedAt > 0 ? $now - $admittedAt : null;
        $invocation = (getenv('CLAVIRON_INVOKE_KIND') ?: '-') . '/' . (getenv('CLAVIRON_INVOKE_ID') ?: '-');

        $command = (string) $this->argument('line');
        $finish = $this->option('finish') ?: null;

        // `withoutOverlapping()`, done here and in shared memory rather than
        // by the scheduler in the cache store: no database round trip, fleet
        // wide, and the program guards itself no matter who launched it. The
        // key is the command line, so two schedule entries that run the same
        // command exclude each other too -- which is what "overlap" means to
        // the command, if not to Laravel's per-event mutex name.
        $lock = null;
        $owner = $invocation . '@' . getmypid();

        if ($this->option('lock')) {
            if (!function_exists('orbit_lock_acquire')) {
                Log::warning("errand invocation={$invocation} --lock ignored, no Orbit here: {$command}");
            } else {
                $lock = 'errand:' . sha1($command);

                if (!orbit_lock_acquire($lock, $owner, $this->lockTtl())) {
                    Log::info(sprintf(
                        'errand invocation=%s skipped, lock held by %s: %s',
                        $invocation,
                        orbit_lock_owner($lock) ?? '?',
                        $command
                    ));

                    return self::SUCCESS;
                }
            }
        }

        $startedAt = microtime(true);
        $code = 0;
        $output = new BufferedOutput();
        $done = false;

        // A budget kill is a PHP fatal, not a Throwable: `finally` never runs,
        // and without this the lock would outlive the command by its whole TTL
        // and the event would never be finished -- `onFailure()` silent, the
        // next occurrence skipped for as long as the lock lasts. A shutdown
        // function is the one thing PHP still runs after a fatal.
        register_shutdown_function(function () use (&$done, $lock, $owner, $invocation, $command, $finish, $output, $startedAt): void {
            if ($done) {
                return;
            }

            $fatal = error_get_last();
            $reason = $fatal !== null ? trim(explode("\n", (string) $fatal['message'])[0]) : 'exited';

            Log::error(sprintf(
                'errand invocation=%s run=%dms exit=fatal lock=%s %s -- %s',
                $invocation,
                (int) round((microtime(true) - $startedAt) * 1000),
                $lock === null ? '-' : 'released',
                $command,
                $reason
            ));

            $captured = $output->fetch();

            try {
                if (($file = $this->option('output')) !== null && $file !== '') {
                    file_put_contents($file, $captured, $this->option('append') ? FILE_APPEND : 0);
                }
                if ($lock !== null) {
                    orbit_lock_release($lock, $owner);
                }
                if ($finish !== null) {
                    // Nonzero, so `onFailure()` runs and the event closes.
                    Artisan::call('schedule:finish', ['id' => $finish, 'code' => 255]);
                }
            } catch (Throwable $exception) {
                report($exception);
            }

            $this->publishOutcome('InvocationFailed', $invocation, $command, 255, $startedAt, $captured, $reason);
        });

        try {
            $code = Artisan::call($command, [], $output);
        } catch (Throwable $exception) {
            $code = 1;
            report($exception);
        } finally {
            $elapsed = (int) round((microtime(true) - $startedAt) * 1000);
            // The record of this invocation. Laravel's scheduler wrote nothing
            // that means "ran": it submitted an invocation and moved on.
            Log::debug(sprintf(
                'errand invocation=%s queue=%s boot=%s run=%dms exit=%d lock=%s %s',
                $invocation,
                $queued === null ? '-' : "{$queued}ms",
                $boot === null ? '-' : "{$boot}ms",
                $elapsed,
                $code,
                $lock === null ? '-' : 'held',
                $command
            ));

            if ($lock !== null) {
                orbit_lock_release($lock, $owner);
            }

            $captured = $output->fetch();

            if (($file = $this->option('output')) !== null && $file !== '') {
                // What the subprocess's `> file` / `>> file` did, done by the
                // program that has the output in hand.
                file_put_contents($file, $captured, $this->option('append') ? FILE_APPEND : 0);
            }

            if ($finish !== null) {
                // The same close Laravel's `&` child performs: finds the event
                // by mutex name, runs `->then()` / `->onSuccess()` /
                // `->onFailure()`, releases the lock, dispatches the event.
                Artisan::call('schedule:finish', ['id' => $finish, 'code' => $code]);
            }

            $done = true;

            // Last, so a subscriber that hears it finds the event already
            // closed: lock released, after-callbacks run, output on disk.
            $this->publishOutcome($code === 0 ? 'InvocationCompleted' : 'InvocationFailed', $invocation, $command, $code, $startedAt, $captured);
        }

        return $code;
    }

    /**
     * Tell whoever is listening how this errand ended -- and what it said.
     *
     * The result was always here: `Artisan::call` hands back the exit code and
     * fills the output buffer. It goes out as a typed Laravel event; listeners
     * in this process see the object directly, and the event bridge publishes
     * it to the bus for every other process in the fleet -- bounded to the
     * frame, marked when cut, with the full copy on disk when `--output` asked
     * for one.
     *
     * Dispatching never throws for a publish failure: the bridge logs it.
     */
    private function publishOutcome(
        string $topic,
        string $invocation,
        string $command,
        int $exit,
        float $startedAt,
        string $captured,
        ?string $reason = null,
    ): void {
        $runMs = (int) round((microtime(true) - $startedAt) * 1000);
        $file = $this->option('output') ?: null;

        $event = $topic === 'InvocationCompleted'
            ? new InvocationCompleted($invocation, $command, $exit, $runMs, $captured, false, $file)
            : new InvocationFailed($invocation, $command, $exit, $runMs, $captured, false, $file, $reason);

        try {
            event($event);
        } catch (Throwable $exception) {
            report($exception);
        }
    }

    /**
     * How long the overlap lock may outlive a holder that never released it.
     *
     * Bounded by the invocation's own budget when there is one: the engine
     * interrupts the command at `CLAVIRON_INVOKE_TIMEOUT`, so a lock older
     * than a few budgets is a dead holder, not a slow one. Without a budget
     * the old default stands.
     */
    private function lockTtl(): int
    {
        if (($ttl = $this->option('lock-ttl')) !== null && $ttl !== '') {
            return (int) $ttl;
        }

        $budget = (int) (getenv('CLAVIRON_INVOKE_TIMEOUT') ?: 0);

        return $budget > 0 ? $budget * 3 * 1000 : 86_400_000;
    }
}
