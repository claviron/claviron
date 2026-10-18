<?php

declare(strict_types=1);

namespace Claviron\Laravel\Invocation;

use Claviron\Laravel\Events\InvocationCompleted;
use Claviron\Laravel\Events\InvocationFailed;
use Claviron\Laravel\EventBridge;
use Claviron\Runtime\Event;
use Claviron\Runtime\Invocation;
use Illuminate\Contracts\Container\Container;
use InvalidArgumentException;
use RuntimeException;
use Throwable;

/**
 * The serving side: one booted application, waiting on the ring for the
 * operations it handles.
 *
 * Runs inside a Claviron task; the task runner owns restarts and drain, and
 * `claviron_is_cancelled()` is how the loop hears the latter. Each
 * invocation is one `handle()` call, its outcome published as
 * `InvocationCompleted` or `InvocationFailed` with the invocation's id.
 */
final class Runner
{
    private ?Invocation $current = null;

    public function __construct(
        private readonly Container $app,
        private readonly Registry $registry,
    ) {}

    /**
     * Start only the registrations of this fixed application/profile.
     * Configuration controls the per-operation budget; zero is unbounded.
     */
    public function serve(): void
    {
        $handlers = $this->registry->handlers();
        $events = $this->registry->events();
        $budgetSeconds = $this->registry->budgetSeconds();
        if ($handlers === [] && $events === []) {
            throw new InvalidArgumentException('No invocations or events are registered for this application/profile.');
        }
        if (!EventBridge::available() || !function_exists('claviron_task_receive') || !(new \ReflectionFunction('claviron_task_receive'))->isInternal()) {
            throw new RuntimeException('Serving invocations requires the Claviron runtime.');
        }

        $bridge = $this->app->make(EventBridge::class);
        $bridge->subscribe($events);

        foreach ($handlers as $class) {
            orbit_invoke_subscribe($class);
        }

        // A fatal -- the budget, memory -- ends the process without a return.
        // Say so for the invocation that was in flight; the task runner
        // brings the daemon back.
        register_shutdown_function(function (): void {
            $error = error_get_last();
            if ($this->current !== null && $error !== null && in_array($error['type'], [E_ERROR, E_CORE_ERROR, E_COMPILE_ERROR, E_USER_ERROR], true)) {
                $this->publish(new InvocationFailed(
                    $this->current->id,
                    $this->current->operation,
                    255,
                    0,
                    '',
                    false,
                    null,
                    strtok((string) $error['message'], "\n") ?: null,
                ));
            }
        });

        while (!$this->cancelled()) {
            $input = claviron_task_receive();
            if ($input === null) {
                break;
            }
            if ($input instanceof Event) {
                if ($budgetSeconds > 0) {
                    claviron_budget_begin($budgetSeconds);
                }
                try {
                    $bridge->deliver($input);
                } catch (Throwable $failure) {
                    report($failure);
                } finally {
                    if ($budgetSeconds > 0) {
                        claviron_budget_end();
                    }
                }
            } else {
                $this->run($input, $budgetSeconds);
            }
        }
    }

    private function run(Invocation $invocation, int $budgetSeconds): void
    {
        $this->current = $invocation;
        $startedAt = microtime(true);

        if ($budgetSeconds > 0) {
            claviron_budget_begin($budgetSeconds);
        }

        try {
            /** @var Handler $handler */
            $handler = $this->app->make($this->registry->handlerClass($invocation->operation));
            $result = $handler->handle(Invoke::decode($invocation->payload));

            $this->publish(new InvocationCompleted(
                $invocation->id,
                $invocation->operation,
                0,
                $this->elapsedMs($startedAt),
                $result === null ? '' : json_encode($result, JSON_THROW_ON_ERROR),
            ));
        } catch (Throwable $failure) {
            report($failure);

            $this->publish(new InvocationFailed(
                $invocation->id,
                $invocation->operation,
                1,
                $this->elapsedMs($startedAt),
                '',
                false,
                null,
                strtok($failure->getMessage(), "\n") ?: $failure::class,
            ));
        } finally {
            if ($budgetSeconds > 0) {
                claviron_budget_end();
            }
            $this->current = null;
        }
    }

    private function publish(InvocationCompleted|InvocationFailed $outcome): void
    {
        try {
            event($outcome);
        } catch (Throwable $exception) {
            report($exception);
        }
    }

    private function elapsedMs(float $startedAt): int
    {
        return (int) round((microtime(true) - $startedAt) * 1000);
    }

    private function cancelled(): bool
    {
        return function_exists('claviron_is_cancelled') && claviron_is_cancelled();
    }
}
