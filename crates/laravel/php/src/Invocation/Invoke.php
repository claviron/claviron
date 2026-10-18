<?php

declare(strict_types=1);

namespace Claviron\Laravel\Invocation;

use Claviron\Laravel\EventBridge;
use Claviron\Laravel\Events\InvocationCompleted;
use Claviron\Laravel\Events\InvocationFailed;
use Claviron\Laravel\Events\InvocationOutcome;
use Illuminate\Contracts\Events\Dispatcher;
use RuntimeException;

/**
 * Submit work to whoever serves an operation, and optionally wait for its
 * outcome.
 *
 * Not a queue: nothing is stored, nothing is retried, and a submission
 * nobody serves is lost. What it offers instead is an answer -- the
 * `InvocationCompleted` / `InvocationFailed` event carrying the id `send()`
 * returned -- in microseconds within the host, with no broker.
 */
final class Invoke
{
    private const OBJECT = '@php';

    /** @var array<string, InvocationOutcome> outcomes seen for ids being awaited */
    private array $outcomes = [];

    /** @var array<string, true> */
    private array $awaiting = [];

    private bool $listening = false;

    public function __construct(
        private readonly EventBridge $bridge,
        private readonly Dispatcher $dispatcher,
    ) {
    }

    /** True inside the engine, where the invocation functions exist. */
    public static function available(): bool
    {
        return function_exists('orbit_invoke');
    }

    /**
     * Submit `$payload` under `$operation`; a handler class name, usually.
     *
     * Returns the invocation id that every outcome for it will carry.
     */
    public function send(string $operation, mixed $payload): string
    {
        if (!self::available()) {
            throw new RuntimeException('Claviron invocations require the Claviron runtime.');
        }

        // Outcomes must be subscribed before the work exists: a subscription
        // starts at the head of the bus, and a fast handler answers in
        // milliseconds.
        $this->listen();

        return orbit_invoke($operation, self::encode($payload));
    }

    /**
     * Wait up to `$timeoutMs` for the outcome of one invocation.
     *
     * Null means nothing arrived in time. That is "unknown", not "failed":
     * the handler may still be running, or nobody may be serving the
     * operation at all.
     */
    public function await(string $invocation, int $timeoutMs): ?InvocationOutcome
    {
        $this->listen();
        $this->awaiting[$invocation] = true;

        try {
            $deadline = microtime(true) + $timeoutMs / 1000;

            while (!isset($this->outcomes[$invocation])) {
                $remaining = (int) round(($deadline - microtime(true)) * 1000);
                if ($remaining <= 0) {
                    return null;
                }
                $this->bridge->pump($remaining);
            }

            $outcome = $this->outcomes[$invocation];
            unset($this->outcomes[$invocation]);

            return $outcome;
        } finally {
            unset($this->awaiting[$invocation]);
        }
    }

    /**
     * Scalars and arrays travel as they are, readable by any subscriber;
     * an object is serialized behind a marker only PHP will look for.
     */
    public static function encode(mixed $payload): mixed
    {
        return is_object($payload) ? [self::OBJECT => base64_encode(serialize($payload))] : $payload;
    }

    public static function decode(mixed $payload): mixed
    {
        if (is_array($payload) && count($payload) === 1 && isset($payload[self::OBJECT]) && is_string($payload[self::OBJECT])) {
            return unserialize(base64_decode($payload[self::OBJECT], true));
        }

        return $payload;
    }

    private function listen(): void
    {
        if ($this->listening) {
            return;
        }
        $this->listening = true;

        $this->bridge->subscribe([InvocationCompleted::class, InvocationFailed::class]);

        $record = function (InvocationOutcome $outcome): void {
            if (isset($this->awaiting[$outcome->invocation])) {
                $this->outcomes[$outcome->invocation] = $outcome;
            }
        };
        $this->dispatcher->listen(InvocationCompleted::class, $record);
        $this->dispatcher->listen(InvocationFailed::class, $record);
    }
}
