<?php

declare(strict_types=1);

namespace Claviron\Laravel;

use Claviron\Laravel\Events\FleetEvent;
use Claviron\Runtime\Event;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Connect Laravel events to Claviron's application event bus.
 *
 * There are two wire formats. FleetEvent uses a shared topic and a
 * language-neutral payload. An ordinary Laravel event uses its class name as
 * the topic and PHP serialization, so only PHP applications can read it.
 *
 * Publishing is immediate; receiving requires subscribe() and an execution
 * that calls pump() or passes received frames to deliver(). Laravel listeners
 * are registered with Laravel's dispatcher, not with this bridge.
 */
final class EventBridge
{
    /** @var array<string, class-string<FleetEvent>> topic => class */
    private array $typedInbound = [];

    /** @var array<string, class-string> topic => serialized Laravel event class */
    private array $serializedInbound = [];

    /** @var array<class-string, true> */
    private array $relayed = [];

    /**
     * True while an event received from the bus is being dispatched locally.
     *
     * The outbound listener must not see that dispatch as something to
     * publish, or the bridge echoes: receive, dispatch, publish, receive...
     * Found the hard way -- one `InvocationCompleted` became a 27 MB log in
     * under a minute. A container dispatches on one thread, so a flag is
     * enough.
     */
    private bool $delivering = false;

    /** The fleet envelope being synchronously dispatched through Laravel. */
    private ?string $currentEventId = null;

    /**
     * Use the application's dispatcher for outbound observation and inbound
     * delivery to its existing Laravel listeners.
     */
    public function __construct(private readonly Dispatcher $dispatcher)
    {
    }

    /**
     * Check whether this PHP execution has a prepared application event bus.
     *
     * This is a capability check, not a subscription, consumer-health check,
     * or guarantee that another process will receive a published event.
     */
    public static function available(): bool
    {
        return function_exists('claviron_events_available') && claviron_events_available();
    }

    /**
     * Publish one event to the application event bus.
     *
     * FleetEvent uses its shared topic and language-neutral payload. An
     * ordinary object uses its class name and a PHP-serialized payload scoped
     * to the current PHP application. For FleetEvent, increasingly small
     * payload budgets are tried until a frame accepts the event.
     *
     * Returns false when the bus is unavailable or publication fails; failures
     * are logged. True means the bus accepted a frame, not that a listener ran.
     *
     * @return bool Whether publication succeeded.
     */
    public function publish(object $event): bool
    {
        if (!self::available()) {
            return false;
        }

        if (! $event instanceof FleetEvent) {
            try {
                claviron_event_publish($event::class, serialize($event));

                return true;
            } catch (Throwable $exception) {
                Log::warning(sprintf(
                    'claviron bus: could not publish %s: %s',
                    $event::class,
                    $exception->getMessage(),
                ));

                return false;
            }
        }

        $last = null;

        foreach ([512, 256, 0] as $budget) {
            try {
                if (function_exists('claviron_event_publish_shared')
                    && (new \ReflectionFunction('claviron_event_publish_shared'))->isInternal()) {
                    claviron_event_publish_shared($event::topic(), $event->toPayload($budget));
                } else {
                    // Compatibility with runtime v2: its only publish primitive
                    // was shared. Runtime v3 exposes the distinction natively.
                    claviron_event_publish($event::topic(), $event->toPayload($budget));
                }

                return true;
            } catch (Throwable $exception) {
                $last = $exception;
            }
        }

        Log::warning(sprintf('claviron bus: could not publish %s: %s', $event::topic(), $last?->getMessage() ?? '?'));

        return false;
    }

    /**
     * Register which incoming topics this execution can decode.
     *
     * FleetEvent classes must provide fromPayload(); ordinary Laravel event
     * classes are received as PHP-serialized objects under their class names.
     * This registers native subscriptions when the bus is available, but does
     * not register Laravel listeners or start a receive loop. Call it before
     * the work whose outcome is needed: new subscriptions start at the current
     * head and do not replay earlier frames.
     *
     * @param list<class-string> $classes Event classes to receive.
     */
    public function subscribe(array $classes): void
    {
        foreach ($classes as $class) {
            if (is_a($class, FleetEvent::class, true)) {
                if (!method_exists($class, 'fromPayload')) {
                    throw new \InvalidArgumentException(sprintf(
                        '%s cannot be subscribed because it has no fromPayload decoder.',
                        $class,
                    ));
                }
                $this->typedInbound[$class::topic()] = $class;
            } else {
                $this->serializedInbound[$class] = $class;
            }
        }

        if (self::available()) {
            claviron_event_subscribe_many(array_values(array_unique([
                ...array_keys($this->typedInbound),
                ...array_keys($this->serializedInbound),
            ])));
        }
    }

    /**
     * Route ordinary Laravel events through the bus before listeners run.
     *
     * During provider boot, subscribe to each class and install a Laravel
     * listener that publishes its serialized object. On successful publication
     * that listener stops the original local listener chain; a receiving
     * execution later dispatches the event to Laravel listeners. If publication
     * fails, the original local listener chain continues. Register this before
     * ordinary listeners when their delivery must be routed through the bus.
     * FleetEvent is excluded because its outbound path uses listenForOutbound().
     *
     * @param list<class-string> $classes Ordinary Laravel event classes to relay.
     */
    public function relay(array $classes): void
    {
        foreach ($classes as $class) {
            if (is_a($class, FleetEvent::class, true)) {
                throw new \InvalidArgumentException(sprintf(
                    '%s is a shared event; register it with subscribe(), not relay().',
                    $class,
                ));
            }
        }

        $this->subscribe($classes);

        foreach ($classes as $class) {
            if (isset($this->relayed[$class])) {
                continue;
            }

            $this->relayed[$class] = true;
            $this->dispatcher->listen($class, function (object $event): ?bool {
                if ($this->delivering) {
                    return null;
                }

                // The fleet delivery, not this source dispatch, owns the
                // ordinary Laravel listener chain. If the runtime cannot
                // publish, preserve Laravel's local behavior.
                return $this->publish($event) ? false : null;
            });
        }
    }

    /**
     * Return the bus envelope ID while an incoming event is dispatched.
     *
     * Returns null outside deliver()'s synchronous Laravel listener chain.
     * The ID is delivery context, not a property added to the event object.
     */
    public function currentEventId(): ?string
    {
        return $this->currentEventId;
    }

    /**
     * Receive available frames and pass each one to deliver().
     *
     * Task and scheduler executions call this themselves. Zero timeout polls
     * without waiting; a positive timeout waits for the first frame only, then
     * drains the rest without waiting. Unknown topics go to the optional native
     * callback. Persistent PHP applications use their own receive loop and
     * call deliver() instead.
     *
     * @param int $timeoutMs Maximum wait for the first frame, in milliseconds.
     * @param (callable(Event): void)|null $native Receiver for unknown topics.
     * @return int Number of recognized frames, including ones already claimed by another worker.
     */
    public function pump(int $timeoutMs = 0, ?callable $native = null): int
    {
        if (!self::available()) {
            return 0;
        }

        $delivered = 0;

        while (($received = claviron_event_receive($timeoutMs)) instanceof Event) {
            $timeoutMs = 0;

            if ($this->deliver($received, $native)) {
                $delivered++;
            }
        }

        return $delivered;
    }

    /**
     * Decode one received frame and dispatch its event through Laravel.
     *
     * Persistent applications call this for frames returned by
     * claviron_application_receive(); pump() uses it for polled frames. Unknown
     * topics go unchanged to the optional native callback. A serialized PHP
     * event is claimed so only one application worker runs its listeners.
     * Incoming dispatch is marked to prevent the outbound listener from
     * publishing the same event again.
     *
     * @param Event $received Frame supplied by the native runtime.
     * @param (callable(Event): void)|null $native Receiver for unknown topics.
     * @return bool Whether the topic was recognized, including when another worker already claimed it.
     */
    public function deliver(Event $received, ?callable $native = null): bool
    {
        $typed = $this->typedInbound[$received->topic] ?? null;
        $serialized = $this->serializedInbound[$received->topic] ?? null;

        if ($typed !== null && is_array($received->payload)) {
            $event = $typed::fromPayload($received->payload);
        } elseif ($serialized !== null && is_string($received->payload)) {
            $event = unserialize($received->payload, ['allowed_classes' => true]);

            if (!is_object($event) || !$event instanceof $serialized) {
                throw new \UnexpectedValueException(sprintf(
                    'Claviron application event payload is not an instance of %s.',
                    $serialized,
                ));
            }
        } else {
            // Not one of ours: a topic this process subscribed to on its own,
            // in its own vocabulary. It stays a native Claviron event and
            // goes to the caller untouched.
            if ($native !== null) {
                $native($received);
            }

            return false;
        }

        // A serialized Laravel event has one logical application consumer,
        // even though every PHP application worker sees the fleet broadcast.
        if ($serialized !== null && !$this->claim($received->id)) {
            return true;
        }

        $previousDelivering = $this->delivering;
        $previousEventId = $this->currentEventId;
        $this->delivering = true;
        $this->currentEventId = $received->id;

        try {
            $this->dispatcher->dispatch($event);
        } finally {
            $this->currentEventId = $previousEventId;
            $this->delivering = $previousDelivering;
        }

        return true;
    }

    /**
     * Publish locally dispatched FleetEvent objects through the bus.
     *
     * The provider installs this wildcard Laravel listener once when the bus
     * is available. Application code can then use ordinary event($event).
     * Events currently being delivered from the bus are ignored to avoid an
     * echo; ordinary Laravel events are not published by this listener.
     */
    public function listenForOutbound(): void
    {
        $this->dispatcher->listen('*', function (string $name, array $payload): void {
            $event = $payload[0] ?? null;

            // Raised here: publish. Received from the bus and being replayed
            // to local listeners: already on the bus, leave it.
            if ($this->delivering || !is_object($event)) {
                return;
            }

            if ($event instanceof FleetEvent) {
                $this->publish($event);
            }
        });
    }

    /**
     * Let only one worker of a persistent PHP application handle a serialized event.
     *
     * Other execution models do not need a claim. Persistent workers use the
     * envelope ID as an Orbit lock key before running Laravel listeners.
     *
     * @return bool Whether this execution may handle the event.
     */
    private function claim(string $eventId): bool
    {
        if (! function_exists('claviron_application_name')
            || ! (new \ReflectionFunction('claviron_application_name'))->isInternal()
            || claviron_application_name() === null) {
            return true;
        }

        if (!function_exists('orbit_lock_acquire')) {
            throw new \RuntimeException('Laravel application event delivery requires Orbit locks.');
        }

        return orbit_lock_acquire(
            "laravel-event:{$eventId}",
            bin2hex(random_bytes(16)),
            60_000,
        );
    }
}
