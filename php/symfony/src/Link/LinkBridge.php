<?php

declare(strict_types=1);

namespace Claviron\Symfony\Link;

use Claviron\Runtime\Event;
use Claviron\Symfony\Link\Events\ApplicationConnected;
use Claviron\Symfony\Link\Events\ApplicationLeft;
use Claviron\Symfony\Link\Events\EdgeConnected;
use Claviron\Symfony\Link\Events\EdgeLeft;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;

/** Decode native link notifications and dispatch ordinary Symfony events. */
final class LinkBridge
{
    private const TOPICS = [
        EdgeConnected::TOPIC,
        ApplicationConnected::TOPIC,
        EdgeLeft::TOPIC,
        ApplicationLeft::TOPIC,
    ];

    private function __construct(private readonly EventDispatcherInterface $dispatcher) {}

    public static function forKernel(KernelInterface $kernel): self
    {
        $dispatcher = $kernel->getContainer()->get('event_dispatcher');
        if (! $dispatcher instanceof EventDispatcherInterface) {
            throw new \RuntimeException('Symfony event_dispatcher is required for link events.');
        }

        return self::forDispatcher($dispatcher);
    }

    public static function forDispatcher(EventDispatcherInterface $dispatcher): self
    {
        if (function_exists('claviron_events_available') && claviron_events_available()) {
            claviron_event_subscribe_many(self::TOPICS);
        }

        return new self($dispatcher);
    }

    public function deliver(Event $event): bool
    {
        $type = match ($event->topic) {
            EdgeConnected::TOPIC => EdgeConnected::class,
            ApplicationConnected::TOPIC => ApplicationConnected::class,
            EdgeLeft::TOPIC => EdgeLeft::class,
            ApplicationLeft::TOPIC => ApplicationLeft::class,
            default => null,
        };
        if ($type === null) {
            return false;
        }
        if (! is_array($event->payload)) {
            throw new \UnexpectedValueException('Claviron link event payload must be a field map.');
        }
        $this->dispatcher->dispatch($type::fromPayload($event->payload));

        return true;
    }
}
