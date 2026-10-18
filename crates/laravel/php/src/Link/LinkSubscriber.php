<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link;

use Claviron\Laravel\EventBridge;
use Claviron\Laravel\Link\Events\ApplicationConnected;
use Claviron\Laravel\Link\Events\ApplicationLeft;
use Claviron\Laravel\Link\Events\EdgeConnected;
use Claviron\Laravel\Link\Events\EdgeLeft;
use Claviron\Laravel\Link\Events\EdgeShutdownInitiated;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Facades\Log;

/**
 * Opt-in: who is on the links this application takes part in.
 *
 * Add it to an event service provider's `$subscribe`. It subscribes this
 * process to its directly observed connections, departures and edge shutdown
 * notices. It does not ask other processes to introduce themselves or receive
 * their conversations. It logs observations without changing shutdown policy.
 * The events are ordinary Laravel events, so an application may listen to
 * them as well.
 */
final class LinkSubscriber
{
    public function __construct(private readonly EventBridge $bridge) {}

    /** @return array<class-string, string> */
    public function subscribe(Dispatcher $events): array
    {
        if (EventBridge::available()) {
            $this->bridge->subscribe([
                EdgeConnected::class,
                ApplicationConnected::class,
                EdgeLeft::class,
                EdgeShutdownInitiated::class,
                ApplicationLeft::class,
            ]);
        }

        return [
            EdgeConnected::class => 'edgeConnected',
            ApplicationConnected::class => 'applicationConnected',
            EdgeLeft::class => 'edgeLeft',
            EdgeShutdownInitiated::class => 'edgeShutdownInitiated',
            ApplicationLeft::class => 'applicationLeft',
        ];
    }

    public function edgeConnected(EdgeConnected $event): void
    {
        Log::info('claviron link: edge connected', [
            'name' => $event->name, 'fleet' => $event->fleet, 'role' => $event->role, 'pid' => $event->pid,
            'application' => $event->application, 'lane' => $event->lane,
        ]);
    }

    public function applicationConnected(ApplicationConnected $event): void
    {
        Log::info('claviron link: application connected', [
            'name' => $event->name, 'fleet' => $event->fleet, 'role' => $event->role, 'pid' => $event->pid,
            'edge' => $event->edge, 'lane' => $event->lane,
        ]);
    }

    public function edgeLeft(EdgeLeft $event): void
    {
        Log::warning('claviron link: edge left', ['edge' => $event->name, 'fleet' => $event->fleet, 'application' => $event->application]);
    }

    public function applicationLeft(ApplicationLeft $event): void
    {
        Log::warning('claviron link: application left', ['application' => $event->name, 'fleet' => $event->fleet, 'edge' => $event->edge]);
    }

    public function edgeShutdownInitiated(EdgeShutdownInitiated $event): void
    {
        Log::info('claviron link: edge shutdown initiated', ['edge' => $event->edge, 'fleet' => $event->fleet, 'application' => $event->application]);
    }
}
