<?php

declare(strict_types=1);

namespace Claviron\Symfony\Link;

use Claviron\Symfony\Link\Events\ApplicationConnected;
use Claviron\Symfony\Link\Events\ApplicationLeft;
use Claviron\Symfony\Link\Events\EdgeConnected;
use Claviron\Symfony\Link\Events\EdgeLeft;
use Psr\Log\LoggerInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

/** Optional ready-made logger; applications can subscribe to these events too. */
final class LinkSubscriber implements EventSubscriberInterface
{
    public function __construct(private readonly LoggerInterface $logger) {}

    public static function getSubscribedEvents(): array
    {
        return [
            EdgeConnected::class => 'edgeConnected',
            ApplicationConnected::class => 'applicationConnected',
            EdgeLeft::class => 'edgeLeft',
            ApplicationLeft::class => 'applicationLeft',
        ];
    }

    public function edgeConnected(EdgeConnected $event): void
    {
        $this->logger->info('claviron link: edge connected', [
            'name' => $event->name, 'fleet' => $event->fleet,
            'role' => $event->role, 'pid' => $event->pid,
            'application' => $event->application, 'lane' => $event->lane,
        ]);
    }

    public function applicationConnected(ApplicationConnected $event): void
    {
        $this->logger->info('claviron link: application connected', [
            'name' => $event->name, 'fleet' => $event->fleet,
            'role' => $event->role, 'pid' => $event->pid,
            'edge' => $event->edge, 'lane' => $event->lane,
        ]);
    }

    public function edgeLeft(EdgeLeft $event): void
    {
        $this->logger->warning('claviron link: edge left', [
            'edge' => $event->name, 'fleet' => $event->fleet,
            'application' => $event->application,
        ]);
    }

    public function applicationLeft(ApplicationLeft $event): void
    {
        $this->logger->warning('claviron link: application left', [
            'application' => $event->name, 'fleet' => $event->fleet,
            'edge' => $event->edge,
        ]);
    }
}
