<?php

declare(strict_types=1);

namespace Claviron\Laravel\Queue;

use Claviron\Laravel\EventBridge;
use RuntimeException;

/** Admit one queued listener job for one fleet ApplicationEvent envelope. */
trait QueuesEventOnce
{
    final public function shouldQueue(object $event): bool
    {
        $eventId = app(EventBridge::class)->currentEventId();

        if ($eventId === null) {
            return true;
        }

        if (!function_exists('orbit_lock_acquire')) {
            throw new RuntimeException('Application event admission requires the Orbit lock runtime.');
        }

        return \orbit_lock_acquire(
            $this->claimKey($eventId),
            bin2hex(random_bytes(16)),
            $this->claimSeconds() * 1000,
        );
    }

    protected function claimSeconds(): int
    {
        return 60;
    }

    private function claimKey(string $eventId): string
    {
        $listener = substr(hash('sha256', static::class), 0, 16);

        return "event-listener:{$listener}:{$eventId}";
    }
}
