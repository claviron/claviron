<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link\Events;

use Claviron\Laravel\Events\FleetEvent;

/**
 * The last lane of an edge left this application's fleet: that edge no longer
 * reaches this application. Reported by this process, to this process only.
 */
final class EdgeLeft extends FleetEvent
{
    public function __construct(
        public readonly string $name,
        public readonly string $fleet,
        public readonly string $application,
    ) {
    }

    public static function topic(): string
    {
        return 'claviron.edge.left.v1';
    }

    public static function fromPayload(array $payload): self
    {
        return new self((string) ($payload['name'] ?? ''), (string) ($payload['fleet'] ?? ''), (string) ($payload['application'] ?? ''));
    }

    public function toPayload(int $budget): array
    {
        return ['name' => $this->name, 'fleet' => $this->fleet, 'application' => $this->application];
    }
}
