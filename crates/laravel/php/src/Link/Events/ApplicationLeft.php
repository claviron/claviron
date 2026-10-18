<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link\Events;

use Claviron\Laravel\Events\FleetEvent;

/**
 * The last lane serving an application name left a fleet this edge reaches:
 * that application can no longer be reached. What runs behind the name is
 * not the edge's business. Reported by this process, to this process only.
 */
final class ApplicationLeft extends FleetEvent
{
    public function __construct(
        public readonly string $name,
        public readonly string $fleet,
        public readonly string $edge,
    ) {
    }

    public static function topic(): string
    {
        return 'claviron.application.left.v1';
    }

    public static function fromPayload(array $payload): self
    {
        return new self((string) ($payload['name'] ?? ''), (string) ($payload['fleet'] ?? ''), (string) ($payload['edge'] ?? ''));
    }

    public function toPayload(int $budget): array
    {
        return ['name' => $this->name, 'fleet' => $this->fleet, 'edge' => $this->edge];
    }
}
