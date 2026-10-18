<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link\Events;

use Claviron\Laravel\Events\FleetEvent;

/** The edge announced shutdown; its Link has not necessarily disconnected. */
final class EdgeShutdownInitiated extends FleetEvent
{
    public function __construct(
        public readonly string $edge,
        public readonly string $fleet,
        public readonly string $application,
    ) {}

    public static function topic(): string
    {
        return 'claviron.edge.shutdown.initiated.v1';
    }

    public static function fromPayload(array $payload): self
    {
        return new self((string) ($payload['edge'] ?? ''), (string) ($payload['fleet'] ?? ''), (string) ($payload['application'] ?? ''));
    }

    public function toPayload(int $budget): array
    {
        return ['edge' => $this->edge, 'fleet' => $this->fleet, 'application' => $this->application];
    }
}
