<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link\Events;

use Claviron\Laravel\Events\FleetEvent;

/**
 * A directly connected peer as observed in this process's Link directory.
 *
 * Fleet is the Link segment's fleet; role is its transport role (edge, php,
 * asgi), not an inferred master/worker role. A missing PID remains unknown.
 */
abstract class Introduction extends FleetEvent
{
    public function __construct(
        public readonly string $name,
        public readonly string $fleet,
        public readonly string $role,
        public readonly ?int $pid,
        public readonly int $lane,
    ) {}

    /** @return array{name: string, fleet: string, role: string, pid: ?int, lane: int} */
    protected static function introduction(array $payload): array
    {
        return [
            'name' => (string) ($payload['name'] ?? ''),
            'fleet' => (string) ($payload['fleet'] ?? ''),
            'role' => (string) ($payload['role'] ?? ''),
            'pid' => isset($payload['pid']) ? (int) $payload['pid'] : null,
            'lane' => (int) $payload['lane'],
        ];
    }

    public function toPayload(int $budget): array
    {
        return ['name' => $this->name, 'fleet' => $this->fleet, 'role' => $this->role, 'pid' => $this->pid, 'lane' => $this->lane];
    }
}
