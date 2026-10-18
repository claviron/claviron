<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link\Events;

/** An edge lane reached this application; observed locally, never broadcast. */
final class EdgeConnected extends Introduction
{
    public function __construct(
        string $name,
        string $fleet,
        string $role,
        ?int $pid,
        int $lane,
        public readonly string $application,
    ) {
        parent::__construct($name, $fleet, $role, $pid, $lane);
    }

    public static function topic(): string
    {
        return 'claviron.edge.connected.v1';
    }

    public static function fromPayload(array $payload): self
    {
        $who = self::introduction($payload);

        return new self($who['name'], $who['fleet'], $who['role'], $who['pid'], $who['lane'], (string) $payload['application']);
    }

    public function toPayload(int $budget): array
    {
        return parent::toPayload($budget) + ['application' => $this->application];
    }
}
