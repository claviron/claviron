<?php

declare(strict_types=1);

namespace Claviron\Laravel\Link\Events;

/** An application lane reached this edge; observed locally, never broadcast. */
final class ApplicationConnected extends Introduction
{
    public function __construct(
        string $name,
        string $fleet,
        string $role,
        ?int $pid,
        int $lane,
        public readonly string $edge,
    ) {
        parent::__construct($name, $fleet, $role, $pid, $lane);
    }

    public static function topic(): string
    {
        return 'claviron.application.connected.v1';
    }

    public static function fromPayload(array $payload): self
    {
        $who = self::introduction($payload);

        return new self($who['name'], $who['fleet'], $who['role'], $who['pid'], $who['lane'], (string) $payload['edge']);
    }

    public function toPayload(int $budget): array
    {
        return parent::toPayload($budget) + ['edge' => $this->edge];
    }
}
