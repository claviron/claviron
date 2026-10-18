<?php

declare(strict_types=1);

namespace Claviron\Symfony\Link\Events;

final readonly class ApplicationConnected extends Introduction
{
    public const TOPIC = 'claviron.application.connected.v1';

    public function __construct(string $name, string $fleet, string $role, ?int $pid, public string $edge, public int $lane)
    {
        parent::__construct($name, $fleet, $role, $pid);
    }

    public static function fromPayload(array $payload): self
    {
        $who = self::introduction($payload);

        return new self($who['name'], $who['fleet'], $who['role'], $who['pid'], self::string($payload, 'edge'), self::integer($payload, 'lane'));
    }
}
