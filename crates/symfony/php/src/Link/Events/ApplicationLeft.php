<?php

declare(strict_types=1);

namespace Claviron\Symfony\Link\Events;

final readonly class ApplicationLeft
{
    public const TOPIC = 'claviron.application.left.v1';

    public function __construct(public string $name, public string $fleet, public string $edge)
    {
    }

    public static function fromPayload(array $payload): self
    {
        return new self(self::string($payload, 'name'), self::string($payload, 'fleet'), self::string($payload, 'edge'));
    }

    private static function string(array $payload, string $key): string
    {
        if (!isset($payload[$key]) || !is_string($payload[$key])) {
            throw new \UnexpectedValueException("Claviron link event field '$key' must be a string.");
        }

        return $payload[$key];
    }
}
