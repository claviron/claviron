<?php

declare(strict_types=1);

namespace Claviron\Symfony\Link\Events;

/** Direct lane identity; fleet names the Link, role names its transport. */
abstract readonly class Introduction
{
    public function __construct(
        public string $name,
        public string $fleet,
        public string $role,
        public ?int $pid,
    ) {}

    /** @return array{name: string, fleet: string, role: string, pid: ?int} */
    protected static function introduction(array $payload): array
    {
        return [
            'name' => self::string($payload, 'name'),
            'fleet' => self::string($payload, 'fleet'),
            'role' => self::string($payload, 'role'),
            'pid' => array_key_exists('pid', $payload) && $payload['pid'] === null ? null : self::integer($payload, 'pid'),
        ];
    }

    protected static function string(array $payload, string $key): string
    {
        if (! isset($payload[$key]) || ! is_string($payload[$key])) {
            throw new \UnexpectedValueException("Claviron link event field '$key' must be a string.");
        }

        return $payload[$key];
    }

    protected static function integer(array $payload, string $key): int
    {
        if (! isset($payload[$key]) || ! is_int($payload[$key])) {
            throw new \UnexpectedValueException("Claviron link event field '$key' must be an integer.");
        }

        return $payload[$key];
    }
}
