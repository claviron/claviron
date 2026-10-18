<?php

declare(strict_types=1);

namespace Claviron\Laravel\Auth\Events;

use Claviron\Laravel\Events\FleetEvent;
use InvalidArgumentException;

/** Shared auth fact; the optional native translator separately hydrates Laravel events. */
final class AuthEvent extends FleetEvent
{
    private function __construct(
        public readonly string $kind,
        public readonly string $subject,
        public readonly ?string $guard,
        public readonly bool $remember,
        public readonly ?string $username,
    ) {}

    public static function topic(): string
    {
        return 'claviron.auth.event.v1';
    }

    public static function fromPayload(array $payload): self
    {
        $kind = $payload['kind'] ?? null;
        $subject = $payload['subject'] ?? null;
        $guard = $payload['guard'] ?? null;
        $remember = $payload['remember'] ?? false;
        $username = $payload['username'] ?? null;
        if (($payload['version'] ?? null) !== 1
            || ! in_array($kind, ['login', 'logout', 'registered'], true)
            || ! is_string($subject) || trim($subject) === ''
            || ($kind !== 'registered' && (! is_string($guard) || trim($guard) === ''))
            || ($kind === 'registered' && $guard !== null)
            || ! is_bool($remember)
            || ($username !== null && ! is_string($username))) {
            throw new InvalidArgumentException('Invalid Claviron auth event payload.');
        }

        return new self($kind, $subject, $guard, $remember, $username);
    }
}
