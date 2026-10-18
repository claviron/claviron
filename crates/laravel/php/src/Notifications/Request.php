<?php

declare(strict_types=1);

namespace Claviron\Laravel\Notifications;

use InvalidArgumentException;

/** Application-owned notification name and recipient reference, never a PHP class from the wire. */
final readonly class Request
{
    public function __construct(
        public string $name,
        public string $recipient,
        public array $data = [],
    ) {
        if (trim($name) === '' || trim($recipient) === '') {
            throw new InvalidArgumentException('Notification name and recipient are required.');
        }
    }

    public static function fromPayload(mixed $payload): self
    {
        if (!is_array($payload) || ($payload['version'] ?? null) !== 1
            || !is_string($payload['name'] ?? null)
            || !is_string($payload['recipient'] ?? null)
            || !is_array($payload['data'] ?? null)) {
            throw new InvalidArgumentException('Invalid notification request.');
        }

        return new self($payload['name'], $payload['recipient'], $payload['data']);
    }

    public function toPayload(): array
    {
        return ['version' => 1, 'name' => $this->name, 'recipient' => $this->recipient, 'data' => $this->data];
    }
}
