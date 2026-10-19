<?php

declare(strict_types=1);

namespace App\Invocations;

use Lumenize\Invocation\Handler;

/**
 * A handler served by a Claviron task (`invoke:serve App\Invocations\Shout`):
 * `Invoke::send(Shout::class, ['text' => 'hello'])` reaches it on the ring,
 * and what it returns comes back as the invocation's outcome.
 */
final class Shout implements Handler
{
    public function handle(mixed $payload): mixed
    {
        return [
            'shout' => strtoupper((string) ($payload['text'] ?? '')),
            'worker' => getenv('CLAVIRON_WORKER_ID') ?: null,
            'pid' => getmypid(),
        ];
    }
}
