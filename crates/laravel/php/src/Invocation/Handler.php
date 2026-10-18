<?php

declare(strict_types=1);

namespace Claviron\Laravel\Invocation;

/**
 * A class that serves invocations submitted under its own name.
 *
 * The operation on the ring is the class name: `Invoke::send(Mailer::class,
 * $mail)` reaches a consumer whose Registrar declares `Mailer::class`.
 * The handler is resolved from the container per invocation, so it may take
 * dependencies; what `handle()` returns travels back as the outcome's output.
 */
interface Handler
{
    public function handle(mixed $payload): mixed;
}
