<?php

declare(strict_types=1);

namespace Claviron\Laravel\Auth;

use Claviron\Laravel\Auth\Events\AuthEvent;
use Illuminate\Auth\Events\Login;
use Illuminate\Auth\Events\Logout;
use Illuminate\Auth\Events\Registered;
use Illuminate\Contracts\Auth\UserProvider;
use Illuminate\Contracts\Events\Dispatcher;
use RuntimeException;

/** Preserve Laravel's native event types and ordinary listener/queue chain. */
final class DispatchAuthEvent
{
    public function __construct(
        private readonly UserProvider $users,
        private readonly Dispatcher $events,
        private readonly string $subjectKey,
    ) {}

    public function handle(AuthEvent $event): void
    {
        $user = $this->users->retrieveByCredentials([$this->subjectKey => $event->subject]);
        if ($user === null) {
            throw new RuntimeException('Claviron auth event subject not found.');
        }
        $native = match ($event->kind) {
            'login' => new Login($event->guard, $user, $event->remember),
            'logout' => new Logout($event->guard, $user),
            'registered' => new Registered($user),
        };
        $this->events->dispatch($native);
    }
}
