<?php

declare(strict_types=1);

namespace Claviron\Laravel\Notifications;

use Claviron\Laravel\Invocation\Handler as InvocationHandler;
use Illuminate\Contracts\Notifications\Dispatcher;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Notifications\Notification;

/**
 * Application subclasses resolve their recipients, enforce their rules and
 * construct an allowed notification. Laravel owns via(), queue and channels.
 */
abstract class Handler implements InvocationHandler
{
    public function __construct(private readonly Dispatcher $notifications)
    {
    }

    final public function handle(mixed $payload): array
    {
        $request = Request::fromPayload($payload);
        $recipient = $this->recipient($request);
        $notification = $this->notification($request);
        $this->notifications->send($recipient, $notification);

        // This acknowledges Laravel's call, not SMTP/browser delivery.
        return [
            'status' => $notification instanceof ShouldQueue ? 'queued' : 'dispatched',
            'name' => $request->name,
            'recipient' => $request->recipient,
        ];
    }

    abstract protected function recipient(Request $request): object;

    abstract protected function notification(Request $request): Notification;
}
