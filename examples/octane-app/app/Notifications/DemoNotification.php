<?php

namespace App\Notifications;

use Illuminate\Bus\Queueable;
use Illuminate\Notifications\Messages\BroadcastMessage;
use Illuminate\Notifications\Notification;

final class DemoNotification extends Notification
{
    use Queueable;

    public function __construct(
        private readonly string $message,
        private readonly string $sentAt,
    ) {
    }

    public function via(object $notifiable): array
    {
        return ['broadcast'];
    }

    public function toBroadcast(object $notifiable): BroadcastMessage
    {
        return (new BroadcastMessage([
            'message' => $this->message,
            'sentAt' => $this->sentAt,
            'source' => 'Laravel Notification',
        ]))->onConnection('sync');
    }

    public function broadcastType(): string
    {
        return 'demo.notification';
    }
}
