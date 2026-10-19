<?php

namespace App\Events;

use Illuminate\Broadcasting\InteractsWithSockets;
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcastNow;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

final class DemoMessage implements ShouldBroadcastNow
{
    use Dispatchable;
    use InteractsWithSockets;
    use SerializesModels;

    public function __construct(
        public readonly string $message,
        public readonly string $sentAt,
    ) {
    }

    public function broadcastOn(): array
    {
        return [new PrivateChannel('demo')];
    }

    public function broadcastAs(): string
    {
        return 'DemoMessage';
    }
}
