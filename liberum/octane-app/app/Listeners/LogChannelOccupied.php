<?php

declare(strict_types=1);

namespace App\Listeners;

use Claviron\Laravel\Broadcasting\Events\ChannelOccupied;
use Claviron\Laravel\Queue\QueuesEventOnce;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Facades\Log;

final class LogChannelOccupied implements ShouldQueue
{
    use QueuesEventOnce;

    public function handle(ChannelOccupied $event): void
    {
        Log::info('Broadcast channel occupied', [
            'app_id' => $event->appId,
            'channel' => $event->channel,
            'time' => $event->time->format('Y-m-d\TH:i:s.uP'),
            'user_id' => $event->userId,
        ]);
    }
}
