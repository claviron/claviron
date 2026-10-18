<?php

declare(strict_types=1);

namespace App\Listeners;

use Claviron\Laravel\Broadcasting\Events\ChannelVacated;
use Claviron\Laravel\Queue\QueuesEventOnce;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Facades\Log;

final class LogChannelVacated implements ShouldQueue
{
    use QueuesEventOnce;

    public function handle(ChannelVacated $event): void
    {
        Log::info('Broadcast channel vacated', [
            'app_id' => $event->appId,
            'channel' => $event->channel,
            'time' => $event->time->format('Y-m-d\TH:i:s.uP'),
            'user_id' => $event->userId,
        ]);
    }
}
