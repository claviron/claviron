<?php

declare(strict_types=1);

namespace App\Listeners;

use App\Events\DemoEvent;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Facades\Log;

final class HandleDemoEvent implements ShouldQueue
{
    public function handle(DemoEvent $event): void
    {
        Log::info('Demo event handled for user', [
            'user' => $event->user->name,
        ]);
    }
}
