<?php

declare(strict_types=1);

namespace App\Listeners;

use Lumenize\Auth\Events\AuthEvent;
use Illuminate\Support\Facades\Log;

/** Demo observer only: the subject belongs to Revolution, not Octane's DB. */
final class LogRevolutionLogin
{
    public function handle(AuthEvent $event): void
    {
        if ($event->kind !== 'login') {
            return;
        }

        Log::info('Demo: Revolution login observed', [
            'subject' => $event->subject,
            'username' => $event->username,
            'guard' => $event->guard,
        ]);
    }
}
