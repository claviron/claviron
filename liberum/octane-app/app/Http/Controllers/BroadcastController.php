<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Events\DemoMessage;
use App\Notifications\DemoNotification;
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Notification;

/**
 * Publishing through the native broadcaster. The Echo page at `/` posts here
 * and listens on the `demo` private channel for what comes back.
 */
final class BroadcastController extends Controller
{
    public function message(Request $request): JsonResponse
    {
        $message = $request->string('message')->trim()->value();

        broadcast(new DemoMessage(
            $message === '' ? 'Hello from the embedded Laravel queue.' : $message,
            now()->toIso8601String(),
        ));

        return response()->json(['published' => true]);
    }

    public function notification(): JsonResponse
    {
        Notification::route('broadcast', new PrivateChannel('demo'))->notifyNow(
            new DemoNotification(
                'Your Laravel notification arrived through Claviron.',
                now()->toIso8601String(),
            ),
        );

        return response()->json(['published' => true]);
    }
}
