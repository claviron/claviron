<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Models\User;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

final class SessionController extends Controller
{
    public function show(Request $request): JsonResponse
    {
        $session = $request->session();
        $firstSeenAt = $session->remember(
            'claviron_demo.first_seen_at',
            static fn (): string => now()->toIso8601String(),
        );
        $userId = $session->remember(
            'claviron_demo.user_id',
            static fn (): ?int => User::query()->oldest('id')->value('id'),
        );
        $visits = (int) $session->increment('claviron_demo.visits');
        $user = $userId === null
            ? null
            : User::query()->find($userId, ['id', 'name', 'email']);

        return response()->json([
            'session' => [
                'id' => $session->getId(),
                'visits' => $visits,
                'first_seen_at' => $firstSeenAt,
                'driver' => config('session.driver'),
                'store' => config('session.store'),
            ],
            'user' => $user?->only(['id', 'name', 'email']),
            'served_by' => $request->attributes->get('claviron') ?? [],
        ]);
    }

    public function destroy(Request $request): JsonResponse
    {
        $session = $request->session();
        $previousId = $session->getId();

        $session->invalidate();
        $session->regenerateToken();

        return response()->json([
            'previous_id' => $previousId,
            'session' => [
                'id' => $session->getId(),
                'visits' => 0,
            ],
        ]);
    }
}
