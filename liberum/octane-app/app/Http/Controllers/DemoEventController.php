<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Events\DemoEvent;
use App\Events\SharedEvent;
use App\Models\User;
use Illuminate\Http\JsonResponse;

final class DemoEventController extends Controller
{
    public function __invoke(): JsonResponse
    {
        $user = User::query()->findOrFail(1);
        event(new DemoEvent($user));

        return response()->json(['published' => true], 202);
    }

    public function shared(): JsonResponse
    {
        $user = User::query()->findOrFail(1);
        event(new SharedEvent($user));

        return response()->json(['published' => true], 202);
    }
}
