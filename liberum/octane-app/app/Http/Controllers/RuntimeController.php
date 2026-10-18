<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

/**
 * What the runtime tells a request about itself: the attributes Claviron
 * attaches, the request as PHP sees it, and the whole of `$_SERVER`.
 */
final class RuntimeController extends Controller
{
    /** Hold an Orbit-forwarded PHP request open while testing edge drain. */
    public function sleep(): JsonResponse
    {
        sleep(10);

        return response()->json([
            'completed' => true,
            'sleep_seconds' => 10,
            'pid' => getmypid(),
        ]);
    }

    public function __invoke(Request $request): JsonResponse
    {
        return response()->json([
            'driver' => config('octane.server'),
            ...($request->attributes->get('claviron') ?? []),
            'method' => $request->method(),
            'url' => $request->fullUrl(),
            'query' => $request->query(),
            'server' => $_SERVER,
        ]);
    }
}
