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
        $opcache = function_exists('opcache_get_status') ? opcache_get_status(false) : false;

        return response()->json([
            'driver' => config('octane.server'),
            'opcache' => $opcache === false ? null : [
                'enabled' => $opcache['opcache_enabled'],
                'cache_full' => $opcache['cache_full'],
                'restart_pending' => $opcache['restart_pending'],
                'statistics' => array_intersect_key($opcache['opcache_statistics'], array_flip([
                    'num_cached_scripts', 'hits', 'misses', 'oom_restarts', 'hash_restarts', 'manual_restarts',
                ])),
                'memory_usage' => array_intersect_key($opcache['memory_usage'], array_flip([
                    'used_memory', 'free_memory', 'wasted_memory',
                ])),
                'validate_timestamps' => ini_get('opcache.validate_timestamps'),
            ],
            ...($request->attributes->get('claviron') ?? []),
            'method' => $request->method(),
            'url' => $request->fullUrl(),
            'query' => $request->query(),
            'server' => $_SERVER,
        ]);
    }
}
