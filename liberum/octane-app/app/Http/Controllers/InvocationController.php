<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Invocations\Shout;
use Claviron\Laravel\Events\InvocationFailed;
use Claviron\Laravel\Invocation\Invoke;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Http\Response;

/**
 * Invocations sent over the fleet and awaited from a request: one served by
 * a PHP handler on another worker, two served by Rust handlers in the
 * runtime's router.
 */
final class InvocationController extends Controller
{
    /**
     * `App\Invocations\Shout`, served by the `invoke-shout` task.
     */
    public function shout(Invoke $invoke): JsonResponse
    {
        $id = $invoke->send(Shout::class, ['text' => 'hello']);
        $outcome = $invoke->await($id, 5000);

        if ($outcome === null) {
            return response()->json(['id' => $id, 'status' => 'unknown'], 504);
        }

        return response()->json([
            'id' => $id,
            'status' => $outcome::topic(),               // InvocationCompleted | InvocationFailed
            'run_ms' => $outcome->runMs,
            'result' => json_decode($outcome->output, true),
        ]);
    }

    /**
     * The same operation shape served by a `js:` task (`web/js-tasks/shout.js`).
     */
    public function jsShout(Invoke $invoke): JsonResponse
    {
        $id = $invoke->send('js.shout.v1', ['text' => 'hello from php']);
        $outcome = $invoke->await($id, 5000);

        if ($outcome === null) {
            return response()->json(['id' => $id, 'status' => 'unknown'], 504);
        }

        return response()->json([
            'id' => $id,
            'status' => $outcome::topic(),
            'run_ms' => $outcome->runMs,
            'result' => json_decode($outcome->output, true),
        ]);
    }

    public function geoIp(Request $request, Invoke $invoke): JsonResponse|Response
    {
        $outcome = $invoke->await($invoke->send('claviron.geoip.lookup.v1', ['ip' => $request->ip()]), 2000);

        return $this->passThrough($outcome);
    }

    public function ping(Invoke $invoke): JsonResponse|Response
    {
        $outcome = $invoke->await($invoke->send('claviron.ping.v1', ['echo' => 'ping']), 2000);

        return $this->passThrough($outcome);
    }

    /**
     * A Rust handler's output is already JSON; hand it on as the body rather
     * than encoding it a second time.
     */
    private function passThrough(?object $outcome): JsonResponse|Response
    {
        if ($outcome === null) {
            return response()->json(['status' => 'unknown'], 504);
        }
        if ($outcome instanceof InvocationFailed) {
            return response()->json(['status' => 'failed', 'reason' => $outcome->reason], 502);
        }

        return response($outcome->output, 200)->header('Content-Type', 'application/json');
    }
}
