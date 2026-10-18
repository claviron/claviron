<?php

declare(strict_types=1);

namespace Claviron\Laravel\Octane;

use Claviron\Laravel\EventBridge;
use Claviron\Runtime\Event;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Foundation\Application;
use Illuminate\Http\Request;
use Laravel\Octane\Exceptions\TaskExceptionResult;
use Laravel\Octane\RequestContext;
use Laravel\Octane\Worker;
use Throwable;

/**
 * Octane worker with a lifecycle boundary for Claviron application events.
 *
 * An event arrives between HTTP requests. Running its Laravel listeners on
 * Octane's base application would reuse managers that still point at the
 * previous, already-flushed request sandbox. Octane's operation boundary
 * gives the event a live sandbox and reports listener failures without
 * terminating the persistent application loop.
 */
final class ClavironWorker extends Worker
{
    private ?Throwable $requestError = null;

    public function handle(Request $request, RequestContext $context): void
    {
        $this->requestError = null;

        parent::handle($request, $context);

        if ($this->requestError instanceof Throwable) {
            $error = $this->requestError;
            $this->requestError = null;

            // Octane has already rendered the 500 response, dispatched its
            // lifecycle events and flushed the request sandbox. Propagate the
            // original failure now so the host does not mistake the stopped
            // client for a script that returned successfully.
            throw $error;
        }
    }

    protected function handleWorkerError(
        Throwable $e,
        Application $app,
        Request $request,
        RequestContext $context,
        bool $hasResponded,
    ): void {
        $this->requestError = $e;

        parent::handleWorkerError($e, $app, $request, $context, $hasResponded);
    }

    public function handleEvent(Event $event): void
    {
        // `handleTask` is Octane's generic non-HTTP operation boundary. This
        // is not a Claviron Task: the public contract remains an application
        // event, while Octane performs its normal prepare/terminate cycle.
        $result = $this->handleTask(function () use ($event): void {
            try {
                $this->app->make(EventBridge::class)->deliver($event);
            } catch (Throwable $exception) {
                // A callback has no HTTP request to answer, and one bad
                // listener must not trip Octane's StopWorkerIfNecessary path.
                // Report it from the live operation sandbox, then keep the
                // persistent application available for the next input.
                app(ExceptionHandler::class)->report($exception);
            }
        });

        // `handleTask` converts failures in Octane's prepare/terminate
        // lifecycle into a value after dispatching WorkerErrorOccurred. The
        // latter also stops a StoppableClient. Do not let the application loop
        // then look like it returned normally and erase the actual failure.
        if ($result instanceof TaskExceptionResult) {
            throw $result->getOriginal();
        }
    }
}
