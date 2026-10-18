<?php

declare(strict_types=1);

namespace Claviron\Laravel\Octane;

use Closure;
use Claviron\Runtime\Event;
use Illuminate\Foundation\Application;
use Illuminate\Http\Request;
use Laravel\Octane\ApplicationFactory;
use Laravel\Octane\RequestContext;
use Symfony\Component\HttpKernel\Exception\HttpExceptionInterface;

final class ApplicationRunner
{
    /** @param class-string<Request>|Closure(Application): class-string<Request> $requestClass */
    public static function run(string $basePath, string|Closure $requestClass = Request::class): void
    {
        if (!function_exists('claviron_runtime_available') || !claviron_runtime_available()) {
            throw new \RuntimeException('Run application.php through Claviron PhpApplicationService, not PHP CLI or FrankenPHP.');
        }

        // No set_time_limit() here: it would change nothing. Claviron cancels
        // PHP's own execution timer on its threads, and an application's budget
        // is Claviron's, not php.ini's: it is armed when receive hands PHP a
        // request or an event and lifted at the next receive (or once a
        // streamed reply's head is sent), from php.yaml
        // `applications.<name>.execution_timeout`, falling back to
        // `lifecycle.execution_timeout`. Waiting in receive is never counted,
        // so an idle application is not ended; only a single request or event
        // that overruns is. set_time_limit() does not extend that budget.
        $_ENV['APP_RUNNING_IN_CONSOLE'] = false;
        $_SERVER['APP_RUNNING_IN_CONSOLE'] = false;

        $client = new ClavironClient(claviron_application_respond(...));
        $worker = new ClavironWorker(new ApplicationFactory($basePath), $client);
        $worker->boot();

        try {
            // Resolve application-owned policy once, from the already-booted container.
            $requestClass = $requestClass instanceof Closure
                ? $requestClass($worker->application())
                : $requestClass;
            if (!is_string($requestClass) || !is_a($requestClass, Request::class, true)) {
                throw new \InvalidArgumentException('The request class must extend Illuminate\Http\Request.');
            }

            while (!$client->isStopped() && ($message = claviron_application_receive()) !== null) {
                // A subscribed event, not a request: something in this process asked
                // the bus for it (an `Invoke::await()` in a controller, say), and the
                // engine hands it to the loop that owns receive. Typed and dispatched
                // through the bridge, then back to waiting.
                if ($message instanceof Event) {
                    $worker->handleEvent($message);
                    unset($message);

                    continue;
                }

                self::handleMessage($message, $client, $worker, $requestClass);
                unset($message);
            }
        } catch (\Throwable $error) {
            error_log((string) $error);

            throw $error;
        } finally {
            $worker->terminate();
        }
    }

    /**
     * @throws \Throwable
     */
    private static function handleMessage(mixed $message, ClavironClient $client, ClavironWorker $worker, string $requestClass): void
    {
        $context = new RequestContext(['message' => $message]);

        try {
            [$request, $context] = $client->marshalRequest($context, $requestClass);
            $worker->handle($request, $context);
        } catch (\Throwable $error) {
            // Worker errors have already been rendered, reported and cleaned up by
            // Octane. Let the host retain their real identity instead of turning
            // the stopped client into a successful script return.
            if ($client->isStopped()) {
                throw $error;
            }

            $client->error($error, $worker->application(), new $requestClass(), $context);
            if (!$error instanceof HttpExceptionInterface) {
                report($error);

                throw $error;
            }
        } finally {
            unset($request, $context);
        }
    }
}
