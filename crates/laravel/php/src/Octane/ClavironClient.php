<?php

namespace Claviron\Laravel\Octane;

use Closure;
use Illuminate\Foundation\Application;
use Illuminate\Http\Request;
use Laravel\Octane\Contracts\StoppableClient;
use Laravel\Octane\Octane;
use Laravel\Octane\OctaneResponse;
use Laravel\Octane\RequestContext;
use RuntimeException;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\HttpKernel\Exception\BadRequestHttpException;
use Symfony\Component\HttpKernel\Exception\HttpExceptionInterface;
use Symfony\Component\HttpKernel\Exception\UnsupportedMediaTypeHttpException;
use Throwable;

/**
 * Octane client for the Claviron application model.
 *
 * Buffered messages in, buffered responses out, without SAPI superglobal
 * emulation: the engine hands `claviron_application_receive()` a request as a
 * PHP value and takes the reply through `claviron_application_respond()`. The
 * loop is owned by ApplicationRunner; the application's entry supplies its
 * base path and optional Request subclass.
 */
final class ClavironClient implements StoppableClient
{
    private bool $stopped = false;

    private int $requestCount = 0;

    private readonly string $bootId;

    /** @param Closure(array{status: int, headers: array<string, list<string>>, body: string}): void $send */
    public function __construct(private readonly Closure $send)
    {
        $this->bootId = bin2hex(random_bytes(8));
    }

    /**
     * @param class-string<Request> $requestClass
     * @return array{Request, RequestContext}
     */
    public function marshalRequest(RequestContext $context, string $requestClass = Request::class): array
    {
        $message = $context->data['message'];
        $headers = array_change_key_case($message['headers'], CASE_LOWER);
        $uri = parse_url($message['uri']);
        if ($uri === false) {
            throw new BadRequestHttpException('Invalid request URI.');
        }

        $contentType = strtolower(trim(explode(';', $headers['content-type'][0] ?? '')[0]));
        if (str_starts_with($contentType, 'multipart/')) {
            throw new UnsupportedMediaTypeHttpException('Multipart uploads are not supported by this buffered adapter yet.');
        }

        $queryString = $uri['query'] ?? '';
        parse_str($queryString, $query);
        $parameters = [];
        if ($contentType === 'application/x-www-form-urlencoded') {
            parse_str($message['body'], $parameters);
        }

        $cookies = [];
        foreach ($headers['cookie'] ?? [] as $cookieHeader) {
            foreach (explode(';', $cookieHeader) as $cookie) {
                $pair = explode('=', trim($cookie), 2);
                if (count($pair) === 2 && $pair[0] !== '') {
                    $cookies[$pair[0]] ??= rawurldecode($pair[1]);
                }
            }
        }

        $server = $message['server'];
        $server['REQUEST_METHOD'] = $message['method'];
        $server['REQUEST_URI'] = ($uri['path'] ?? '/').($queryString !== '' ? '?'.$queryString : '');
        $server['QUERY_STRING'] = $queryString;
        $server['SERVER_PROTOCOL'] = $message['protocol'];
        $server['SCRIPT_NAME'] = '';
        $server['SCRIPT_FILENAME'] = '';
        $server['REQUEST_TIME_FLOAT'] = microtime(true);
        $server['REQUEST_TIME'] = time();
        foreach ($headers as $name => $values) {
            $key = in_array($name, ['content-type', 'content-length'], true)
                ? strtoupper(str_replace('-', '_', $name))
                : 'HTTP_'.strtoupper(str_replace('-', '_', $name));
            $server[$key] = implode($name === 'cookie' ? '; ' : ', ', $values);
        }
        if (! isset($server['HTTP_HOST']) && isset($uri['host'])) {
            $server['HTTP_HOST'] = $uri['host'].(isset($uri['port']) ? ':'.$uri['port'] : '');
            $headers['host'] = [$server['HTTP_HOST']];
        }

        $request = new $requestClass($query, $parameters, [], $cookies, [], $server, $message['body']);
        $request->headers->replace($headers);
        $request->attributes->set('claviron', [
            'boot_id' => $this->bootId,
            'request_count' => ++$this->requestCount,
            'process_id' => getmypid(),
            'worker' => getenv('CLAVIRON_WORKER_ID') ?: null,
            'thread' => getenv('CLAVIRON_THREAD_ID') ?: null,
        ]);

        return [$request, $context];
    }

    public function respond(RequestContext $context, OctaneResponse $octaneResponse): void
    {
        $response = $octaneResponse->response;
        if ($response instanceof StreamedResponse || $response instanceof BinaryFileResponse) {
            throw new RuntimeException('Streaming and file responses require a streaming Claviron adapter.');
        }

        $empty = ($context->data['message']['method'] ?? 'GET') === 'HEAD' || $response->isEmpty();
        $body = $empty ? '' : ($octaneResponse->outputBuffer ?? '').$response->getContent();
        $headers = $response->headers->all();
        unset($headers['transfer-encoding'], $headers['connection']);
        if (! $empty) {
            $headers['content-length'] = [(string) strlen($body)];
        }

        ($this->send)([
            'status' => $response->getStatusCode(),
            'headers' => $headers,
            'body' => $body,
        ]);
    }

    public function error(Throwable $error, Application $app, Request $request, RequestContext $context): void
    {
        $status = $error instanceof HttpExceptionInterface ? $error->getStatusCode() : 500;
        $this->respond($context, new OctaneResponse(new Response(
            Octane::formatExceptionForClient($error, (bool) $app->make('config')->get('app.debug')),
            $status,
            ['Content-Type' => 'text/plain; charset=UTF-8'],
        )));
    }

    public function stop(): void
    {
        $this->stopped = true;
    }

    public function isStopped(): bool
    {
        return $this->stopped;
    }
}
