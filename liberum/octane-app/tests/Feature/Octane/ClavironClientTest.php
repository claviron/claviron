<?php

use Claviron\Laravel\Octane\ClavironClient;
use Illuminate\Http\Request;
use Laravel\Octane\OctaneResponse;
use Laravel\Octane\RequestContext;
use Symfony\Component\HttpFoundation\Cookie;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\HttpKernel\Exception\UnsupportedMediaTypeHttpException;

function clavironMessage(array $overrides = []): array
{
    return array_replace([
        'method' => 'GET', 'uri' => 'https://octane.home/runtime',
        'protocol' => 'HTTP/2', 'headers' => [], 'body' => '',
        'server' => ['HTTPS' => 'on', 'REMOTE_ADDR' => '127.0.0.1'],
    ], $overrides);
}

test('marshals independent requests while retaining instance identity', function () {
    $client = new ClavironClient(fn (array $response) => null);
    [$first] = $client->marshalRequest(new RequestContext(['message' => clavironMessage([
        'method' => 'POST', 'uri' => 'https://octane.home/echo?q=one',
        'headers' => ['content-type' => ['application/json'], 'cookie' => ['token=a%3Db; theme=dark'], 'x-tag' => ['one', 'two']],
        'body' => '{"value":42}',
    ])]));
    [$second] = $client->marshalRequest(new RequestContext(['message' => clavironMessage()]));

    expect($first->fullUrl())->toBe('https://octane.home/echo?q=one')
        ->and($first->method())->toBe('POST')
        ->and($first->input('value'))->toBe(42)
        ->and($first->cookie('token'))->toBe('a=b')
        ->and($first->headers->all('x-tag'))->toBe(['one', 'two'])
        ->and($first->ip())->toBe('127.0.0.1')
        ->and($second->cookies->all())->toBe([])
        ->and($second->headers->has('x-tag'))->toBeFalse()
        ->and($second->getContent())->toBe('')
        ->and($second->query())->toBe([])
        ->and($second->attributes->get('claviron')['boot_id'])->toBe($first->attributes->get('claviron')['boot_id'])
        ->and($second->attributes->get('claviron')['request_count'])->toBe(2);
});

test('parses urlencoded bodies', function () {
    $client = new ClavironClient(fn (array $response) => null);
    [$request] = $client->marshalRequest(new RequestContext(['message' => clavironMessage([
        'method' => 'POST', 'headers' => ['content-type' => ['application/x-www-form-urlencoded; charset=UTF-8']],
        'body' => 'name=hello+world&items[]=a&items[]=b',
    ])]));
    expect($request->request->all())->toBe(['name' => 'hello world', 'items' => ['a', 'b']]);
});

test('rejects unsupported multipart rather than silently losing uploads', function () {
    $client = new ClavironClient(fn (array $response) => null);
    $client->marshalRequest(new RequestContext(['message' => clavironMessage([
        'headers' => ['content-type' => ['multipart/form-data; boundary=test']],
    ])]));
})->throws(UnsupportedMediaTypeHttpException::class);

test('preserves binary output and repeated response cookies', function () {
    $sent = [];
    $client = new ClavironClient(function (array $response) use (&$sent) {
        $sent[] = $response;
    });
    $response = new Response("a\0b", 201, ['Content-Length' => '999', 'Connection' => 'close', 'Transfer-Encoding' => 'chunked']);
    $response->headers->setCookie(new Cookie('one', '1'));
    $response->headers->setCookie(new Cookie('two', '2'));
    $client->respond(new RequestContext(['message' => clavironMessage()]), new OctaneResponse($response, 'prefix'));
    expect($sent)->toHaveCount(1)
        ->and($sent[0]['status'])->toBe(201)
        ->and($sent[0]['body'])->toBe("prefixa\0b")
        ->and($sent[0]['headers']['content-length'])->toBe(['9'])
        ->and($sent[0]['headers']['set-cookie'])->toHaveCount(2)
        ->and($sent[0]['headers'])->not->toHaveKeys(['connection', 'transfer-encoding']);
});

test('omits bodies for HEAD and empty statuses', function (string $method, int $status) {
    $sent = [];
    $client = new ClavironClient(function (array $response) use (&$sent) {
        $sent[] = $response;
    });
    $client->respond(new RequestContext(['message' => clavironMessage(['method' => $method])]), new OctaneResponse(new Response('body', $status), 'output'));
    expect($sent[0]['body'])->toBe('');
})->with([['HEAD', 200], ['GET', 204], ['GET', 304]]);

test('rejects streaming responses without invoking them', function () {
    $client = new ClavironClient(fn (array $response) => null);
    $client->respond(new RequestContext, new OctaneResponse(new StreamedResponse(function () {
        throw new LogicException('Must not execute');
    })));
})->throws(RuntimeException::class, 'Streaming and file responses');

test('stop is explicit and idempotent', function () {
    $client = new ClavironClient(fn (array $response) => null);
    expect($client->isStopped())->toBeFalse();
    $client->stop();
    $client->stop();
    expect($client->isStopped())->toBeTrue();
});

test('returns safe errors and preserves HTTP exception status', function () {
    config(['app.debug' => false]);
    $sent = [];
    $client = new ClavironClient(function (array $response) use (&$sent) {
        $sent[] = $response;
    });
    $context = new RequestContext(['message' => clavironMessage()]);
    $client->error(new RuntimeException('sensitive detail'), $this->app, new Request, $context);
    $client->error(new UnsupportedMediaTypeHttpException('unsupported upload'), $this->app, new Request, $context);

    expect($sent)->toHaveCount(2)
        ->and($sent[0]['status'])->toBe(500)
        ->and($sent[0]['body'])->not->toContain('sensitive detail')
        ->and($sent[1]['status'])->toBe(415)
        ->and($client->isStopped())->toBeFalse();
});
