<?php

use Lumenize\EventBridge;
use Lumenize\Events\FleetEvent;
use Lumenize\Octane\ClavironClient;
use Lumenize\Octane\ClavironWorker;
use Claviron\Runtime\Event as RuntimeEvent;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Laravel\Octane\ApplicationFactory;
use Laravel\Octane\CurrentApplication;
use Laravel\Octane\RequestContext;

test('boots once and resets request scoped state across real Octane requests', function () {
    $original = $this->app;
    $sent = [];
    $client = new ClavironClient(function (array $response) use (&$sent) {
        $sent[] = $response;
    });
    $worker = new ClavironWorker(new ApplicationFactory(base_path()), $client);
    try {
        $worker->boot();
        $app = $worker->application();
        $app->scoped('claviron.test.scope', fn () => new stdClass);
        $app->make('router')->get('/claviron-test', function (Request $request) {
            $scope = app('claviron.test.scope');
            $previous = $scope->value ?? null;
            $scope->value = $request->query('value');

            return response()->json([
                'previous' => $previous, 'value' => $scope->value,
                'header' => $request->header('x-probe'),
                'runtime' => $request->attributes->get('claviron'),
            ]);
        });
        foreach (['one', 'two'] as $value) {
            $context = new RequestContext(['message' => [
                'method' => 'GET', 'uri' => 'https://octane.home/claviron-test?value='.$value,
                'protocol' => 'HTTP/2', 'headers' => ['x-probe' => [$value]], 'body' => '',
                'server' => ['HTTPS' => 'on', 'REMOTE_ADDR' => '127.0.0.1'],
            ]]);
            [$request] = $client->marshalRequest($context);
            $worker->handle($request, $context);
        }
        expect($sent)->toHaveCount(2);
        $first = json_decode($sent[0]['body'], true, flags: JSON_THROW_ON_ERROR);
        $second = json_decode($sent[1]['body'], true, flags: JSON_THROW_ON_ERROR);
        expect($sent[0]['status'])->toBe(200)
            ->and($sent[1]['status'])->toBe(200)
            ->and($first['value'])->toBe('one')
            ->and($second['value'])->toBe('two')
            ->and($second['header'])->toBe('two')
            ->and($second['previous'])->toBeNull()
            ->and($second['runtime']['boot_id'])->toBe($first['runtime']['boot_id'])
            ->and($second['runtime']['request_count'])->toBe(2)
            ->and($client->isStopped())->toBeFalse();

        // A Claviron event arrives between requests, after Octane flushed the
        // previous request sandbox. Its listeners still need a live config and
        // log manager, and must not stop the persistent application.
        // The runtime creates inbound envelopes; their constructor is private.
        // Hydrate the published PHP declaration only as a local test fixture.
        $runtimeEvent = static function (string $id, string $topic, mixed $payload): RuntimeEvent {
            $type = new ReflectionClass(RuntimeEvent::class);
            $event = $type->newInstanceWithoutConstructor();

            foreach (compact('id', 'topic', 'payload') as $property => $value) {
                $type->getProperty($property)->setValue($event, $value);
            }

            return $event;
        };
        $eventType = new class([]) extends FleetEvent
        {
            public function __construct(public readonly array $payload)
            {
            }

            public static function topic(): string
            {
                return 'octane_test_inbound';
            }

            public static function fromPayload(array $payload): static
            {
                return new static($payload);
            }

        };
        $eventClass = $eventType::class;
        $eventConfig = null;
        $app->make(EventBridge::class)->subscribe([$eventClass]);
        $app->make(Dispatcher::class)->listen(
            $eventClass,
            function () use (&$eventConfig): void {
                Log::info('Claviron event sandbox probe');
                $eventConfig = config('app.name');
                throw new RuntimeException('expected event listener failure');
            },
        );
        $worker->handleEvent($runtimeEvent(
            '018f6f52-8fd2-7dd9-9b6a-67780173e69a',
            $eventClass::topic(),
            ['probe' => true],
        ));

        expect($eventConfig)->toBe(config('app.name'))
            ->and($client->isStopped())->toBeFalse();

        // A native Laravel event needs no Claviron interface or codec. Its
        // class is the ApplicationEvent topic and PHP serialization is the
        // one generic Laravel-to-Laravel payload adapter.
        $serializedEvent = null;
        $serializedEventId = null;
        $app->make(EventBridge::class)->subscribe([stdClass::class]);
        $app->make(Dispatcher::class)->listen(
            stdClass::class,
            function (stdClass $event) use ($app, &$serializedEvent, &$serializedEventId): void {
                $serializedEvent = $event->value;
                $serializedEventId = $app->make(EventBridge::class)->currentEventId();
            },
        );
        $worker->handleEvent($runtimeEvent(
            '018f6f52-8fd2-7dd9-9b6a-67780173e69b',
            stdClass::class,
            serialize((object) ['value' => 'native-laravel-event']),
        ));

        expect($serializedEvent)->toBe('native-laravel-event')
            ->and($serializedEventId)->toBe('018f6f52-8fd2-7dd9-9b6a-67780173e69b')
            ->and($app->make(EventBridge::class)->currentEventId())->toBeNull();
    } finally {
        if ($worker->application()) {
            $worker->terminate();
        }
        CurrentApplication::set($original);
    }
});
