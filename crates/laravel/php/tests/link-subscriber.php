<?php

declare(strict_types=1);

// Exercise the subscriber and decoder without booting PHP applications or Orbit.
$subscriptions = [];
function claviron_events_available(): bool
{
    return true;
}
function claviron_event_subscribe_many(array $topics): void
{
    $GLOBALS['subscriptions'] = $topics;
}

require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

use Claviron\Laravel\EventBridge;
use Claviron\Laravel\Link\Events\ApplicationConnected;
use Claviron\Laravel\Link\Events\EdgeConnected;
use Claviron\Laravel\Link\Events\EdgeShutdownInitiated;
use Claviron\Laravel\Link\LinkSubscriber;
use Illuminate\Container\Container;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\Facades\Log;

$container = new Container;
Log::setFacadeApplication($container);
$logger = new class
{
    public array $entries = [];

    public function info(string $message, array $context): void
    {
        $this->entries[] = [$message, $context];
    }
};
Log::swap($logger);
$dispatcher = new Dispatcher($container);
$subscriber = new LinkSubscriber(new EventBridge($dispatcher));
$container->instance(LinkSubscriber::class, $subscriber);
$dispatcher->subscribe($subscriber);

if (! in_array(EdgeShutdownInitiated::topic(), $subscriptions, true)) {
    throw new LogicException('Shutdown notice was not subscribed alongside Link observations');
}
$payload = ['edge' => 'example-edge', 'fleet' => 'example', 'application' => 'example-app'];
$event = EdgeShutdownInitiated::fromPayload($payload);
if ($event->toPayload(0) !== $payload) {
    throw new LogicException('Shutdown notice lost its identity');
}
$dispatcher->dispatch($event);
if ($logger->entries !== [['claviron link: edge shutdown initiated', $payload]]) {
    throw new LogicException('Shutdown notice did not reach the existing Link subscriber log');
}

foreach ([EdgeConnected::topic(), ApplicationConnected::topic()] as $topic) {
    if (! in_array($topic, $subscriptions, true)) {
        throw new LogicException('Direct connections must be observed alongside shutdown');
    }
}
foreach (['claviron.transponder.query.v1', 'claviron.transponder.reply.v1'] as $topic) {
    if (in_array($topic, $subscriptions, true)) {
        throw new LogicException('Link observer must not subscribe to other processes introductions');
    }
}
$edgePayload = ['name' => 'example-edge', 'fleet' => 'example', 'role' => 'edge', 'pid' => 1234, 'lane' => 3, 'application' => 'example-app'];
$applicationPayload = ['name' => 'example-app', 'fleet' => 'example', 'role' => 'php', 'pid' => null, 'lane' => 4, 'edge' => 'example-edge'];
$edge = EdgeConnected::fromPayload($edgePayload);
$application = ApplicationConnected::fromPayload($applicationPayload);
if ($edge->toPayload(0) !== $edgePayload || $application->toPayload(0) !== $applicationPayload) {
    throw new LogicException('Direct connection observation lost its lane identity');
}
$logger->entries = [];
$dispatcher->dispatch($edge);
$dispatcher->dispatch($application);
if ($logger->entries != [
    ['claviron link: edge connected', $edgePayload],
    ['claviron link: application connected', $applicationPayload],
]) {
    throw new LogicException('Each direct connection must produce one observer log');
}

echo "Link local connection, shutdown subscription, payload and log passed.\n";
