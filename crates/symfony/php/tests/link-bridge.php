<?php

declare(strict_types=1);

use Claviron\Runtime\Event;
use Claviron\Symfony\Link\Events\ApplicationConnected;
use Claviron\Symfony\Link\Events\ApplicationLeft;
use Claviron\Symfony\Link\Events\EdgeConnected;
use Claviron\Symfony\Link\Events\EdgeLeft;
use Claviron\Symfony\Link\LinkBridge;
use Symfony\Component\EventDispatcher\EventDispatcher;

require $argv[1] ?? throw new InvalidArgumentException('Pass an installed Symfony autoloader.');

$dispatcher = new EventDispatcher;
$received = [];
$dispatcher->addListener(EdgeConnected::class, static function (EdgeConnected $event) use (&$received): void {
    $received[] = $event;
});
$dispatcher->addListener(ApplicationLeft::class, static function (ApplicationLeft $event) use (&$received): void {
    $received[] = $event;
});
$dispatcher->addListener(ApplicationConnected::class, static function (ApplicationConnected $event) use (&$received): void {
    $received[] = $event;
});
$dispatcher->addListener(EdgeLeft::class, static function (EdgeLeft $event) use (&$received): void {
    $received[] = $event;
});
$bridge = LinkBridge::forDispatcher($dispatcher);

// The SAPI fallback has a private constructor because native PHP owns event
// creation. Populate it only in this isolated decoder test.
$reflection = new ReflectionClass(Event::class);
$frame = static function (string $topic, mixed $payload) use ($reflection): Event {
    $event = $reflection->newInstanceWithoutConstructor();
    $reflection->getProperty('id')->setValue($event, 'example-id');
    $reflection->getProperty('topic')->setValue($event, $topic);
    $reflection->getProperty('payload')->setValue($event, $payload);

    return $event;
};

function check(bool $condition): void
{
    if (! $condition) {
        throw new RuntimeException('Symfony link event contract failed.');
    }
}

check($bridge->deliver($frame(EdgeConnected::TOPIC, [
    'name' => 'example-edge', 'fleet' => 'example', 'role' => 'edge',
    'pid' => 42, 'application' => 'example-app', 'lane' => 3,
])));
check($bridge->deliver($frame(ApplicationLeft::TOPIC, [
    'name' => 'example-app', 'fleet' => 'example', 'edge' => 'example-edge',
])));
check($bridge->deliver($frame(ApplicationConnected::TOPIC, [
    'name' => 'example-app', 'fleet' => 'example', 'role' => 'php',
    'pid' => null, 'edge' => 'example-edge', 'lane' => 4,
])));
check($bridge->deliver($frame(EdgeLeft::TOPIC, [
    'name' => 'example-edge', 'fleet' => 'example', 'application' => 'example-app',
])));
check(count($received) === 4);
check($received[0] instanceof EdgeConnected && $received[0]->application === 'example-app');
check($received[1] instanceof ApplicationLeft && $received[1]->edge === 'example-edge');
check($received[2] instanceof ApplicationConnected && $received[2]->edge === 'example-edge' && $received[2]->pid === null && $received[2]->lane === 4);
check($received[3] instanceof EdgeLeft && $received[3]->application === 'example-app');
check(! $bridge->deliver($frame('example.other', [])));
check(! $bridge->deliver($frame('claviron.transponder.query.v1', [])));
check(! $bridge->deliver($frame('claviron.transponder.reply.v1', [])));

try {
    $bridge->deliver($frame(EdgeConnected::TOPIC, ['name' => 7]));
    throw new LogicException('Expected malformed link event rejection.');
} catch (UnexpectedValueException) {
}

echo "Symfony link events decoded and dispatched.\n";
