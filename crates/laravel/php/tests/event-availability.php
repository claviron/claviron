<?php

declare(strict_types=1);

// Isolated bridge test; native service readiness is distinct from engine presence.
$ready = false;
$subscriptions = [];
function claviron_runtime_available(): bool { return true; }
function claviron_events_available(): bool { return $GLOBALS['ready']; }
function claviron_event_subscribe_many(array $topics): void { $GLOBALS['subscriptions'][] = $topics; }

require $argv[1] ?? throw new LogicException('Supply a Composer autoloader');

$bridge = new \Claviron\Laravel\EventBridge(new \Illuminate\Events\Dispatcher());
if ($bridge::available()) { throw new LogicException('An engine without events was reported ready'); }
$bridge->subscribe([\stdClass::class]);
if ($subscriptions !== []) { throw new LogicException('Unprepared event bus was called'); }
if ($bridge->publish(new \stdClass())) { throw new LogicException('Unprepared bus claimed publication'); }

$ready = true;
if (! $bridge::available()) { throw new LogicException('Prepared event bus was not reported ready'); }
$bridge->subscribe([\stdClass::class, \stdClass::class]);
if ($subscriptions !== [[\stdClass::class]]) { throw new LogicException('Prepared subscription was missing or duplicated'); }

echo "Event readiness contract passed: detached engine skips the bus; prepared bus subscribes.\n";
