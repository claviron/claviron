<?php
require __DIR__ . '/identity.php';
// The console belongs to programs. A request is an HTTP request: it has no
// terminal, and STDOUT here would be the worker's, not the client's.
if (isset($_SERVER['CLAVIRON_PHP_MODEL'])) { throw new Exception('request carries a program model'); }
if (defined('STDOUT') || defined('STDERR')) { throw new Exception('request grew a console'); }
if (getenv('CLAVIRON_PHP_RUNNER') !== 'script' || ($_SERVER['CLAVIRON_PHP_RUNNER'] ?? null) !== 'script') {
    throw new Exception('booted request lost configured runner');
}
if (!claviron_event_subscribe('request.loopback')) {
    throw new Exception('request event subscription was not created');
}
$payload = [
    'source' => 'request',
    'id' => 7,
    'ratio' => 1.5,
    'ready' => true,
    'missing' => null,
    3 => "\0\xff",
];
claviron_event_publish('request.loopback', $payload);
$event = claviron_event_receive();
if (!$event instanceof Claviron\Runtime\Event
    || $event->topic !== 'request.loopback'
    || $event->payload !== $payload) {
    throw new Exception('request event loopback failed');
}
echo getenv('PROGRAM_VALUE') === false && getenv('CLAVIRON_PHP_PROGRAM') === false && !claviron_is_cancelled()
    ? 'request-ok'
    : 'contaminated';
