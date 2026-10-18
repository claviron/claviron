<?php
declare(strict_types=1);
if (getenv('CLAVIRON_PHP_RUNNER') !== false || isset($_SERVER['CLAVIRON_PHP_RUNNER'])) {
    throw new Exception('persistent application inherited HTTP route runner');
}
require __DIR__ . '/runtime-guard.php';
require __DIR__ . '/runtime-guard.php';
if (!claviron_runtime_available()) { throw new Exception('native runtime marker missing'); }
if (claviron_app_name() !== 'application-lifecycle') { throw new Exception('bad runtime application name'); }
foreach (['claviron_runtime_available', 'claviron_app_name', 'claviron_application_receive', 'claviron_is_cancelled', 'claviron_event_subscribe', 'claviron_event_subscribe_many', 'claviron_event_publish', 'claviron_event_receive'] as $function) {
    if (!(new ReflectionFunction($function))->isInternal()) { throw new Exception('stub replaced native function'); }
}
if (!(new ReflectionClass(\Claviron\Runtime\Event::class))->isInternal()) { throw new Exception('stub replaced native event class'); }
$boot = bin2hex(random_bytes(8));
$count = 0;
if (!str_starts_with(getenv('CLAVIRON_THREAD_ID'), 'application:')) { throw new Exception('bad application identity'); }
if (getenv('APP_ENV') !== 'application-test' || $argv[1] !== 'literal argument') { throw new Exception('bad boot environment/args'); }
if (claviron_event_subscribe_many(['application.test']) !== 1) { throw new Exception('bad application event subscription'); }
while (($request = claviron_application_receive()) !== null) {
    if ($request instanceof \Claviron\Runtime\Event) {
        if ($request->topic !== 'application.test' || $request->payload !== ['ready' => true]) {
            throw new Exception('bad native application event');
        }
        file_put_contents(getenv('TEST_DIR').'/event-received', 'yes');
        continue;
    }
    ++$count;
    if ($request['uri'] === '/exit') { exit(1); }
    if ($request['uri'] === '/slow') { usleep(200000); }
    if ($request['uri'] === '/held') {
        file_put_contents(getenv('TEST_DIR').'/entered', 'yes');
        while (!file_exists(getenv('TEST_DIR').'/release')) { usleep(1000); }
    }
    if ($request['uri'] === '/invalid') {
        claviron_application_respond(['status' => 200, 'headers' => ['bad' => ["bad\r\nvalue"]], 'body' => '']);
    }
    if ($request['uri'] === '/stream') {
        claviron_application_stream(['status' => 200, 'headers' => ['content-type' => ['text/event-stream']]]);
        foreach (['one', 'two', 'three'] as $piece) {
            if (!claviron_application_write("data: {$piece}\n\n")) { break; }
        }
        claviron_application_end();
        continue;
    }
    if ($request['uri'] === '/publish') {
        claviron_event_publish('application.published', ['source' => 'php']);
    }
    claviron_application_respond([
        'status' => 200,
        'headers' => ['x-boot' => [$boot], 'x-count' => [(string)$count], 'set-cookie' => ['a=1', 'b=2'], 'x-scheme' => [$request['server']['REQUEST_SCHEME'] ?? 'none'], 'x-https' => [$request['server']['HTTPS'] ?? 'none']],
        'body' => $request['uri'].':'.$request['body'],
    ]);
}
