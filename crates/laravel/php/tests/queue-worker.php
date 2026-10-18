<?php

declare(strict_types=1);

// Supply an installed Laravel application's autoloader; never consume real jobs.
require $argv[1];

use Claviron\Laravel\Queue\Worker;
use Illuminate\Queue\Worker as LaravelWorker;
use Illuminate\Queue\WorkerOptions;

$cancelled = false;
function claviron_is_cancelled(): bool
{
    return $GLOBALS['cancelled'];
}

$native = new ReflectionClass(Worker::class);
$parent = new ReflectionClass(LaravelWorker::class);
$method = $parent->getMethod('stopIfNecessary');
$adapted = $native->getMethod('stopIfNecessary');

foreach ([false, true] as $empty) {
    $options = new WorkerOptions();
    $options->memory = 0;
    $options->stopWhenEmpty = $empty;
    $options->maxJobs = 2;
    $values = [
        'options' => $options,
        'lastRestart' => null,
        'startTime' => time(),
        'jobsProcessed' => 2,
        'job' => null,
        'lastJobProcessedAt' => time(),
    ];
    $arguments = array_map(static fn (ReflectionParameter $parameter) => $values[$parameter->name], $method->getParameters());
    $expected = $method->invokeArgs($parent->newInstanceWithoutConstructor(), $arguments);
    $actual = $adapted->invokeArgs($native->newInstanceWithoutConstructor(), $arguments);
    if ($actual !== $expected) {
        throw new RuntimeException('Adapter changed Laravel stop policy or argument positions.');
    }
}

$cancelled = true;
$worker = $native->newInstanceWithoutConstructor();
$adapted->invokeArgs($worker, $arguments);
if (!$worker->shouldQuit || $native->getMethod('supportsAsyncSignals')->invoke($worker)) {
    throw new RuntimeException('Native cancellation or process-signal isolation failed.');
}

echo "Queue adapter loaded: Laravel stop-policy arguments, cancellation and signal isolation verified.\n";
