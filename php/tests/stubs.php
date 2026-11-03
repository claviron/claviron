<?php

declare(strict_types=1);

// Run from the repository root: php -n php/sapi/tests/stubs.php
$stub = dirname(__DIR__) . '/fallback/functions.php';
require dirname(__DIR__) . '/fallback/types.php';
require dirname(__DIR__) . '/fallback/types.php';
require $stub;
require $stub; // Includes must be idempotent, not only require_once.

if (!class_exists(\Claviron\Runtime\Event::class, false)) {
    throw new RuntimeException('Event fallback is missing.');
}

if (claviron_runtime_available() !== false) {
    throw new RuntimeException('The fallback must not claim an embedded runtime.');
}

if (claviron_events_available() !== false) {
    throw new RuntimeException('The fallback must not claim a prepared event bus.');
}

if (claviron_runtime_configured() !== false) {
    throw new RuntimeException('The fallback must not claim prepared host bindings.');
}

if (claviron_application_name() !== null) {
    throw new RuntimeException('The fallback must not claim a named application.');
}

$names = [];
$native = dirname(__DIR__, 3) . '/crates/php-embed/src';
$sources = [
    new SplFileInfo($native . '/functions.rs'),
    ...iterator_to_array(new RecursiveIteratorIterator(new RecursiveDirectoryIterator($native . '/functions'))),
];
foreach ($sources as $source) {
    if ($source->getExtension() !== 'rs') {
        continue;
    }
    preg_match_all('/FunctionBuilder::new\(\s*"([^"]+)"/', file_get_contents($source->getPathname()), $matches);
    array_push($names, ...$matches[1]);
}
preg_match_all('/function (\w+)\(/', file_get_contents($stub), $matches);
$stubNames = array_values(array_filter($matches[1], fn (string $name): bool => !str_starts_with($name, '__')));
sort($names);
sort($stubNames);
if ($names !== $stubNames) {
    throw new RuntimeException('Stub coverage differs from native function registrations.');
}

// The handle is opaque. Reflection creates only a test argument, no native resource.
$cache = (new ReflectionClass(Claviron\Runtime\CacheHandle::class))->newInstanceWithoutConstructor();
$postgres = (new ReflectionClass(Claviron\Runtime\PostgresConnection::class))->newInstanceWithoutConstructor();
$mysql = (new ReflectionClass(Claviron\Runtime\MySqlConnection::class))->newInstanceWithoutConstructor();
$sqlite = (new ReflectionClass(Claviron\Runtime\SqliteConnection::class))->newInstanceWithoutConstructor();
$cursor = (new ReflectionClass(Claviron\Runtime\PostgresCursor::class))->newInstanceWithoutConstructor();
foreach ($names as $name) {
    if (in_array($name, ['claviron_runtime_available', 'claviron_runtime_configured', 'claviron_events_available', 'claviron_application_name'], true)) {
        continue;
    }
    $function = new ReflectionFunction($name);
    $args = [];
    foreach ($function->getParameters() as $parameter) {
        $args[] = $parameter->isDefaultValueAvailable() ? $parameter->getDefaultValue() : match ((string) $parameter->getType()) {
            'Claviron\Runtime\CacheHandle' => $cache,
            'Claviron\Runtime\PostgresConnection' => $postgres,
            'Claviron\Runtime\MySqlConnection' => $mysql,
            'Claviron\Runtime\SqliteConnection' => $sqlite,
            'Claviron\Runtime\PostgresCursor' => $cursor,
            'string' => 'test',
            'int' => 1,
            'array' => [],
            'mixed' => null,
        };
    }
    try {
        $function->invokeArgs($args);
    } catch (RuntimeException $error) {
        if (!str_contains($error->getMessage(), $name . '() requires ')) {
            throw $error;
        }
        continue;
    }
    throw new RuntimeException($name . ' must throw instead of pretending to work.');
}
echo 'All ' . count($names) . " native function declarations covered; fallback checks passed.\n";
