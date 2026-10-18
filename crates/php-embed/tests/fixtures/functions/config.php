<?php

declare(strict_types=1);

function check(bool $condition): void
{
    if (!$condition) {
        throw new RuntimeException('Native config contract failed.');
    }
}

$function = new ReflectionFunction('claviron_config');
check($function->getNumberOfRequiredParameters() === 1);
check($function->getParameters()[1]->getDefaultValue() === null);
check(claviron_config('example.enabled', true) === false);
check(claviron_config('example.count', 99) === 0);
check(claviron_config('example.optional', 'default') === null);
check(claviron_config('example.list.0') === 'first');
check(claviron_config('example.list') === ['first']);
check(claviron_config('example.secret', 'hidden') === 'hidden');
check(claviron_config('unknown.value') === null);
check(claviron_config('unknown.value', ['fallback']) === ['fallback']);
$object = new stdClass;
check(claviron_config('unknown.value', $object) === $object);
check(claviron_config('example.binary') === "\0\xff");
check(claviron_config('example.integer_map') === [-1 => 'negative']);

$array = claviron_config('example.list');
$array[0] = 'changed in PHP';
check(claviron_config('example.list.0') === 'first');

foreach (['example..count', 'example.overflow'] as $key) {
    try {
        claviron_config($key, 'must not hide error');
    } catch (Throwable $error) {
        check(str_contains($error->getMessage(), 'Claviron config lookup failed'));
        continue;
    }
    throw new RuntimeException('Native config error became a default.');
}

return claviron_config('example.generation');
