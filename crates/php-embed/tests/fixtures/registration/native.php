<?php
set_exception_handler(static function (Throwable $error): void {
    http_response_code(500);
    echo $error;
});
function require_same($expected, $actual): void
{
    if ($expected !== $actual) {
        throw new RuntimeException(var_export([$expected, $actual], true));
    }
}
require_same(true, claviron_runtime_available());
require_same('php-extension-test', claviron_app_name());
require_same(true, claviron_is_on_http());
require_same(false, claviron_is_cancelled());
require_same(null, claviron_application_name());

if (function_exists('claviron_postgres_connection')) {
    $disconnect = new ReflectionFunction('claviron_postgres_disconnect');
    require_same(1, $disconnect->getNumberOfRequiredParameters());
    require_same('Claviron\\Runtime\\PostgresConnection', (string) $disconnect->getParameters()[0]->getType());
    require_same('bool', (string) $disconnect->getReturnType());
}

// Traverse names, types and defaults after registration's Rust locals are gone.
foreach (get_defined_functions()['internal'] as $name) {
    if (!str_starts_with($name, 'claviron_') && !str_starts_with($name, 'orbit_')) {
        continue;
    }
    $function = new ReflectionFunction($name);
    foreach ($function->getParameters() as $parameter) {
        require_same(true, $parameter->getName() !== '');
        if ($parameter->isDefaultValueAvailable()) {
            $parameter->getDefaultValue();
        }
        (string) $parameter->getType();
    }
    (string) $function->getReturnType();
}
$class = new ReflectionClass('Claviron\Runtime\Event');
foreach ($class->getMethods() as $method) {
    foreach ($method->getParameters() as $parameter) {
        require_same(true, $parameter->getName() !== '');
    }
}
if (class_exists('Claviron\Runtime\Invocation', false)) {
    require_same(true, property_exists('Claviron\Runtime\Invocation', 'submitted_at'));
    require_same(false, property_exists('Claviron\Runtime\Invocation', 'submittedAt'));
}
header('X-Registration-Test: ok');
echo 'ok';
