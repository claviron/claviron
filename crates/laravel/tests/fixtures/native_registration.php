<?php
set_exception_handler(static function (Throwable $error): void {
    http_response_code(500);
    echo $error;
});
$function = new ReflectionFunction('claviron_laravel_broadcast');
$parameters = $function->getParameters();
if (array_map(fn ($parameter) => $parameter->getName(), $parameters) !== ['key', 'channels', 'event', 'data', 'socket_id']
    || $function->getNumberOfRequiredParameters() !== 4
    || (string) $function->getReturnType() !== 'void'
    || !$parameters[4]->allowsNull()
    || $parameters[4]->getDefaultValue() !== null) {
    throw new RuntimeException('Laravel function metadata changed');
}
try {
    claviron_laravel_broadcast('test-key', ['channel'], 'event', '{}');
    throw new RuntimeException('Expected an unconfigured broadcast failure');
} catch (Exception $error) {
    if (!str_contains($error->getMessage(), 'Claviron Laravel broadcast extension is not configured')) {
        throw $error;
    }
}
if (!claviron_runtime_available()) {
    throw new RuntimeException('PHP did not recover from the host exception');
}
header('X-Registration-Test: ok');
echo 'ok';
