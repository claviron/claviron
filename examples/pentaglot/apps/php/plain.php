<?php
declare(strict_types=1);

$opcache = function_exists('opcache_get_status') ? opcache_get_status(false) : false;
file_put_contents('php://stderr', 'Pentaglot plain PHP startup: '.json_encode([
    'pid' => getmypid(), 'instance' => getenv('CLAVIRON_THREAD_ID'),
    'native_runtime' => claviron_runtime_available(),
    'opcache_enabled' => $opcache !== false && $opcache['opcache_enabled'],
    'validate_timestamps' => (bool) ini_get('opcache.validate_timestamps'),
    'xdebug_loaded' => extension_loaded('xdebug'),
    'symfony_loaded' => class_exists(Symfony\Component\HttpKernel\Kernel::class, false),
], JSON_THROW_ON_ERROR)."\n");

$requests = 0;
while (($request = claviron_application_receive()) !== null) {
    $path = parse_url($request['uri'], PHP_URL_PATH);
    $method = $request['method'];
    $allowed = match ($path) {
        '/php' => 'GET',
        '/php/echo' => 'POST',
        default => null,
    };
    $status = $allowed === null ? 404 : ($method === $allowed ? 200 : 405);
    $headers = ['content-type' => ['application/json'], 'cache-control' => ['no-cache, private']];
    if ($status === 405) {
        $headers['allow'] = [$allowed];
    }
    claviron_application_respond([
        'status' => $status,
        'headers' => $headers,
        'body' => json_encode([
            'runtime' => 'php', 'engine' => 'PHP / Claviron PHP embed',
            'method' => $method, 'path' => $path,
            'body' => $request['body'], 'requests' => ++$requests,
        ], JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_AMP | JSON_HEX_QUOT | JSON_THROW_ON_ERROR),
    ]);
    unset($request);
}
