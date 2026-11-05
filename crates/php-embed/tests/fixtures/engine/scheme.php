<?php
if (getenv('CLAVIRON_PHP_RUNNER') !== 'script' || ($_SERVER['CLAVIRON_PHP_RUNNER'] ?? null) !== 'script') {
    throw new Exception('direct-engine request lost configured runner');
}
echo implode('|', array_map(fn($key) => $_SERVER[$key] ?? '-', ['REQUEST_SCHEME', 'HTTPS', 'HTTP_X_FORWARDED_PROTO', 'HTTP_FORWARDED', 'SSL_CLIENT_VERIFY', 'REQUEST_URI', 'REMOTE_ADDR', 'REMOTE_PORT', 'SERVER_ADDR', 'SERVER_PORT', 'CONTENT_LENGTH', 'PHP_SELF', 'HTTP_X_SAMPLE_NAME']));
