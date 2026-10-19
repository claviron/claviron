<?php

header('Content-Type: application/json');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('Pragma: no-cache');
header('Expires: 0');

// SAPI-agnostic header collection. `getallheaders()` is registered only by
// mod_apache2handler / php-fpm / LiteSpeed SAPIs — CLI and embed (claviron
// runs PHP via ext-php-rs::embed) don't expose it. Reconstruct from the
// HTTP_* CGI vars that every SAPI populates in $_SERVER.
$headers = [];
foreach ($_SERVER as $key => $value) {
    if (strncmp($key, 'HTTP_', 5) !== 0) {
        continue;
    }
    $name = str_replace(' ', '-', ucwords(strtolower(str_replace('_', ' ', substr($key, 5)))));
    $headers[$name] = $value;
}

$server = [
    'server_name' => $_SERVER['SERVER_NAME'] ?? null,
    'server_addr' => $_SERVER['SERVER_ADDR'] ?? null,
    'server_port' => $_SERVER['SERVER_PORT'] ?? null,
    'protocol' => $_SERVER['SERVER_PROTOCOL'] ?? null,
    'https' => $_SERVER['HTTPS'] ?? null,
    'document_root' => $_SERVER['DOCUMENT_ROOT'] ?? null,
    'script_name' => $_SERVER['SCRIPT_NAME'] ?? null,
    'script_filename' => $_SERVER['SCRIPT_FILENAME'] ?? null,
    'request_uri' => $_SERVER['REQUEST_URI'] ?? null,
    'query_string' => $_SERVER['QUERY_STRING'] ?? null,
    'remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null,
    'http_host' => $_SERVER['HTTP_HOST'] ?? null,
    'x_forwarded_for' => $_SERVER['HTTP_X_FORWARDED_FOR'] ?? $headers['x-forwarded-for'] ?? null,
    'x_forwarded_proto' => $_SERVER['HTTP_X_FORWARDED_PROTO'] ?? $headers['x-forwarded-proto'] ?? null,
];

$client = [
    'ip' => $_SERVER['REMOTE_ADDR'] ?? null,
    'host' => $_SERVER['HTTP_HOST'] ?? null,
    'port' => $_SERVER['REMOTE_PORT'] ?? null,
    'scheme' => (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off') ? 'https' : 'http',
    'method' => $_SERVER['REQUEST_METHOD'] ?? null,
    'url' => $_SERVER['REQUEST_URI'] ?? null,
    'user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? null,
    'accept' => $headers['Accept'] ?? null,
    'accept_language' => $headers['Accept-Language'] ?? null,
    'referer' => $headers['Referer'] ?? null,
    'origin' => $headers['Origin'] ?? null,
];


$cookies = $_COOKIE;
$env_vars = $_ENV;


$opcacheStatus = null;
if (function_exists('opcache_get_status')) {
    $status = opcache_get_status(false);
    if (is_array($status)) {
        $memoryUsage = $status['memory_usage'] ?? [];
        $statistics = $status['opcache_statistics'] ?? [];
        $jit = $status['jit'] ?? [];
        $opcacheStatus = [
            'available' => true,
            'enabled' => $status['opcache_enabled'] ?? null,
            'cache_full' => $status['cache_full'] ?? null,
            'restart_pending' => $status['restart_pending'] ?? null,
            'restart_in_progress' => $status['restart_in_progress'] ?? null,
            'cached_scripts' => $statistics['num_cached_scripts'] ?? null,
            'cached_keys' => $statistics['num_cached_keys'] ?? null,
            'max_cached_keys' => $statistics['max_cached_keys'] ?? null,
            'hits' => $statistics['hits'] ?? null,
            'misses' => $statistics['misses'] ?? null,
            'oom_restarts' => $statistics['oom_restarts'] ?? null,
            'hash_restarts' => $statistics['hash_restarts'] ?? null,
            'manual_restarts' => $statistics['manual_restarts'] ?? null,
            'used_memory' => $memoryUsage['used_memory'] ?? null,
            'free_memory' => $memoryUsage['free_memory'] ?? null,
            'wasted_memory' => $memoryUsage['wasted_memory'] ?? null,
            'current_wasted_percentage' => $memoryUsage['current_wasted_percentage'] ?? null,
            'jit' => [
                'enabled' => $jit['enabled'] ?? null,
                'on' => $jit['on'] ?? null,
                'kind' => $jit['kind'] ?? null,
                'opt_level' => $jit['opt_level'] ?? null,
                'opt_flags' => $jit['opt_flags'] ?? null,
                'buffer_size' => $jit['buffer_size'] ?? null,
                'buffer_free' => $jit['buffer_free'] ?? null,
            ],
        ];
    } else {
        $opcacheStatus = [
            'available' => true,
            'enabled' => false,
        ];
    }
} else {
    $opcacheStatus = [
        'available' => false,
        'enabled' => false,
    ];
}

$apcuStatus = [
    'available' => function_exists('apcu_enabled'),
    'enabled' => function_exists('apcu_enabled') ? apcu_enabled() : false,
    'cache_info' => null,
    'sma_info' => null,
];

if ($apcuStatus['available'] && $apcuStatus['enabled']) {
    $cacheInfo = @apcu_cache_info(false);
    $smaInfo = @apcu_sma_info(false);

    $apcuStatus['cache_info'] = is_array($cacheInfo) ? [
        'num_slots' => $cacheInfo['num_slots'] ?? null,
        'ttl' => $cacheInfo['ttl'] ?? null,
        'num_hits' => $cacheInfo['num_hits'] ?? null,
        'num_misses' => $cacheInfo['num_misses'] ?? null,
        'num_inserts' => $cacheInfo['num_inserts'] ?? null,
        'num_entries' => $cacheInfo['num_entries'] ?? null,
        'expunges' => $cacheInfo['expunges'] ?? null,
        'start_time' => $cacheInfo['start_time'] ?? null,
        'mem_size' => $cacheInfo['mem_size'] ?? null,
    ] : null;

    $apcuStatus['sma_info'] = is_array($smaInfo) ? [
        'num_seg' => $smaInfo['num_seg'] ?? null,
        'seg_size' => $smaInfo['seg_size'] ?? null,
        'avail_mem' => $smaInfo['avail_mem'] ?? null,
    ] : null;
}

$redisStatus = [
    'extension_loaded' => extension_loaded('redis'),
    'class_available' => class_exists('Redis'),
    'session_handler' => ini_get('session.save_handler') ?: null,
    'session_path' => ini_get('session.save_path') ?: null,
    'default_socket_timeout' => ini_get('default_socket_timeout') ?: null,
];

$runtime = [
    'sapi_name' => php_sapi_name(),
    'php_version' => PHP_VERSION,
    'php_version_id' => PHP_VERSION_ID,
    'zts' => (bool)PHP_ZTS,
    'debug' => defined('ZEND_DEBUG_BUILD') ? (bool)ZEND_DEBUG_BUILD : null,
    'claviron_is_cancelled_available' => function_exists('claviron_is_cancelled'),
    'claviron_is_cancelled' => function_exists('claviron_is_cancelled')
        ? claviron_is_cancelled()
        : null,
];
// $extensions = array_map('strtolower', get_loaded_extensions());
echo json_encode([
    'php' => phpversion(),
    'config' => php_ini_loaded_file(),
    'sapi' => php_sapi_name(),
    'runtime' => $runtime,
    'loaded_extensions' => get_loaded_extensions(),
//     'opcache' => $opcacheStatus,
    'apcu' => $apcuStatus,
    'redis' => $redisStatus,
    'server' => $server,
    'client' => $client,
    'headers' => $headers,
    'environment' => $env_vars,
    'cookies' => $cookies,
], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
