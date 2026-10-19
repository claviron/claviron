<?php
header('Content-Type: application/json');
echo json_encode([
    'php_pid' => getmypid(),                                // worker process PID
    'sapi_thread_id' => $_SERVER['CLAVIRON_THREAD_ID'] ?? null,
    'zend_thread_id' => function_exists('zend_thread_id')
        ? zend_thread_id()
        : null,
    'claviron_runner' => $_SERVER['CLAVIRON_RUNNER'] ?? null,
    'claviron_php_runner' => $_SERVER['CLAVIRON_PHP_RUNNER'] ?? null,
    'claviron_hostname' => $_SERVER['CLAVIRON_HOSTNAME'] ?? null,
    'claviron_is_cancelled' => claviron_is_cancelled(),
    'claviron_sapi_stats' => function_exists('claviron_sapi_stats')
        ? claviron_sapi_stats()
        : null,
    'claviron_app_name' => claviron_app_name(),
], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
