<?php
declare(strict_types=1);

$autoload = require __DIR__.'/vendor/autoload.php';

$kernel = new Pentaglot\Php\Kernel('prod', false);
$kernel->boot();
$opcache = function_exists('opcache_get_status') ? opcache_get_status(false) : false;
file_put_contents('php://stderr', 'Pentaglot Symfony startup: '.json_encode([
    'pid' => getmypid(), 'environment' => $kernel->getEnvironment(), 'debug' => $kernel->isDebug(),
    'symfony_version' => Symfony\Component\HttpKernel\Kernel::VERSION,
    'authoritative_autoloader' => $autoload->isClassMapAuthoritative(),
    'opcache_enabled' => $opcache !== false && $opcache['opcache_enabled'],
    'opcache_memory_mb' => (int) ini_get('opcache.memory_consumption'),
    'opcache_max_files' => (int) ini_get('opcache.max_accelerated_files'),
    'opcache_interned_strings_mb' => (int) ini_get('opcache.interned_strings_buffer'),
    'validate_timestamps' => (bool) ini_get('opcache.validate_timestamps'),
    'xdebug_loaded' => extension_loaded('xdebug'),
], JSON_THROW_ON_ERROR)."\n");
Claviron\Symfony\ApplicationRunner::run($kernel);
