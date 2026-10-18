<?php

declare(strict_types=1);

// PHP may change its virtual cwd to this runner's directory when executing it.
// The host pins the consumer root as the first argument, before PHP starts.
$basePath = $_SERVER['argv'][1] ?? null;
if (!is_string($basePath) || $basePath === '') {
    throw new RuntimeException('The Laravel application directory is required as the first argument.');
}
if (!chdir($basePath)) {
    throw new RuntimeException('Cannot enter the Laravel application directory.');
}

require $basePath.'/vendor/autoload.php';

\Claviron\Laravel\Octane\ApplicationRunner::run($basePath);
