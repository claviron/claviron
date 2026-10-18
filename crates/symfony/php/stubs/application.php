<?php

declare(strict_types=1);

use App\Kernel;
use Claviron\Symfony\ApplicationRunner;

require __DIR__.'/vendor/autoload.php';

// Copy to the application root. Claviron supplies the fixed application env.
$environment = $_SERVER['APP_ENV'] ?? $_ENV['APP_ENV'] ?? 'prod';
$debug = filter_var($_SERVER['APP_DEBUG'] ?? $_ENV['APP_DEBUG'] ?? false, FILTER_VALIDATE_BOOL);

ApplicationRunner::run(new Kernel($environment, $debug));
