<?php

declare(strict_types=1);

// Fixture-owned entry, standing in for an application's editable artisan file.
$basePath = getcwd();
require $basePath.'/vendor/autoload.php';

$app = require $basePath.'/bootstrap/app.php';
$kernel = $app->make(\Illuminate\Contracts\Console\Kernel::class);
$input = new \Symfony\Component\Console\Input\ArgvInput([
    $basePath.'/artisan',
    ...array_slice($_SERVER['argv'], 1),
]);
$output = new \Symfony\Component\Console\Output\ConsoleOutput();

$status = $kernel->handle($input, $output);
$kernel->terminate($input, $status);
exit($status);
