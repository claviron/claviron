<?php

declare(strict_types=1);

// The Claviron application model's entry point, as shipped by `claviron/laravel`.
//
// Copy this file to the application root and point `php.yaml`'s
// `applications.<name>.script_path` at it. It boots Octane's Worker once and then
// loops on `claviron_application_receive()`; everything request-specific is
// the client's, everything lifecycle-specific is the engine's.

use Claviron\Laravel\Octane\ApplicationRunner;
use Illuminate\Http\Request;

require __DIR__.'/vendor/autoload.php';

ApplicationRunner::run(basePath: __DIR__, requestClass: Request::class);
