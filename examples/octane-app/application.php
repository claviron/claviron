<?php

declare(strict_types=1);

use Lumenize\Octane\ApplicationRunner;

require __DIR__ . '/vendor/autoload.php';

// The receive / request / event / terminate loop is the package's, not this
// application's: ApplicationRunner owns it, and this entry says only where the
// application lives. There is no set_time_limit() here either — the budget of
// a request or event is Claviron's (php.yaml
// `applications.<name>.execution_timeout`); see ApplicationRunner.
ApplicationRunner::run(basePath: __DIR__);
