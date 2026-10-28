<?php
declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

Claviron\Symfony\ApplicationRunner::run(new Pentaglot\Php\Kernel('prod', false));
