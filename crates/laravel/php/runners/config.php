<?php

declare(strict_types=1);

// Config evaluation bootstrap, not an HTTP application or Artisan entry.
// The native host executes the pinned source after this callable returns.
return static function (string $basePath, array $environment): void {
    require $basePath.'/vendor/autoload.php';

    \Claviron\Laravel\Config\ConfigEvaluator::prepare($basePath, $environment);
};
