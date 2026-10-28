<?php
declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

$kernel = new Pentaglot\Php\Kernel('prod', false);
try {
    $kernel->boot();
    $container = $kernel->getContainer();
    $warmers = $container->get('cache_warmer');
    $warmers->enableOptionalWarmers();
    $warmers->warmUp($kernel->getCacheDir(), $kernel->getBuildDir());
    $router = $container->get('router');
    $router->getMatcher();
    $router->getGenerator();
    printf("Symfony production container and router cache warmed; debug=%s\n", $kernel->isDebug() ? 'true' : 'false');
} finally {
    $kernel->shutdown();
}
