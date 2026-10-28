<?php
declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

$application = new Symfony\Bundle\FrameworkBundle\Console\Application(new Pentaglot\Php\Kernel('prod', false));
$application->setAutoExit(false);
exit($application->run(new Symfony\Component\Console\Input\ArrayInput([
    'command' => 'cache:clear',
    '--env' => 'prod',
    '--no-debug' => true,
])));
