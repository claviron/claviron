<?php
declare(strict_types=1);

namespace Pentaglot\Php;

use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel as BaseKernel;
use Symfony\Component\Routing\Loader\Configurator\RoutingConfigurator;

final class Kernel extends BaseKernel
{
    use MicroKernelTrait;

    public function getProjectDir(): string
    {
        return dirname(__DIR__);
    }

    public function registerBundles(): iterable
    {
        yield new FrameworkBundle();
    }

    private function configureContainer(ContainerConfigurator $container): void
    {
        $container->extension('framework', ['http_method_override' => false]);
        $container->services()->set(Controller::class)->public();
    }

    private function configureRoutes(RoutingConfigurator $routes): void
    {
        $routes->add('php', '/php')->controller(Controller::class)->methods(['GET']);
        $routes->add('php_echo', '/php/echo')->controller(Controller::class)->methods(['POST']);
    }
}
