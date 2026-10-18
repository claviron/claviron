<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Broadcasting\Broadcaster;
use Claviron\Laravel\Broadcasting\Events\ChannelOccupied;
use Claviron\Laravel\Broadcasting\Events\ChannelVacated;
use Claviron\Laravel\Broadcasting\UnconfiguredBroadcaster;
use Claviron\Laravel\EventBridge;
use Illuminate\Broadcasting\BroadcastManager;
use Illuminate\Broadcasting\Broadcasters\Broadcaster as IlluminateBroadcaster;
use Illuminate\Support\ServiceProvider;

final class BroadcastingProvider extends ServiceProvider
{
    public function boot(BroadcastManager $broadcast): void
    {
        $broadcast->extend('claviron', static function ($app, array $config): IlluminateBroadcaster {
            $key = (string) ($config['key'] ?? '');
            $secret = (string) ($config['secret'] ?? '');

            if ($key === '' || $secret === '') {
                return new UnconfiguredBroadcaster();
            }

            return new Broadcaster(
                $key,
                $secret,
                (bool) ($config['require_authenticated_user'] ?? true),
            );
        });

        if (EventBridge::available()) {
            $this->app->make(EventBridge::class)->subscribe([
                ChannelOccupied::class,
                ChannelVacated::class,
            ]);
        }
    }
}
