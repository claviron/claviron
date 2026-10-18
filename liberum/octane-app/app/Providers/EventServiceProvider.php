<?php

declare(strict_types=1);

namespace App\Providers;

use App\Events\DemoEvent;
use App\Listeners\HandleDemoEvent;
use App\Listeners\LogChannelOccupied;
use App\Listeners\LogChannelVacated;
use App\Listeners\LogRevolutionLogin;
use Lumenize\Auth\Events\AuthEvent;
use Lumenize\Broadcasting\Events\ChannelOccupied;
use Lumenize\Broadcasting\Events\ChannelVacated;
use Lumenize\EventBridge;
use Lumenize\Link\LinkSubscriber;
use Illuminate\Foundation\Support\Providers\EventServiceProvider as ServiceProvider;

final class EventServiceProvider extends ServiceProvider
{
    protected $listen = [
        AuthEvent::class => [
            LogRevolutionLogin::class,
        ],
        DemoEvent::class => [
            HandleDemoEvent::class,
        ],
        ChannelOccupied::class => [
            LogChannelOccupied::class,
        ],
        ChannelVacated::class => [
            LogChannelVacated::class,
        ],
    ];

    /** Who is on the links this application takes part in, logged. */
    protected $subscribe = [
        LinkSubscriber::class,
    ];

    public function register(): void
    {
        // Install the fleet relay before Laravel registers the ordinary
        // listener chain. The event class and every dispatch site remain
        // completely unaware of Claviron.
        $this->booting(function (): void {
            if (EventBridge::available()) {
                $bridge = $this->app->make(EventBridge::class);
                $bridge->subscribe([AuthEvent::class]);
                $bridge->relay([
                    DemoEvent::class,
                ]);
            }
        });

        parent::register();
    }
}
