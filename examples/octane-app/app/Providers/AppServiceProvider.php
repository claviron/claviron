<?php

namespace App\Providers;

use App\Http\Controllers\OrbitalCellController;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use Laravel\Octane\Events\WorkerStarting;
use Laravel\Octane\Events\WorkerStopping;

class AppServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(OrbitalCellController::class);
    }

    public function boot(): void
    {
        if (! function_exists('claviron_runtime_available') || ! claviron_runtime_available()) {
            return;
        }

        // Resolve on the base application before Octane clones request sandboxes.
        Event::listen(WorkerStarting::class, static function (WorkerStarting $event): void {
            $event->app->make(OrbitalCellController::class);
        });

        Event::listen(WorkerStopping::class, static function (WorkerStopping $event): void {
            if ($event->app->resolved(OrbitalCellController::class)) {
                $event->app->make(OrbitalCellController::class)->release();
            }
        });
    }
}
