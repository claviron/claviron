<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Laravel\Auth\FleetSession;
use Claviron\Laravel\Auth\FleetSessionGuard;
use Claviron\Laravel\Auth\DispatchAuthEvent;
use Claviron\Laravel\Auth\Events\AuthEvent;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Auth\RequestGuard;
use Illuminate\Support\ServiceProvider;
use RuntimeException;

/** Opt-in guard: configure driver=claviron with the application's user provider. */
final class AuthServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        // Opt-in: this is not a subscription in every HTTP/queue instance.
        // The selected event-consuming task owns the actual subscription.
        $events = $this->app['config']->get('auth.claviron_events');
        if ($events !== null) {
            if (!is_array($events) || !is_string($events['provider'] ?? null)
                || !is_string($events['subject_key'] ?? null) || trim($events['subject_key']) === '') {
                throw new RuntimeException('Claviron auth events require provider and subject_key.');
            }
            $this->app->singleton(DispatchAuthEvent::class, function ($app) use ($events): DispatchAuthEvent {
                $users = $app['auth']->createUserProvider($events['provider']);
                if ($users === null) {
                    throw new RuntimeException('Claviron auth event user provider not found.');
                }

                return new DispatchAuthEvent($users, $app->make(Dispatcher::class), $events['subject_key']);
            });
            $this->app->make(Dispatcher::class)->listen(AuthEvent::class, [DispatchAuthEvent::class, 'handle']);
        }
        $this->app['auth']->extend('claviron', function ($app, string $name, array $config): RequestGuard {
            $provider = $app['auth']->createUserProvider($config['provider'] ?? null);

            if ($provider === null) {
                throw new RuntimeException('The claviron auth guard requires a user provider.');
            }

            $session = new FleetSession($provider, $config['subject_key'] ?? null, $config['constraints'] ?? []);
            $guard = new FleetSessionGuard(fn () => $session->user(), $app['request'], $provider);
            $app->refresh('request', $guard, 'setRequest');

            return $guard;
        });
    }
}
