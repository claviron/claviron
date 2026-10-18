<?php

declare(strict_types=1);

namespace Claviron\Laravel\Providers;

use Claviron\Monolog\ClavironHandler;
use Illuminate\Log\LogManager;
use Illuminate\Support\ServiceProvider;
use Monolog\Logger;

/**
 * Registers the `claviron` log driver, in config/logging.php:
 *
 *     'claviron' => [
 *         'driver' => 'claviron',
 *         'channel' => 'laravel',   // a channel logging.yaml declares
 *         'level' => env('LOG_LEVEL', 'debug'),
 *         'target' => null,         // null: the calling class::method; or a fixed name
 *         'skip' => [],             // prefixes of your own logging helpers
 *     ],
 *
 * Choose it where it can work: set `LOG_CHANNEL: claviron` in the runtime's
 * application `env` (php.yaml) and keep `.env`'s channel for `php artisan`
 * from a shell — Laravel's own channel selection, nothing added to the
 * driver. Outside the runtime, or naming an undeclared channel, the driver
 * refuses; Laravel then writes to its emergency log and says why.
 */
final class LogServiceProvider extends ServiceProvider
{
    public function boot(LogManager $log): void
    {
        self::registerDriver($log);
    }

    /**
     * Register the driver on an existing manager owned by a lightweight
     * framework container, such as Lumen's LoggerInterface singleton.
     */
    public static function registerDriver(LogManager $log): void
    {
        $log->extend('claviron', function ($app, array $config): Logger {
            $handler = new ClavironHandler(
                (string) ($config['channel'] ?? 'laravel'),
                $config['level'] ?? 'debug',
                isset($config['target']) ? (string) $config['target'] : null,
                (array) ($config['skip'] ?? []),
                (bool) ($config['bubble'] ?? true),
            );

            return new Logger((string) ($config['name'] ?? $app->environment()), [$handler]);
        });
    }
}
