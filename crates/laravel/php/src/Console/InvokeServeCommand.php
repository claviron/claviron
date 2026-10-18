<?php

declare(strict_types=1);

namespace Claviron\Laravel\Console;

use Claviron\Laravel\Invocation\Runner;
use Illuminate\Console\Command;

/**
 * Boot once, then serve this application's registered handlers/events.
 * Class names and subscriptions belong to registrars, never command arguments.
 */
final class InvokeServeCommand extends Command
{
    protected $signature = 'invoke:serve';

    protected $description = 'Serve the active application\'s registered invocations and events';

    public function handle(Runner $runner): int
    {
        $this->info('Serving registered invocations and events.');

        $runner->serve();

        $this->info('Invocation server stopped.');

        return self::SUCCESS;
    }
}
