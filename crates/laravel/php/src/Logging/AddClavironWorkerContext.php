<?php

declare(strict_types=1);

namespace Claviron\Laravel\Logging;

use Illuminate\Log\Logger;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\FormattableHandlerInterface;

final class AddClavironWorkerContext
{
    public function __invoke(Logger $logger): void
    {
        $workerId = getenv('CLAVIRON_WORKER_ID');

        if ($workerId === false || !ctype_digit($workerId) || (int) $workerId < 1) {
            return;
        }

        $format = "[%datetime%] [Worker #{$workerId}] %channel%.%level_name%: %message% %context% %extra%\n";
        $formatter = new LineFormatter($format, 'Y-m-d H:i:s', true, true, true);

        foreach ($logger->getLogger()->getHandlers() as $handler) {
            if ($handler instanceof FormattableHandlerInterface) {
                $handler->setFormatter($formatter);
            }
        }
    }
}
