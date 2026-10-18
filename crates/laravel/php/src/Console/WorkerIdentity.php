<?php

declare(strict_types=1);

namespace Claviron\Laravel\Console;

/** Process identity for console records emitted by Claviron-owned commands. */
final class WorkerIdentity
{
    public static function label(): ?string
    {
        $workerId = getenv('CLAVIRON_WORKER_ID');

        if ($workerId === false || !ctype_digit($workerId) || (int) $workerId < 1) {
            return null;
        }

        return sprintf('[Worker #%d]', (int) $workerId);
    }

    public static function message(string $message): string
    {
        $label = self::label();

        return $label === null ? $message : $label.' '.$message;
    }
}
