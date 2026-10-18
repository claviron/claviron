<?php

declare(strict_types=1);

namespace Claviron\Laravel\Events;

/**
 * An invoked program ended with a nonzero exit code, or died.
 *
 * `reason` is the fatal's first line when there was one; a plain nonzero exit
 * carries none.
 */
final class InvocationFailed extends InvocationOutcome
{
    public function __construct(
        string $invocation,
        string $command,
        int $exit,
        int $runMs,
        string $output = '',
        bool $truncated = false,
        ?string $outputFile = null,
        public readonly ?string $reason = null,
    ) {
        parent::__construct($invocation, $command, $exit, $runMs, $output, $truncated, $outputFile);
    }

    public static function topic(): string
    {
        return 'InvocationFailed';
    }

    public static function fromPayload(array $payload): static
    {
        return new static(
            (string) ($payload['invocation'] ?? '-/-'),
            (string) ($payload['command'] ?? ''),
            (int) ($payload['exit'] ?? 1),
            (int) ($payload['run_ms'] ?? 0),
            (string) ($payload['output'] ?? ''),
            (bool) ($payload['truncated'] ?? false),
            isset($payload['output_file']) ? (string) $payload['output_file'] : null,
            isset($payload['reason']) ? (string) $payload['reason'] : null,
        );
    }

    public function toPayload(int $budget): array
    {
        $payload = parent::toPayload($budget);

        if ($this->reason !== null) {
            $payload['reason'] = substr($this->reason, 0, 200);
        }

        return $payload;
    }
}
