<?php

declare(strict_types=1);

namespace Claviron\Laravel\Events;

/**
 * What an invoked program reported when it ended.
 *
 * `invocation` is the ring identity the submitter received from
 * `orbit_invoke` (`job/241:2:0`), which is how a submitter that fired several
 * matches each outcome to its request. `output` is the command's captured
 * stdout, bounded to fit the bus frame; `truncated` says whether it was cut,
 * and `outputFile` names the complete copy when the errand was asked to keep
 * one.
 */
abstract class InvocationOutcome extends FleetEvent
{
    public function __construct(
        public readonly string $invocation,
        public readonly string $command,
        public readonly int $exit,
        public readonly int $runMs,
        public readonly string $output = '',
        public readonly bool $truncated = false,
        public readonly ?string $outputFile = null,
    ) {
    }

    public static function fromPayload(array $payload): static
    {
        return new static(
            (string) ($payload['invocation'] ?? '-/-'),
            (string) ($payload['command'] ?? ''),
            (int) ($payload['exit'] ?? 0),
            (int) ($payload['run_ms'] ?? 0),
            (string) ($payload['output'] ?? ''),
            (bool) ($payload['truncated'] ?? false),
            isset($payload['output_file']) ? (string) $payload['output_file'] : null,
        );
    }

    public function toPayload(int $budget): array
    {
        $payload = [
            'invocation' => $this->invocation,
            'command' => $this->command,
            'exit' => $this->exit,
            'run_ms' => $this->runMs,
            'output' => substr($this->output, 0, $budget),
            'truncated' => $this->truncated || strlen($this->output) > $budget,
        ];

        if ($this->outputFile !== null) {
            $payload['output_file'] = $this->outputFile;
        }

        return $payload;
    }

}
