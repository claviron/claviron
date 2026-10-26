<?php

declare(strict_types=1);

namespace Claviron\Monolog;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\LogRecord;
use RuntimeException;
use Throwable;

/**
 * A Monolog handler for any framework that logs through Monolog — Laravel,
 * Symfony, a plain script. Hands each record to Claviron's log pipeline: a copy into this process's
 * lane of the fleet's log ring, written to the channel `logging.yaml`
 * declares by the fleet's one writer. No file is opened, locked or stat'ed
 * here — per request, that is what a file handler costs.
 *
 * The level threshold is Monolog's, applied before anything crosses into
 * Rust. The record's source is found by the runtime from the calling frame
 * (`Namespace\Class::method`) unless the channel names a fixed target.
 */
final class ClavironHandler extends AbstractProcessingHandler
{
    private readonly int $channel;

    /**
     * @param list<string> $skip class prefixes that are logging helpers, never a source
     */
    public function __construct(
        string $channel,
        int|string|Level $level = Level::Debug,
        private readonly ?string $target = null,
        array $skip = [],
        bool $bubble = true,
    ) {
        if (!function_exists('claviron_log_channel')) {
            throw new RuntimeException('The claviron log driver needs the Claviron runtime.');
        }
        $id = claviron_log_channel($channel, $skip);
        if ($id === null) {
            throw new RuntimeException(sprintf(
                'Claviron log channel [%s] is not declared in logging.yaml.',
                $channel,
            ));
        }
        $this->channel = $id;

        parent::__construct($level, $bubble);
    }

    protected function write(LogRecord $record): void
    {
        claviron_log(
            $this->channel,
            strtolower($record->level->getName()),
            $record->message,
            self::context($record->context + $record->extra),
            $this->target,
        );
    }

    /**
     * An exception becomes fields a reader can search; everything else is
     * passed as it is and flattened by the runtime.
     *
     * @param array<array-key, mixed> $context
     * @return array<array-key, mixed>
     */
    private static function context(array $context): array
    {
        foreach ($context as $key => $value) {
            if ($value instanceof Throwable) {
                unset($context[$key]);
                $context[$key . '.class'] = $value::class;
                $context[$key . '.message'] = $value->getMessage();
                $context[$key . '.file'] = $value->getFile();
                $context[$key . '.line'] = $value->getLine();
                $context[$key . '.trace'] = $value->getTraceAsString();
            }
        }

        return $context;
    }
}
