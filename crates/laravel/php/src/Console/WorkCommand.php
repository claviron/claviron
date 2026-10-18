<?php

declare(strict_types=1);

namespace Claviron\Laravel\Console;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Console\WorkCommand as IlluminateWorkCommand;

/** Queue worker console output owned by the Laravel adapter. */
final class WorkCommand extends IlluminateWorkCommand
{
    /**
     * Write one complete record per state transition.
     *
     * Laravel's interactive renderer assembles each line through several
     * writes and sizes a dotted spacer from the terminal width. A task log is
     * a byte stream rather than a terminal, so those fragments can acquire a
     * separate transport prefix. Keeping the record atomic here leaves the
     * task output transport deliberately unaware of Laravel formatting.
     */
    protected function writeOutputForCli(Job $job, $status)
    {
        $isVerbose = $this->output->isVerbose();
        $worker = WorkerIdentity::label();
        $details = $isVerbose
            ? sprintf(
                ' <fg=gray>%s</> <fg=blue>%s</> <fg=blue>%s</>',
                $job->getJobId(),
                $job->getConnectionName(),
                $job->getQueue(),
            )
            : '';

        if ($status === 'starting') {
            $this->latestStartedAt = microtime(true);
            $completion = '<fg=yellow;options=bold>RUNNING</>';
        } else {
            $runTime = $this->runTimeForHumans($this->latestStartedAt);
            $memory = $isVerbose ? ' '.round(memory_get_usage(true) / 1024 / 1024, 1).'MB' : '';
            $result = match ($status) {
                'success' => '<fg=green;options=bold>DONE</>',
                'released_after_exception' => '<fg=yellow;options=bold>FAIL</>',
                default => '<fg=red;options=bold>FAIL</>',
            };
            $completion = sprintf('<fg=gray>%s%s</> %s', $runTime, $memory, $result);
        }

        $this->output->writeln(sprintf(
            '<fg=gray>[%s]</>%s %s%s %s',
            $this->now()->format('Y-m-d H:i:s'),
            $worker === null ? '' : sprintf(' <fg=gray>%s</>', $worker),
            $job->resolveName(),
            $details,
            $completion,
        ));
    }
}
