<?php

namespace App\Console\Commands;

use Illuminate\Console\Attributes\Description;
use Illuminate\Console\Attributes\Signature;
use Illuminate\Console\Command;

#[Signature('app:send-emails')]
#[Description('Command description')]
class SendEmails extends Command
{
    /**
     * Execute the console command.
     */
    public function handle()
    {

        $running = true;
        $startedAt = now();

        pcntl_async_signals(true);

        pcntl_signal(
            SIGTERM,
            function () use (&$running): void {
                $now = now();
                $this->warn("{$now} - SendEmails: SIGTERM signal received.");
                $running = false;
            },
            false //
        );

        pcntl_signal(
            SIGINT,
            function () use (&$running): void {
                $now = now();
                $this->warn("{$now} - SendEmails: SIGINT signal received.");
                $running = false;
            },
            false
        );

        pcntl_signal(SIGHUP, function () use ($startedAt): void {
            $now = now();
            $elapsed = (int)$startedAt->diffInSeconds($now);
            $this->warn("{$now} - SendEmails: HUP signal received. I'm up since {$elapsed}sec, i will survive..");
        });


        $task_name = getenv('CLAVIRON_TASK_NAME');
        $task_slot = getenv('CLAVIRON_TASK_SLOT');
        $task_scope = getenv('CLAVIRON_TASK_SCOPE');
        $task_attempt = getenv('CLAVIRON_TASK_ATTEMPT');
        $worker_id = getenv('CLAVIRON_WORKER_NUMBER');
        $worker_pid = getenv('CLAVIRON_PROCESS_ID');

        $my_pid = getmypid();

        $this->info("{$startedAt} - Starting SendEmails Task({$task_name}), PID({$my_pid}), TaskSlot({$task_slot}), WorkerId({$worker_id}), WorkerPid({$worker_pid})");

        while ($running) {
            $now = now();
            $elapsed = (int)$startedAt->diffInSeconds($now);
            $memory_mb = number_format(memory_get_usage(true) / (1024 * 1024), 2);
            $this->info("{$now} - SendEmails Task({$task_name}), PID({$my_pid}), TaskSlot({$task_slot}), Scope($task_scope),  WorkerId({$worker_id}), WorkerPid({$worker_pid}), Memory({$memory_mb} MB), Attempt($task_attempt), Elapsed({$elapsed}s)");
            sleep(5);
        }

        $this->info("{$startedAt} - Shutdown SendEmails Task({$task_name}), PID({$my_pid}), TaskSlot({$task_slot}), WorkerId({$worker_id}), WorkerPid({$worker_pid})");
    }
}
