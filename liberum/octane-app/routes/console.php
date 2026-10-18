<?php

use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Schedule;

Artisan::command('inspire', function () {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

// Whoever subscribed hears it, in whatever language: the Symfony PHP daemon
// and the JavaScript heartbeat task are both on the same process event bus.
//Schedule::call(function () {
//    if (!function_exists('claviron_event_publish')) {
//        return;
//    }
//    claviron_event_publish('jobs.tick', [
//        'at' => now()->toIso8601String(),
//        'worker' => getenv('CLAVIRON_WORKER_NUMBER') ?: '?',
//        'pid' => getmypid(),
//    ]);
//})->everyTenSeconds();

// Claviron: one scheduled command a minute, published as JobScheduled and run
// by the job-bridge task on whichever worker holds it.
//Schedule::command('inspire')->everyMinute();
//    ->runInBackground();
