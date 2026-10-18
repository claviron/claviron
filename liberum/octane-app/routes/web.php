<?php

use App\Http\Controllers\BroadcastController;
use App\Http\Controllers\DatabaseCapabilitiesController;
use App\Http\Controllers\DatabaseController;
use App\Http\Controllers\DemoEventController;
use App\Http\Controllers\InvocationController;
use App\Http\Controllers\OrbitalCellController;
use App\Http\Controllers\RuntimeController;
use App\Http\Controllers\SessionController;
use Illuminate\Support\Facades\Route;

Route::view('/', 'welcome')->name('welcome');
Route::view('/todos', 'todos')->name('todos');

Route::get('/runtime', RuntimeController::class)->name('runtime');
Route::get('/runtime/sleep', [RuntimeController::class, 'sleep'])->name('runtime.sleep');
Route::get('/_lumenize/php', [OrbitalCellController::class, 'hit'])->name('cell.hit');
Route::get('/_lumenize/php/value', [OrbitalCellController::class, 'value'])->name('cell.value');
Route::get('/events/demo', DemoEventController::class)->name('events.demo');
Route::get('/events/shared', [DemoEventController::class, 'shared'])->name('events.shared');

Route::controller(SessionController::class)
    ->prefix('session')
    ->name('session.')
    ->group(function (): void {
        Route::get('/', 'show')->name('show');
        Route::delete('/', 'destroy')->name('destroy');
    });

Route::controller(InvocationController::class)
    ->prefix('invocation')
    ->name('invocation.')
    ->group(function (): void {
        Route::get('/shout', 'shout')->name('shout');
        Route::get('/js-shout', 'jsShout')->name('js-shout');
        Route::get('/geo-ip', 'geoIp')->name('geo-ip');
        Route::get('/ping', 'ping')->name('ping');
    });

Route::controller(BroadcastController::class)
    ->prefix('broadcast')
    ->name('broadcast.')
    ->group(function (): void {
        Route::post('/message', 'message')->name('message');
        Route::post('/notification', 'notification')->name('notification');
    });

Route::controller(DatabaseController::class)
    ->prefix('database')
    ->name('database.')
    ->group(function (): void {
        Route::get('/round-trip', 'roundTrip')->name('round-trip');
        Route::get('/types', 'types')->name('types');
        Route::get('/leak-transaction', 'leakTransaction')->name('leak-transaction');
        Route::get('/capabilities', DatabaseCapabilitiesController::class)->name('capabilities');
    });

// A record on Laravel's default log channel — `claviron` inside the runtime
// (config/runtime/php.yaml sets LOG_CHANNEL), .env's outside it. Its source is
// found by the runtime, its context becomes fields.
Route::get('/log-demo', function () {
    \Illuminate\Support\Facades\Log::warning('log demo', [
        'order' => ['id' => 7, 'total' => 12.5],
        'paid' => false,
        'exception' => new RuntimeException('just a demo'),
    ]);

    return response()->json(['logged' => true]);
})->name('log-demo');
