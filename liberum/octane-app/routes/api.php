<?php

declare(strict_types=1);

use App\Http\Controllers\Api\TodoController;
use Illuminate\Support\Facades\Route;

Route::delete('todos/completed', [TodoController::class, 'clearCompleted'])->name('todos.clear-completed');
Route::post('todos/reorder', [TodoController::class, 'reorder'])->name('todos.reorder');
Route::post('todos/{todo}/toggle', [TodoController::class, 'toggle'])->name('todos.toggle');
Route::apiResource('todos', TodoController::class);
