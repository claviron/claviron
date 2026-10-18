<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Models\DemoRecord;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use RuntimeException;

/**
 * The native PostgreSQL connection through the surfaces an application
 * actually uses. Each action is one probe with one question.
 */
final class DatabaseController extends Controller
{
    /**
     * A bound select and a transaction on the default connection, with the
     * pool the runtime served it from.
     */
    public function roundTrip(): JsonResponse
    {
        $database = DB::connection();
        $row = $database->selectOne('select ?::int8 as value, current_database() as database', [42]);
        $transaction = $database->transaction(fn () => $database->selectOne(
            'select ?::text as state',
            ['committed'],
        ));

        return response()->json([
            'pool' => $database->getConfig('claviron'),
            'value' => $row->value,
            'database' => $row->database,
            'transaction' => $transaction->state,
        ]);
    }

    /**
     * Type matrix: writes one row with every supported column type through
     * Eloquent, reads it back, and reports the PHP type of each value, so a
     * regression shows up as a changed type rather than a silent cast.
     */
    public function types(): JsonResponse
    {
        $uuid = (string) Str::uuid();

        $record = DemoRecord::create([
            'public_id' => $uuid,
            'name' => 'claviron',
            'notes' => null,
            'active' => true,
            'small_count' => 7,
            'big_count' => 9007199254740991,
            'amount' => '1234.56',
            'ratio' => 0.125,
            'meta' => ['source' => 'eloquent', 'tags' => ['a', 'b']],
            'settings' => ['nested' => ['enabled' => true]],
            'effective_on' => '2026-09-11',
            'runs_at' => '04:30:00',
            'seen_at' => now(),
            'synced_at' => now(),
            'payload' => 'binary-payload',
        ]);

        $fresh = DemoRecord::query()->findOrFail($record->getKey());

        // Each of these filters binds a different parameter type, which is where a
        // typed binary protocol diverges from PDO if the type layer is wrong.
        $filters = [
            'by_uuid' => DemoRecord::query()->where('public_id', $uuid)->exists(),
            'by_timestamp' => DemoRecord::query()->where('seen_at', '<=', now())->exists(),
            'by_date' => DemoRecord::query()->whereDate('effective_on', '2026-09-11')->exists(),
            'by_decimal' => DemoRecord::query()->where('amount', '>', 1000)->exists(),
            'by_boolean' => DemoRecord::query()->where('active', true)->exists(),
            'by_json' => DemoRecord::query()->where('meta->source', 'eloquent')->exists(),
            'by_in' => DemoRecord::query()->whereIn('small_count', [7, 8, 9])->exists(),
            'by_null' => DemoRecord::query()->whereNull('notes')->exists(),
        ];

        $aggregates = [
            'count' => DemoRecord::query()->count(),
            'sum_amount' => (string) DemoRecord::query()->sum('amount'),
            'max_seen_at' => (string) DemoRecord::query()->max('seen_at'),
        ];

        $fresh->update(['small_count' => 8, 'meta' => ['source' => 'updated']]);

        // Rolled back on purpose: proves the transaction pins one pooled
        // connection and that the rollback is visible to the next read.
        $rolledBack = null;
        try {
            DB::transaction(function () use ($fresh): void {
                $fresh->update(['name' => 'rolled-back']);
                throw new RuntimeException('intentional rollback');
            });
        } catch (RuntimeException $exception) {
            $rolledBack = $exception->getMessage();
        }

        $after = DemoRecord::query()->findOrFail($record->getKey());

        return response()->json([
            'connection' => DB::connection()->getName(),
            'created_id' => $record->getKey(),
            'php_types' => [
                'id' => get_debug_type($after->id),
                'public_id' => get_debug_type($after->public_id),
                'active' => get_debug_type($after->active),
                'small_count' => get_debug_type($after->small_count),
                'big_count' => get_debug_type($after->big_count),
                'amount' => get_debug_type($after->amount),
                'ratio' => get_debug_type($after->ratio),
                'meta' => get_debug_type($after->meta),
                'settings' => get_debug_type($after->settings),
                'effective_on' => get_debug_type($after->effective_on),
                'runs_at' => get_debug_type($after->runs_at),
                'seen_at' => get_debug_type($after->seen_at),
                'synced_at' => get_debug_type($after->synced_at),
                'created_at' => get_debug_type($after->created_at),
            ],
            'values' => [
                'public_id' => $after->public_id,
                'amount' => $after->amount,
                'big_count' => $after->big_count,
                'meta' => $after->meta,
                'effective_on' => $after->effective_on?->toDateString(),
                'runs_at' => $after->runs_at,
                'seen_at' => $after->seen_at?->toIso8601String(),
                'synced_at' => $after->synced_at?->toIso8601String(),
                'payload' => $after->payload,
            ],
            'filters' => $filters,
            'aggregates' => $aggregates,
            'update_applied' => $after->small_count === 8,
            'transaction_rolled_back' => $rolledBack !== null && $after->name === 'claviron',
        ]);
    }

    /**
     * Opens a transaction and returns without ending it, the way a fatal
     * error or a forgotten rollback would. The runtime ends it when the
     * request does; `capabilities` reads the level at its next request start
     * to show that nothing leaked.
     */
    public function leakTransaction(): JsonResponse
    {
        $connection = DB::connection();
        $connection->beginTransaction();

        return response()->json(['level_when_returning' => $connection->transactionLevel()]);
    }
}
