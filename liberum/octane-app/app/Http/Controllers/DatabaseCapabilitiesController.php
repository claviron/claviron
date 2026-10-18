<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Models\DemoRecord;
use Illuminate\Database\ConcurrencyErrorDetector;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use RuntimeException;
use Throwable;

/**
 * Capability matrix for the native connection: every framework surface a
 * real application reaches for, run and reported per call. A row that fails
 * names the exact method the Laravel implementation still has to cover.
 */
final class DatabaseCapabilitiesController extends Controller
{
    public function __invoke(): JsonResponse
    {
        $connection = DB::connection();
        $schema = $connection->getSchemaBuilder();

        // Statements that raise a chosen SQLSTATE, so error handling can be
        // exercised without provoking a real deadlock. Nowdoc, so PHP leaves the
        // dollar-quoted bodies alone.
        $raise = [
            '23505' => <<<'SQL'
                do $probe$ begin raise exception 'duplicate key value violates unique constraint' using errcode = '23505'; end $probe$
                SQL,
            '40001' => <<<'SQL'
                do $probe$ begin raise exception 'could not serialize access due to concurrent update' using errcode = '40001'; end $probe$
                SQL,
            '40P01' => <<<'SQL'
                do $probe$ begin raise exception 'deadlock detected' using errcode = '40P01'; end $probe$
                SQL,
        ];

        $probe = static function (callable $callback): array {
            try {
                $value = $callback();

                return ['ok' => true, 'value' => is_scalar($value) || $value === null
                    ? $value
                    : (is_array($value) ? 'array('.count($value).')' : get_debug_type($value))];
            } catch (Throwable $exception) {
                return ['ok' => false, 'error' => get_debug_type($exception).': '.$exception->getMessage()];
            }
        };

        return response()->json([
            // Read before anything else: the connection object outlives the
            // request in a persistent worker, so a level left behind by an earlier
            // request shows up here.
            'transaction_level_at_request_start' => $connection->transactionLevel(),
            'connection' => [
                'getServerVersion' => $probe(fn () => $connection->getServerVersion()),
                'escape_string' => $probe(fn () => $connection->escape("o'brien")),
                'escape_bool' => $probe(fn () => $connection->escape(true)),
                'escape_binary' => $probe(fn () => $connection->escape(chr(0).chr(255), true)),
                'selectResultSets' => $probe(fn () => $connection->selectResultSets('select 1 as one')),
                'unprepared' => $probe(fn () => $connection->unprepared('select 1')),
                'raw_select' => $probe(fn () => $connection->select('select 1 as one')[0]->one),
            ],
            'schema' => [
                'hasTable' => $probe(fn () => $schema->hasTable('demo_records')),
                'getColumnListing' => $probe(fn () => $schema->getColumnListing('demo_records')),
                'getColumns' => $probe(fn () => $schema->getColumns('demo_records')),
                'getIndexes' => $probe(fn () => $schema->getIndexes('demo_records')),
                'getForeignKeys' => $probe(fn () => $schema->getForeignKeys('demo_records')),
                'getTables' => $probe(fn () => $schema->getTables()),
                'getViews' => $probe(fn () => $schema->getViews()),
                'getTypes' => $probe(fn () => $schema->getTypes()),
                'hasColumn' => $probe(fn () => $schema->hasColumn('demo_records', 'public_id')),
                'getCurrentSchemaName' => $probe(fn () => $schema->getCurrentSchemaName()),
            ],
            'ddl' => [
                'create_drop' => $probe(function () use ($schema) {
                    $schema->dropIfExists('claviron_probe');
                    $schema->create('claviron_probe', function ($table): void {
                        $table->id();
                        $table->string('label')->nullable();
                    });
                    $existed = $schema->hasTable('claviron_probe');
                    $schema->drop('claviron_probe');

                    return $existed;
                }),
            ],
            // Not capabilities but semantics: what a pooled connection does that a
            // PDO handle does not. Each pair writes session state in one execute
            // and reads it back in the next, so a mismatch means the two statements
            // landed on different pooled connections.
            'pool_semantics' => [
                'set_outside_transaction' => $probe(function () use ($connection) {
                    $connection->statement("set application_name = 'claviron-probe'");

                    return $connection->selectOne('select current_setting(\'application_name\') as v')->v;
                }),
                'set_local_inside_transaction' => $probe(fn () => $connection->transaction(function () use ($connection) {
                    $connection->statement("set local application_name = 'inside-tx'");

                    return $connection->selectOne('select current_setting(\'application_name\') as v')->v;
                })),
                'temp_table_across_statements' => $probe(function () use ($connection) {
                    $connection->statement('create temp table claviron_tmp_probe (i int)');

                    return (int) $connection->selectOne('select count(*) as c from claviron_tmp_probe')->c;
                }),
                // `pg_try_advisory_lock`, never the blocking form: a lock taken
                // here is released by no one — the connection goes back to the pool
                // still holding it — so the blocking call hangs the next request
                // forever, and with no statement timeout that takes the whole
                // application instance down with it.
                //
                // False here means an earlier request already leaked this lock.
                'advisory_lock_leaks_into_pool' => $probe(function () use ($connection) {
                    $acquired = (bool) $connection->selectOne(
                        'select pg_try_advisory_lock(424242) as v'
                    )->v;

                    if ($acquired) {
                        $connection->selectOne('select pg_advisory_unlock_all() as v');
                    }

                    return $acquired ? 'acquired (released best-effort)' : 'already held by a pooled connection';
                }),
            ],
            // PostgreSQL spells JSON key-existence `?`, which collides with the
            // placeholder marker. Laravel doubles it to `??` because that is what
            // PDO unescapes back to a literal `?`.
            'json_operators' => [
                'raw_double_question' => $probe(fn () => $connection->selectOne(
                    'select \'{"a":1}\'::jsonb ?? \'a\' as v'
                )->v),
                'whereJsonContainsKey' => $probe(fn () => DB::connection()
                    ->table('demo_records')->whereJsonContainsKey('settings->nested')->count()),
                'double_question_with_a_real_binding' => $probe(fn () => $connection->selectOne(
                    'select \'{"a":1}\'::jsonb ?? \'a\' as v, ? as bound', ['x']
                )->bound),
            ],
            // A runaway query must not occupy the worker until it finishes.
            'statement_budget' => [
                'within_budget' => $probe(fn () => $connection->selectOne('select pg_sleep(0.2) as v') !== null),
                'over_budget' => $probe(function () use ($connection) {
                    $started = microtime(true);

                    try {
                        $connection->selectOne('select pg_sleep(30) as v');

                        return 'NOT CANCELLED';
                    } catch (Throwable $exception) {
                        return sprintf(
                            'cancelled after %.1fs: %s',
                            microtime(true) - $started,
                            str_contains($exception->getMessage(), 'statement timeout')
                                ? 'statement timeout'
                                : $exception->getMessage(),
                        );
                    }
                }),
                'usable_after_cancel' => $probe(fn () => $connection->selectOne('select 1 as v')->v),
            ],
            // The rows stay on PostgreSQL; PHP holds one batch.
            'cursor' => [
                'walks_every_row' => $probe(function () use ($connection) {
                    $seen = 0;
                    foreach ($connection->cursor('select generate_series(1, 2500) as n') as $row) {
                        $seen += $row->n > 0 ? 1 : 0;
                    }

                    return $seen;
                }),
                'binds_parameters' => $probe(function () use ($connection) {
                    return iterator_count($connection->cursor('select generate_series(1, ?) as n', [5]));
                }),
                'inside_a_transaction' => $probe(fn () => $connection->transaction(
                    fn () => iterator_count($connection->cursor('select generate_series(1, 10) as n'))
                )),
                'abandoned_midway_leaves_a_usable_connection' => $probe(function () use ($connection) {
                    foreach ($connection->cursor('select generate_series(1, 100000) as n') as $row) {
                        break;
                    }

                    return $connection->selectOne('select 1 as v')->v;
                }),
                'memory_is_bounded' => $probe(function () use ($connection) {
                    $sql = 'select generate_series(1, 50000) as n';

                    $before = memory_get_usage();
                    $streamed = iterator_count($connection->cursor($sql));
                    $streamedCost = memory_get_usage() - $before;

                    $before = memory_get_usage();
                    $rows = $connection->select($sql);
                    $materialisedCost = memory_get_usage() - $before;
                    unset($rows);

                    return sprintf(
                        '%d rows: cursor %.1f MB, select %.1f MB',
                        $streamed,
                        $streamedCost / 1048576,
                        $materialisedCost / 1048576,
                    );
                }),
            ],
            // A failure has to arrive the way PDO reports one, or nothing
            // downstream can tell a deadlock from a typo.
            'errors' => [
                'sqlstate_surfaces' => $probe(function () use ($connection, $raise) {
                    try {
                        $connection->statement($raise['23505']);

                        return 'NOT RAISED';
                    } catch (Throwable $exception) {
                        return $exception->getCode();
                    }
                }),
                'concurrency_error_detected' => $probe(function () use ($connection, $raise) {
                    try {
                        $connection->statement($raise['40001']);

                        return 'NOT RAISED';
                    } catch (Throwable $exception) {
                        return (new ConcurrencyErrorDetector)
                            ->causedByConcurrencyError($exception);
                    }
                }),
                'deadlock_detected' => $probe(function () use ($connection, $raise) {
                    try {
                        $connection->statement($raise['40P01']);

                        return 'NOT RAISED';
                    } catch (Throwable $exception) {
                        return (new ConcurrencyErrorDetector)
                            ->causedByConcurrencyError($exception);
                    }
                }),
                'retries_a_serialization_failure' => $probe(function () use ($connection, $raise) {
                    $attempts = 0;

                    $connection->transaction(function () use ($connection, $raise, &$attempts): void {
                        $attempts++;

                        if ($attempts < 3) {
                            $connection->statement($raise['40001']);
                        }
                    }, 3);

                    return $attempts;
                }),
                'does_not_retry_a_business_error' => $probe(function () use ($connection) {
                    $attempts = 0;

                    try {
                        $connection->transaction(function () use (&$attempts): void {
                            $attempts++;

                            throw new RuntimeException('a business rule said no');
                        }, 3);
                    } catch (RuntimeException) {
                        // Expected once, not three times.
                    }

                    return $attempts;
                }),
            ],
            'sql_parsing' => [
                'multi_statement_unprepared' => $probe(fn () => $connection->unprepared(
                    'create temp table m1 (i int); drop table m1;'
                )),
                'dollar_quoted_body' => $probe(fn () => $connection->selectOne(
                    'select $$why? because.$$ as v'
                )->v),
                'line_comment_with_question_mark' => $probe(fn () => $connection->selectOne(
                    "select 1 as v -- does this ? break\n"
                )->v),
                'literal_question_mark' => $probe(fn () => $connection->selectOne(
                    "select 'why?' as v"
                )->v),
            ],
            'query' => [
                'insertGetId' => $probe(fn () => DB::connection()->table('demo_records')->insertGetId([
                    'public_id' => (string) Str::uuid(),
                    'name' => 'capability-probe',
                    'created_at' => now(),
                    'updated_at' => now(),
                ])),
                'toRawSql' => $probe(fn () => DemoRecord::query()->where('name', "o'brien")->toRawSql()),
                'cursor' => $probe(fn () => iterator_count(DemoRecord::query()->cursor())),
                'chunk' => $probe(function (): int {
                    $seen = 0;
                    DemoRecord::query()->orderBy('id')->chunk(2, function ($rows) use (&$seen): void {
                        $seen += $rows->count();
                    });

                    return $seen;
                }),
                'paginate' => $probe(fn () => DemoRecord::query()->paginate(2)->total()),
                'nested_transaction' => $probe(function () use ($connection) {
                    return $connection->transaction(function () use ($connection) {
                        return $connection->transaction(fn () => $connection->transactionLevel());
                    });
                }),
                'upsert' => $probe(fn () => DB::connection()->table('demo_records')->upsert([[
                    'public_id' => (string) Str::uuid(),
                    'name' => 'upsert-probe',
                    'created_at' => now(),
                    'updated_at' => now(),
                ]], ['public_id'], ['name'])),
            ],
        ]);
    }
}
