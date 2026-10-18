<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Type-matrix table for the Claviron PostgreSQL connection.
 *
 * Unqualified on purpose: the connection's search_path names the application
 * schema, so this table and Laravel's own migrations table land there.
 *
 * No `$connection` of its own: it follows `--database`, which is the plain-PDO
 * `pgsql` connection from a shell and `claviron` when Artisan is driven from
 * inside the runtime.
 *
 * Every column here exists to exercise one encode/decode path in
 * claviron-php-embed's type layer, so a successful round trip through Eloquent
 * is a real compatibility signal rather than a happy-path demo.
 */
return new class extends Migration
{
    private const TABLE = 'demo_records';

    public function up(): void
    {
        Schema::connection($this->getConnection())->create(self::TABLE, function (Blueprint $table): void {
            $table->id();
            $table->uuid('public_id')->unique();
            $table->string('name');
            $table->text('notes')->nullable();
            $table->boolean('active')->default(true);
            $table->integer('small_count')->default(0);
            $table->bigInteger('big_count')->default(0);
            $table->decimal('amount', 12, 2)->default(0);
            $table->double('ratio')->nullable();
            $table->json('meta')->nullable();
            $table->jsonb('settings')->nullable();
            $table->date('effective_on')->nullable();
            $table->time('runs_at')->nullable();
            $table->timestamp('seen_at')->nullable();
            $table->timestampTz('synced_at')->nullable();
            $table->binary('payload')->nullable();
            $table->timestamps();
            $table->softDeletes();
        });
    }

    public function down(): void
    {
        Schema::connection($this->getConnection())->dropIfExists(self::TABLE);
    }
};
