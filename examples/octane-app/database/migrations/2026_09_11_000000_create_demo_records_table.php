<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Type-matrix table for the application's selected connection.
 * The default is the shared demo SQLite file; an explicit --database may
 * select another driver. SQLite exercises PDO/Eloquent, not the native PG bridge.
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
