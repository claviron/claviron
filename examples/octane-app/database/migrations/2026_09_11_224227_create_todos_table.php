<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Follows the selected Laravel connection, shared SQLite by default.
 */
return new class extends Migration
{
    private const TABLE = 'todos';

    public function up(): void
    {
        Schema::connection($this->getConnection())->create(self::TABLE, function (Blueprint $table): void {
            $table->id();
            $table->string('title');
            $table->text('notes')->nullable();
            $table->string('priority')->default('normal');
            $table->date('due_on')->nullable();
            // Null means open. A timestamp is more useful than a boolean: it
            // answers "when" as well as "whether", and it sorts.
            $table->timestamp('completed_at')->nullable();
            // Manual ordering. Sparse by design so a reorder rewrites only the
            // rows that moved.
            $table->unsignedInteger('position')->default(0);
            $table->timestamps();

            $table->index(['completed_at', 'position']);
        });
    }

    public function down(): void
    {
        Schema::connection($this->getConnection())->dropIfExists(self::TABLE);
    }
};
