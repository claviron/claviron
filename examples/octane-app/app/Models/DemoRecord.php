<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletes;

/**
 * Eloquent surface over the application's selected connection, SQLite by default.
 */
final class DemoRecord extends Model
{
    use HasFactory;
    use SoftDeletes;

    protected $table = 'demo_records';

    protected $guarded = [];

    protected $casts = [
        'active' => 'boolean',
        'small_count' => 'integer',
        'big_count' => 'integer',
        'amount' => 'decimal:2',
        'ratio' => 'float',
        'meta' => 'array',
        'settings' => 'array',
        'effective_on' => 'date',
        'seen_at' => 'datetime',
        'synced_at' => 'datetime',
    ];
}
