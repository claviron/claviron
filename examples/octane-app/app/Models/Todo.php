<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property string $title
 * @property string|null $notes
 * @property string $priority
 * @property Carbon|null $due_on
 * @property Carbon|null $completed_at
 * @property int $position
 */
final class Todo extends Model
{
    use HasFactory;

    public const PRIORITIES = ['low', 'normal', 'high'];

    protected $fillable = ['title', 'notes', 'priority', 'due_on', 'completed_at', 'position'];

    protected $casts = [
        'due_on' => 'date',
        'completed_at' => 'datetime',
        'position' => 'integer',
    ];

    public function isCompleted(): bool
    {
        return $this->completed_at !== null;
    }

    public function scopePending(Builder $query): Builder
    {
        return $query->whereNull('completed_at');
    }

    public function scopeCompleted(Builder $query): Builder
    {
        return $query->whereNotNull('completed_at');
    }

    /**
     * Manual order, with the id as a stable tiebreaker so two todos that share
     * a position never swap places between two reads of the same list.
     */
    public function scopeOrdered(Builder $query): Builder
    {
        return $query->orderBy('position')->orderBy('id');
    }
}
