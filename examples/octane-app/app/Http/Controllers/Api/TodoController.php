<?php

declare(strict_types=1);

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Http\Requests\StoreTodoRequest;
use App\Http\Requests\UpdateTodoRequest;
use App\Http\Resources\TodoResource;
use App\Models\Todo;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Http\Resources\Json\AnonymousResourceCollection;
use Illuminate\Support\Facades\DB;
use Illuminate\Validation\Rule;

/**
 * Every read and write here lands on the runtime-owned pool through the model's
 * hot-path connection. Nothing in this controller opens a connection, and the
 * only place a connection is held across more than one statement is the
 * reorder transaction.
 */
final class TodoController extends Controller
{
    public function index(Request $request): AnonymousResourceCollection
    {
        $filters = $request->validate([
            'status' => ['sometimes', Rule::in(['all', 'pending', 'completed'])],
            'priority' => ['sometimes', Rule::in(Todo::PRIORITIES)],
            'q' => ['sometimes', 'string', 'max:255'],
        ]);

        $query = Todo::query()->ordered();

        match ($filters['status'] ?? 'all') {
            'pending' => $query->pending(),
            'completed' => $query->completed(),
            default => null,
        };

        if (isset($filters['priority'])) {
            $query->where('priority', $filters['priority']);
        }

        if (isset($filters['q'])) {
            // `lower(...) like` rather than PostgreSQL's `ilike`, so the same
            // query runs in a test process on SQLite. The wildcards in the
            // term itself are escaped; the ones around it are the search.
            $term = '%'.str_replace(['%', '_'], ['\%', '\_'], mb_strtolower($filters['q'])).'%';
            $query->where(function ($query) use ($term): void {
                $query->whereRaw('lower(title) like ?', [$term])
                    ->orWhereRaw("lower(coalesce(notes, '')) like ?", [$term]);
            });
        }

        return TodoResource::collection($query->get())->additional([
            'meta' => $this->summary(),
        ]);
    }

    public function store(StoreTodoRequest $request): JsonResponse
    {
        $todo = Todo::query()->create([
            ...$request->validated(),
            'position' => (int) Todo::query()->max('position') + 1,
        ]);

        return TodoResource::make($todo)
            ->additional(['meta' => $this->summary()])
            ->response()
            ->setStatusCode(201);
    }

    public function show(Todo $todo): TodoResource
    {
        return TodoResource::make($todo);
    }

    public function update(UpdateTodoRequest $request, Todo $todo): TodoResource
    {
        $attributes = $request->safe()->except('completed');

        if ($request->has('completed')) {
            $attributes['completed_at'] = $request->boolean('completed') ? now() : null;
        }

        $todo->update($attributes);

        return TodoResource::make($todo->refresh())->additional(['meta' => $this->summary()]);
    }

    /**
     * Flipping is its own endpoint because the client does not have to know the
     * current state to ask for the opposite of it.
     */
    public function toggle(Todo $todo): TodoResource
    {
        $todo->update(['completed_at' => $todo->isCompleted() ? null : now()]);

        return TodoResource::make($todo->refresh())->additional(['meta' => $this->summary()]);
    }

    public function destroy(Todo $todo): JsonResponse
    {
        $todo->delete();

        return response()->json(['meta' => $this->summary()]);
    }

    /**
     * The new order arrives as the full list of ids. One transaction on one
     * pooled connection, so a concurrent read never sees half an order.
     */
    public function reorder(Request $request): JsonResponse
    {
        $validated = $request->validate([
            'ids' => ['required', 'array', 'min:1'],
            'ids.*' => ['integer'],
        ]);

        DB::transaction(function () use ($validated): void {
            foreach (array_values($validated['ids']) as $index => $id) {
                Todo::query()->whereKey($id)->update(['position' => $index + 1]);
            }
        });

        return response()->json(['meta' => $this->summary()]);
    }

    public function clearCompleted(): JsonResponse
    {
        $deleted = Todo::query()->completed()->delete();

        return response()->json(['deleted' => $deleted, 'meta' => $this->summary()]);
    }

    /**
     * One round trip for the three counts the client shows, instead of three.
     *
     * @return array{total: int, pending: int, completed: int}
     */
    private function summary(): array
    {
        $counts = Todo::query()
            ->selectRaw('count(*) as total')
            ->selectRaw('count(*) filter (where completed_at is null) as pending')
            ->selectRaw('count(*) filter (where completed_at is not null) as completed')
            ->first();

        return [
            'total' => (int) $counts->total,
            'pending' => (int) $counts->pending,
            'completed' => (int) $counts->completed,
        ];
    }
}
