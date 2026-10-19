<?php

declare(strict_types=1);

use App\Models\Todo;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

it('lists todos in manual order with a count summary', function (): void {
    Todo::factory()->create(['title' => 'second', 'position' => 2]);
    Todo::factory()->create(['title' => 'first', 'position' => 1]);
    Todo::factory()->completed()->create(['title' => 'done', 'position' => 3]);

    $response = $this->getJson('/api/todos')->assertOk();

    expect(array_column($response->json('data'), 'title'))->toBe(['first', 'second', 'done'])
        ->and($response->json('meta'))->toBe(['total' => 3, 'pending' => 2, 'completed' => 1]);
});

it('creates a todo at the end of the list', function (): void {
    Todo::factory()->create(['position' => 7]);

    $this->postJson('/api/todos', [
        'title' => 'write the migration',
        'priority' => 'high',
        'due_on' => '2026-10-01',
    ])
        ->assertCreated()
        ->assertJsonPath('data.title', 'write the migration')
        ->assertJsonPath('data.priority', 'high')
        ->assertJsonPath('data.due_on', '2026-10-01')
        ->assertJsonPath('data.completed', false)
        ->assertJsonPath('data.position', 8);
});

it('rejects a todo without a title', function (): void {
    $this->postJson('/api/todos', ['title' => ''])
        ->assertUnprocessable()
        ->assertJsonValidationErrors('title');
});

it('rejects an unknown priority', function (): void {
    $this->postJson('/api/todos', ['title' => 'ok', 'priority' => 'urgent'])
        ->assertUnprocessable()
        ->assertJsonValidationErrors('priority');
});

it('filters by status', function (): void {
    Todo::factory()->create(['title' => 'open one']);
    Todo::factory()->completed()->create(['title' => 'closed one']);

    expect($this->getJson('/api/todos?status=pending')->json('data.0.title'))->toBe('open one')
        ->and($this->getJson('/api/todos?status=completed')->json('data.0.title'))->toBe('closed one');
});

it('searches title and notes case-insensitively', function (): void {
    Todo::factory()->create(['title' => 'Ship the POOL', 'notes' => null]);
    Todo::factory()->create(['title' => 'unrelated', 'notes' => 'mentions the pool here']);
    Todo::factory()->create(['title' => 'nothing', 'notes' => null]);

    $titles = array_column($this->getJson('/api/todos?q=pool')->json('data'), 'title');

    expect($titles)->toHaveCount(2)
        ->and($titles)->toContain('Ship the POOL', 'unrelated');
});

it('toggles a todo both ways', function (): void {
    $todo = Todo::factory()->create();

    $this->postJson("/api/todos/{$todo->id}/toggle")
        ->assertOk()
        ->assertJsonPath('data.completed', true);

    $this->postJson("/api/todos/{$todo->id}/toggle")
        ->assertOk()
        ->assertJsonPath('data.completed', false)
        ->assertJsonPath('data.completed_at', null);
});

it('updates only the fields it is given', function (): void {
    $todo = Todo::factory()->create(['title' => 'old', 'priority' => 'low', 'notes' => 'keep me']);

    $this->patchJson("/api/todos/{$todo->id}", ['title' => 'new'])
        ->assertOk()
        ->assertJsonPath('data.title', 'new')
        ->assertJsonPath('data.priority', 'low')
        ->assertJsonPath('data.notes', 'keep me');
});

it('completes a todo through update', function (): void {
    $todo = Todo::factory()->create();

    $this->patchJson("/api/todos/{$todo->id}", ['completed' => true])
        ->assertOk()
        ->assertJsonPath('data.completed', true);

    expect($todo->refresh()->completed_at)->not->toBeNull();
});

it('rewrites positions on reorder', function (): void {
    $first = Todo::factory()->create(['position' => 1]);
    $second = Todo::factory()->create(['position' => 2]);
    $third = Todo::factory()->create(['position' => 3]);

    $this->postJson('/api/todos/reorder', ['ids' => [$third->id, $first->id, $second->id]])
        ->assertOk();

    expect($third->refresh()->position)->toBe(1)
        ->and($first->refresh()->position)->toBe(2)
        ->and($second->refresh()->position)->toBe(3);
});

it('deletes a todo', function (): void {
    $todo = Todo::factory()->create();

    $this->deleteJson("/api/todos/{$todo->id}")
        ->assertOk()
        ->assertJsonPath('meta.total', 0);

    expect(Todo::query()->count())->toBe(0);
});

it('clears only the completed todos', function (): void {
    Todo::factory()->count(2)->completed()->create();
    Todo::factory()->create(['title' => 'survivor']);

    $this->deleteJson('/api/todos/completed')
        ->assertOk()
        ->assertJsonPath('deleted', 2);

    expect(Todo::query()->pluck('title')->all())->toBe(['survivor']);
});
