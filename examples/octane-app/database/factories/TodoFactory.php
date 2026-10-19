<?php

declare(strict_types=1);

namespace Database\Factories;

use App\Models\Todo;
use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends Factory<Todo>
 */
final class TodoFactory extends Factory
{
    protected $model = Todo::class;

    /**
     * @return array<string, mixed>
     */
    public function definition(): array
    {
        return [
            'title' => rtrim(fake()->sentence(4), '.'),
            'notes' => fake()->boolean(40) ? fake()->sentence(10) : null,
            'priority' => fake()->randomElement(Todo::PRIORITIES),
            'due_on' => fake()->boolean(50)
                ? fake()->dateTimeBetween('now', '+3 weeks')->format('Y-m-d')
                : null,
            'completed_at' => null,
            'position' => 0,
        ];
    }

    public function completed(): static
    {
        return $this->state(fn (): array => [
            'completed_at' => fake()->dateTimeBetween('-1 week', 'now'),
        ]);
    }

    public function priority(string $priority): static
    {
        return $this->state(fn (): array => ['priority' => $priority]);
    }
}
