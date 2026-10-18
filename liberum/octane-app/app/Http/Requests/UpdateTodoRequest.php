<?php

declare(strict_types=1);

namespace App\Http\Requests;

use App\Models\Todo;
use Illuminate\Foundation\Http\FormRequest;
use Illuminate\Validation\Rule;

final class UpdateTodoRequest extends FormRequest
{
    public function authorize(): bool
    {
        return true;
    }

    /**
     * Every field is optional: the client sends only what changed, which is
     * what an inline edit produces.
     *
     * @return array<string, array<int, mixed>>
     */
    public function rules(): array
    {
        return [
            'title' => ['sometimes', 'required', 'string', 'max:255'],
            'notes' => ['sometimes', 'nullable', 'string', 'max:2000'],
            'priority' => ['sometimes', Rule::in(Todo::PRIORITIES)],
            'due_on' => ['sometimes', 'nullable', 'date'],
            'completed' => ['sometimes', 'boolean'],
        ];
    }
}
