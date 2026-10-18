<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use Claviron\Runtime\Orbital;
use Illuminate\Http\Request;
use Illuminate\Http\Response;

/**
 * One independent integer cell per persistent PHP application instance.
 */
final class OrbitalCellController extends Controller
{
    private readonly Orbital $cell;

    public function __construct()
    {
        $this->cell = Orbital::int(0);
    }

    public function hit(Request $request): Response
    {
        $validated = $request->validate(['value' => ['sometimes', 'integer']]);
        $value = $this->cell->add((int) ($validated['value'] ?? 1));

        return response("cell={$value}\n", 200, [
            'Content-Type' => 'text/plain; charset=utf-8',
        ]);
    }

    public function value(): Response
    {
        $value = $this->cell->get();

        return response("cell={$value}\n", 200, [
            'Content-Type' => 'text/plain; charset=utf-8',
        ]);
    }

    public function release(): void
    {
        $this->cell->release();
    }
}
