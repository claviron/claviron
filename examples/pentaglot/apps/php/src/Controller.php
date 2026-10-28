<?php
declare(strict_types=1);

namespace Pentaglot\Php;

use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

final class Controller
{
    private int $requests = 0;

    public function __invoke(Request $request): JsonResponse
    {
        return new JsonResponse([
            'runtime' => 'php', 'engine' => 'Symfony / Claviron PHP embed',
            'method' => $request->getMethod(), 'path' => $request->getPathInfo(),
            'body' => $request->getContent(), 'requests' => ++$this->requests,
        ]);
    }
}
