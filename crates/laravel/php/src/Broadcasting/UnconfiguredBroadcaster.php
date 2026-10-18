<?php

declare(strict_types=1);

namespace Claviron\Laravel\Broadcasting;

use Illuminate\Broadcasting\Broadcasters\Broadcaster as IlluminateBroadcaster;
use LogicException;

final class UnconfiguredBroadcaster extends IlluminateBroadcaster
{
    public function auth($request): never
    {
        throw new LogicException('Claviron broadcasting requires a Pusher key and secret.');
    }

    public function validAuthenticationResponse($request, $result): never
    {
        throw new LogicException('Claviron broadcasting requires a Pusher key and secret.');
    }

    public function broadcast(array $channels, $event, array $payload = []): never
    {
        throw new LogicException('Claviron broadcasting requires a Pusher key and secret.');
    }
}
