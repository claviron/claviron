<?php

declare(strict_types=1);

namespace Claviron\Laravel\Broadcasting;

use Illuminate\Broadcasting\Broadcasters\Broadcaster as IlluminateBroadcaster;
use Illuminate\Broadcasting\Broadcasters\UsePusherChannelConventions;
use Illuminate\Support\Arr;
use LogicException;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

final class Broadcaster extends IlluminateBroadcaster
{
    use UsePusherChannelConventions;

    public function __construct(
        private readonly string $key,
        private readonly string $secret,
        private readonly bool $requireAuthenticatedUser = true,
    ) {
        if ($this->key === '' || $this->secret === '') {
            throw new LogicException('Claviron broadcasting requires a Pusher key and secret.');
        }
    }

    public function auth($request): array
    {
        $channelName = $this->normalizeChannelName((string) $request->channel_name);

        if (empty($request->channel_name)
            || ($this->requireAuthenticatedUser
                && $this->isGuardedChannel($request->channel_name)
                && ! $this->retrieveUser($request, $channelName))) {
            throw new AccessDeniedHttpException();
        }

        return parent::verifyUserCanAccessChannel($request, $channelName);
    }

    public function validAuthenticationResponse($request, $result): array
    {
        $channel = (string) $request->channel_name;
        $socket = (string) $request->socket_id;

        if (str_starts_with($channel, 'private-')) {
            return ['auth' => $this->signature("{$socket}:{$channel}")];
        }

        $channelName = $this->normalizeChannelName($channel);
        $user = $this->retrieveUser($request, $channelName);
        $userId = method_exists($user, 'getAuthIdentifierForBroadcasting')
            ? $user->getAuthIdentifierForBroadcasting()
            : $user->getAuthIdentifier();
        $channelData = json_encode(
            ['user_id' => $userId, 'user_info' => $result],
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
        );

        return [
            'auth' => $this->signature("{$socket}:{$channel}:{$channelData}"),
            'channel_data' => $channelData,
        ];
    }

    public function broadcast(array $channels, $event, array $payload = []): void
    {
        if (! function_exists('claviron_laravel_broadcast')) {
            throw new LogicException('The Claviron Laravel broadcast extension is unavailable.');
        }

        $socket = Arr::pull($payload, 'socket');
        $data = json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);

        claviron_laravel_broadcast(
            $this->key,
            $this->formatChannels($channels),
            (string) $event,
            $data,
            $socket === null ? null : (string) $socket,
        );
    }

    private function signature(string $value): string
    {
        return $this->key.':'.hash_hmac('sha256', $value, $this->secret);
    }
}
