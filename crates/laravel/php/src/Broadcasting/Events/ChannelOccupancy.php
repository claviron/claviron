<?php

declare(strict_types=1);

namespace Claviron\Laravel\Broadcasting\Events;

use Carbon\CarbonImmutable;
use Claviron\Laravel\Events\FleetEvent;
use UnexpectedValueException;

/** A fleet-global Pusher channel occupancy transition. */
abstract class ChannelOccupancy extends FleetEvent
{
    public function __construct(
        public readonly string $appId,
        public readonly string $channel,
        public readonly CarbonImmutable $time,
        public readonly ?string $userId = null,
        public readonly mixed $userInfo = null,
    ) {
    }

    public static function fromPayload(array $payload): static
    {
        $time = $payload['time'] ?? null;

        if (!is_string($time) || $time === '') {
            throw new UnexpectedValueException('Channel occupancy event requires an ISO-8601 time.');
        }

        return new static(
            (string) ($payload['app_id'] ?? ''),
            (string) ($payload['channel'] ?? ''),
            CarbonImmutable::parse($time),
            isset($payload['user_id']) ? (string) $payload['user_id'] : null,
            $payload['user_info'] ?? null,
        );
    }

    public function toPayload(int $budget): array
    {
        return [
            'app_id' => $this->appId,
            'channel' => $this->channel,
            'time' => $this->time->format('Y-m-d\TH:i:s.uP'),
            'user_id' => $this->userId,
            'user_info' => $this->userInfo,
        ];
    }

}
