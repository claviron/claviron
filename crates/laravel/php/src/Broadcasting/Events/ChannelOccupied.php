<?php

declare(strict_types=1);

namespace Claviron\Laravel\Broadcasting\Events;

/** The first subscriber occupied a Pusher channel in this application. */
final class ChannelOccupied extends ChannelOccupancy
{
    public static function topic(): string
    {
        return 'channel_occupied';
    }
}
