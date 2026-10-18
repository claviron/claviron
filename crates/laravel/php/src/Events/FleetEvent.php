<?php

declare(strict_types=1);

namespace Claviron\Laravel\Events;

use Illuminate\Contracts\Support\Jsonable;

/** A language-neutral event carried across the fleet. */
abstract class FleetEvent implements Jsonable
{
    abstract public static function topic(): string;

    /**
     * @throws \JsonException
     */
    public function toPayload(int $budget): array
    {
        return json_decode($this->toJson(), true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * @throws \JsonException
     */
    public function toJson($options = 0): string
    {
        return json_encode($this, $options | JSON_THROW_ON_ERROR);
    }
}
