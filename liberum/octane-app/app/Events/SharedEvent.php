<?php

declare(strict_types=1);

namespace App\Events;

use App\Models\User;
use Lumenize\Events\FleetEvent;

/** A language-neutral fleet event that JavaScript and PHP can both observe. */
final class SharedEvent extends FleetEvent
{
    public function __construct(
        public readonly User $user,
    ) {
    }

    public static function topic(): string
    {
        return 'demo.shared.v1';
    }

}
