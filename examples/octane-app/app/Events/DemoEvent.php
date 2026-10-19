<?php

declare(strict_types=1);

namespace App\Events;

use App\Models\User;
use Illuminate\Queue\SerializesModels;

final readonly class DemoEvent
{
    use SerializesModels;

    public function __construct(
        public User $user
    )
    {
    }
}
