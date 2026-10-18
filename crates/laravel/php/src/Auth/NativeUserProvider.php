<?php

declare(strict_types=1);

namespace Claviron\Laravel\Auth;

use Illuminate\Contracts\Auth\Authenticatable;

/** Hydration only: the host has already loaded this request's user. */
interface NativeUserProvider
{
    public function hydrateFromNative(array $attributes): Authenticatable;
}
