<?php

declare(strict_types=1);

namespace Claviron\Laravel\Auth;

use Illuminate\Auth\RequestGuard;
use Illuminate\Http\Request;

/** A persistent Laravel container must not carry a user into its next request. */
final class FleetSessionGuard extends RequestGuard
{
    public function setRequest(Request $request)
    {
        $this->forgetUser();

        return parent::setRequest($request);
    }
}
