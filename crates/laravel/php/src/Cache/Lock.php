<?php

declare(strict_types=1);

namespace Claviron\Laravel\Cache;

use Illuminate\Cache\Lock as IlluminateLock;
use InvalidArgumentException;

/**
 * A Laravel lock held in the Orbit lock ring.
 *
 * Orbit leases always expire, so a lock asked for with no duration cannot be
 * acquired here; it can still be restored and released, which is what
 * `restoreLock()` needs.
 */
final class Lock extends IlluminateLock
{
    public function acquire()
    {
        if ($this->seconds <= 0) {
            throw new InvalidArgumentException(
                "Claviron lock [{$this->name}] needs a duration: Orbit leases always expire.",
            );
        }

        return orbit_lock_acquire($this->name, $this->owner, $this->seconds * 1000);
    }

    public function release()
    {
        return orbit_lock_release($this->name, $this->owner);
    }

    public function forceRelease()
    {
        orbit_lock_force_release($this->name);
    }

    protected function getCurrentOwner()
    {
        return orbit_lock_owner($this->name);
    }
}
